package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"logs-migrator/internal/config"
	"logs-migrator/internal/dbx"
	"logs-migrator/internal/ranger"
	"logs-migrator/internal/stagewriter"
	"logs-migrator/internal/util"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

func Run(
	ctx context.Context,
	srcDb,
	dstDb *sql.DB,
	secureDir string,
	cfg config.Config,
) error {
	// Определяем минимальный и максимальный ID записей, которые нужно мигрировать
	minPk, maxPk, err := getMinMaxNID(ctx, srcDb, dstDb, cfg)
	if err != nil {
		return err
	}
	log.Printf("[INFO] numeric ID range: %d - %d\n", minPk, maxPk)

	// Разбиваем на шарды
	shards := ranger.Split(minPk, maxPk, uint64(cfg.ChunkSize))
	log.Printf("[INFO] shards: %d\n", len(shards))

	// Получаем список колонок табьлицы-источника и целеной таблицы
	srcTableColumns := dbx.MustTableColumns(ctx, srcDb, cfg.SrcTable)
	dstTableColumns := dbx.MustTableColumns(ctx, dstDb, cfg.DstTable)

	// Оборачиваем родительский контекст для воркеров
	workersCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()

	// Создаем очереди
	stageJobs := make(chan ranger.Range, len(shards))
	loadJobs := make(chan loadJob, len(shards))
	errs := make(chan error, 1)

	// Создаем счетчики
	var totalStaged atomic.Uint64
	var totalLoaded atomic.Uint64
	var filesStaged atomic.Uint64
	var filesLoaded atomic.Uint64

	// Включаем Fast-load если указан флаг
	if cfg.UseFastLoad {
		originalSettings := dbx.EnableFastLoad(ctx, dstDb, cfg.InnodbBufferPoolSize, cfg.InnodbIOCapacity, cfg.InnodbIOCapacityMax)
		defer func() {
			dbx.DisableFastLoad(dstDb, originalSettings)
		}()
	}

	// Фиксируем время старта
	start := time.Now()

	// Запускаем Stage-воркеров
	var stageWG sync.WaitGroup
	for i := 0; i < cfg.StageWorkers; i++ {
		stageWG.Add(1)
		id := i + 1
		go func(id int) {
			defer stageWG.Done()
			if err := runStageWorker(workersCtx, id, srcDb, srcTableColumns, cfg, secureDir, stageJobs, loadJobs, &totalStaged, &filesStaged); err != nil {
				select {
				case errs <- err:
					cancelWork()
				default:
				}
			}
		}(id)
	}

	// Запускаем Load-воркеров
	var loadWG sync.WaitGroup
	for i := 0; i < cfg.LoadWorkers; i++ {
		loadWG.Add(1)
		id := i + 1
		go func(id int) {
			defer loadWG.Done()
			if err := runLoadWorker(workersCtx, id, dstDb, dstTableColumns, cfg, loadJobs, &totalLoaded, &filesLoaded); err != nil {
				select {
				case errs <- err:
					cancelWork()
				default:
				}
			}
		}(id)
	}

	// Запускаем продюсера
	go func() {
		defer close(stageJobs)
		for _, sh := range shards {
			select {
			case <-ctx.Done():
				return
			case stageJobs <- sh:
			}
		}
	}()

	// Ждём когда отработает этап стейджа...
	stageWG.Wait()
	// ... и закрываем очередь загрузки
	close(loadJobs)
	// Ждём когда завершится этап загрузки
	loadWG.Wait()

	// Печатаем статистику. Если в канале с ошибками есть записи, то пишем, что миграция не удалась
	close(errs)
	if e := <-errs; e != nil {
		printStats(start, filesStaged.Load(), totalStaged.Load(), filesLoaded.Load(), totalLoaded.Load(), true)
		return e
	}

	printStats(start, filesStaged.Load(), totalStaged.Load(), filesLoaded.Load(), totalLoaded.Load(), false)

	return nil
}

type loadJob struct {
	Path string
	Rows uint64
}

// runStageWorker запускает Stage-воркера, который идет в БД-источник, забирает данные, добавляет UUIDv7
// и сохраняет во временный файл для последующей загрузки в целевую БД
func runStageWorker(
	ctx context.Context,
	id int,
	src *sql.DB,
	columns []string,
	cfg config.Config,
	secureDir string,
	in <-chan ranger.Range,
	out chan<- loadJob,
	totalRows *atomic.Uint64,
	totalFiles *atomic.Uint64,
) error {
	logPrefix := fmt.Sprintf("[STAGE#%d]", id)
	loc, err := time.LoadLocation(cfg.UUIDTZ)
	if err != nil {
		return err
	}

	// Слушаем job'ы из канала in
	for job := range in {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		chunkPath, written, err := processShardToCSV(
			ctx,
			src,
			cfg,
			columns,
			job.From,
			job.To,
			secureDir,
			loc,
		)
		if err != nil {
			return fmt.Errorf("%s: %w", logPrefix, err)
		}

		// Если ничего не записано, скипаем, значит в заданном диапазоне ID ничего не найдено
		if written == 0 {
			continue
		}

		log.Printf("%s processed range [%d..%d]: %d rows", logPrefix, job.From, job.To, written)

		totalFiles.Add(1)
		totalRows.Add(written)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- loadJob{Path: chunkPath, Rows: written}:
		}
	}

	return nil
}

// processShardToCSV считывает данные из исходной базы данных для заданного диапазона и записывает их в CSV
func processShardToCSV(
	ctx context.Context,
	db *sql.DB,
	cfg config.Config,
	columns []string,
	from, to uint64,
	tmpDir string,
	loc *time.Location,
) (chunkPath string, written uint64, err error) {
	// Отправляем запрос в БД-источник
	query := dbx.BuildSelectByRange(cfg.SrcTable, columns, cfg.SrcNID, cfg.SrcFilter)
	rows, err := db.QueryContext(ctx, query, from, to)
	if err != nil {
		return "", 0, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	// Готовим переменные для хранения результатов запроса
	cols, _ := rows.Columns()
	values := make([]any, len(cols))
	valuePointers := make([]any, len(cols))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	// Создаем структуру для записи данных в CSV
	writer, err := stagewriter.New(tmpDir, cfg.SrcTable, from, to, cfg.TSColumnIdx-1, loc)
	if err != nil {
		return "", 0, err
	}
	defer writer.Close()

	// Обходим полученные записи
	for rows.Next() {
		if err := rows.Scan(valuePointers...); err != nil {
			writer.CleanupOnError()
			return "", 0, fmt.Errorf("scan: %w", err)
		}

		if err := writer.WriteRow(values); err != nil {
			writer.CleanupOnError()
			return "", 0, err
		}
	}

	// Если в процессе обхода возникла ошибка, нужно её выкинуть наружу
	if err := rows.Err(); err != nil {
		writer.CleanupOnError()
		return "", 0, fmt.Errorf("rows iteration: %w", err)
	}

	written = writer.RowsWritten()

	// Удаляем пустые файлы
	if written == 0 {
		writer.CleanupOnError()
		return "", 0, nil
	}

	return writer.Path(), written, nil
}

// runLoadWorker запускает Load-воркера, который загружает данные из временного файла в целевую БД.
// После успешной загрузки удаляет временный файл
func runLoadWorker(
	ctx context.Context,
	id int,
	dst *sql.DB,
	columns []string,
	cfg config.Config,
	in <-chan loadJob,
	totalRows *atomic.Uint64,
	totalFiles *atomic.Uint64,
) error {
	logPrefix := fmt.Sprintf("[LOAD#%d]", id)

	for j := range in {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		log.Printf("%s start LOAD IN FILE %s", logPrefix, filepath.Base(j.Path))

		if err := loadDataInfile(ctx, dst, j.Path, cfg.DstTable, cfg.DstUuid, columns, cfg.UseLocalInfile); err != nil {
			return fmt.Errorf("%s LOAD DATA: %w", logPrefix, err)
		}

		totalFiles.Add(1)
		totalRows.Add(j.Rows)
		log.Printf("%s loaded %s (+%d rows)", logPrefix, filepath.Base(j.Path), j.Rows)
	}

	return nil
}

func loadDataInfile(ctx context.Context, db *sql.DB, stagedPath, dstTable, uuidCol string, columns []string, useLocalInfile bool) error {
	if len(columns) == 0 {
		return fmt.Errorf("destination table has no columns")
	}

	// Строим SQL для LOAD DATA INFILE или LOAD DATA LOCAL INFILE
	loadSQL := dbx.BuildLoadDataSQL(stagedPath, dstTable, uuidCol, columns, useLocalInfile)
	if loadSQL == "" {
		return fmt.Errorf("failed to build LOAD DATA SQL")
	}

	// Запрос может быть достаточно долгим, поэтому лучше контекст обернуть с большим таймаутом
	loadCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	// Выполняем LOAD DATA INFILE
	_, err := db.ExecContext(loadCtx, loadSQL)

	// Удаляем файл ПОСЛЕ завершения ExecContext (в любом случае - успех или ошибка)
	if removeErr := os.Remove(stagedPath); removeErr != nil {
		log.Printf("[WARN] failed to remove %s: %v", stagedPath, removeErr)
	}

	return err
}

// getMinMaxNID расчитывает минимальный и максимальный числовой ID
func getMinMaxNID(
	ctx context.Context,
	srcDb,
	dstDb *sql.DB,
	cfg config.Config,
) (min uint64, max uint64, err error) {
	dstMaxNID := dbx.MustMaxPk(ctx, dstDb, cfg.DstTable, cfg.DstNID)
	srcMinID, srcMaxPk := dbx.MustPKRange(ctx, srcDb, cfg.SrcTable, cfg.SrcNID, cfg.SrcFilter)

	if srcMinID == nil || srcMaxPk == nil || *srcMaxPk <= *srcMinID || *srcMaxPk <= *dstMaxNID {
		return 0, 0, fmt.Errorf("[INFO] No rows")
	}

	min = *srcMinID
	if dstMaxNID != nil && *srcMinID < *dstMaxNID {
		min = *dstMaxNID
	}

	max = *srcMaxPk

	return
}

// printStats печатает статистку миграции
func printStats(start time.Time, filesStaged, rowsStaged, filesLoaded, rowsLoaded uint64, failed bool) {
	duration := time.Since(start)
	if duration <= 0 {
		duration = time.Millisecond
	}

	title := "[IMPORT SUCCESS]"
	if failed {
		title = "[IMPORT FAILED]"
	}

	log.Println("------------------------------------------------------------")
	log.Println(title)
	log.Printf("[STATS] staged: files=%s rows=%s", util.FormatNumber(filesStaged), util.FormatNumber(rowsStaged))
	log.Printf("[STATS] loaded: files=%s rows=%s", util.FormatNumber(filesLoaded), util.FormatNumber(rowsLoaded))
	log.Printf("[STATS] duration: %s", duration.Truncate(time.Second))
	log.Printf("[STATS] speed: %.0f rows/s", float64(rowsLoaded)/duration.Seconds())
	log.Println("------------------------------------------------------------")
}
