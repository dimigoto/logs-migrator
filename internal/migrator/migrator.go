package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"logs-migrator/internal/config"
	"logs-migrator/internal/csv"
	"logs-migrator/internal/dbx"
	"logs-migrator/internal/ranger"
	"logs-migrator/internal/util"
	"logs-migrator/internal/uuidv7"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dateLayout = "2006-01-02 15:04:05"
)

func Run(
	ctx context.Context,
	srcDb,
	dstDb *sql.DB,
	secureDir string,
	cfg config.MigrateConfig,
) error {
	// Определяем минимальный и максимальный ID записей, которые нужно мигрировать
	minPk, maxPk, err := getMinMaxNID(ctx, srcDb, dstDb, cfg)
	if err != nil {
		return err
	}
	log.Printf("[INFO] numeric ID range: %d - %d\n", minPk, maxPk)

	// Разбиваем на шарды
	shards := ranger.SplitByLimit(minPk, maxPk, uint64(cfg.ChunkSize))
	log.Printf("[INFO] shards: %d\n", len(shards))

	// Получаем список колонок табьлицы-источника и целеной таблицы
	srcTableColumns := dbx.MustTableColumns(ctx, srcDb, cfg.SrcTable)
	dstTableColumns := dbx.MustTableColumns(ctx, dstDb, cfg.DstTable)

	// Создаем очереди
	stageJobs := make(chan ranger.Range, len(shards))
	loadJobs := make(chan loadJob, len(shards))
	errs := make(chan error, 1)

	// Создаем счетчики
	var totalStaged atomic.Uint64
	var totalLoaded atomic.Uint64
	var filesStaged atomic.Uint64
	var filesLoaded atomic.Uint64

	// Включаем Fast-load
	dbx.EnableFastLoad(ctx, dstDb)
	defer func() {
		dbx.DisableFastLoad(dstDb)
	}()

	// Фиксируем время старта
	start := time.Now()

	// Запускаем Stage-воркеров
	var stageWG sync.WaitGroup
	for i := 0; i < cfg.StageWorkers; i++ {
		stageWG.Add(1)
		id := i + 1
		go func(id int) {
			defer stageWG.Done()
			if err := runStageWorker(ctx, id, srcDb, srcTableColumns, cfg, secureDir, stageJobs, loadJobs, &totalStaged, &filesStaged); err != nil {
				select {
				case errs <- err:
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
			if err := runLoadWorker(ctx, id, dstDb, dstTableColumns, cfg, loadJobs, &totalLoaded, &filesLoaded); err != nil {
				select {
				case errs <- err:
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
	cfg config.MigrateConfig,
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

	for job := range in {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		last := job.From - 1

		for {
			chunkPath, newLast, written, err := makeRequestAndWriteToCsv(
				ctx,
				src,
				cfg,
				columns,
				last,
				job.To,
				secureDir,
				loc,
			)
			if err != nil {
				return fmt.Errorf("%s: %w", logPrefix, err)
			}

			last = newLast

			log.Printf("%s - %v", logPrefix, job)

			totalFiles.Add(1)
			totalRows.Add(written)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- loadJob{Path: chunkPath, Rows: written}:
			}

			if last >= job.To {
				break
			}

		}
	}

	return nil
}

func makeRequestAndWriteToCsv(
	ctx context.Context,
	db *sql.DB,
	cfg config.MigrateConfig,
	columns []string,
	from, to uint64,
	tmpDir string,
	loc *time.Location,
) (chunkPath string, newFrom, written uint64, err error) {
	query := dbx.BuildSelectByRange(cfg.SrcTable, columns, cfg.SrcFilter)
	rows, err := db.QueryContext(ctx, query, from, to)
	if err != nil {
		return chunkPath, newFrom, written, fmt.Errorf("query error :%w", err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	values := make([]any, len(cols))
	valuePointers := make([]any, len(cols))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	chunkPath = filepath.Join(
		tmpDir,
		fmt.Sprintf("stage_%s_%d-%d_%d.csv", cfg.SrcTable, from, to, time.Now().UnixNano()),
	)
	csvFile, err := csv.New(chunkPath)
	if err != nil {
		return chunkPath, newFrom, written, fmt.Errorf("create tmp csv file: %w", err)
	}
	defer csvFile.Close()

	for rows.Next() {
		if err := rows.Scan(valuePointers...); err != nil {
			return chunkPath, newFrom, written, fmt.Errorf("scan: %w", err)
		}

		uuid, err := createUuid(dbx.AsString(values[cfg.TSColumnIdx-1]), loc)
		if err != nil {
			return chunkPath, newFrom, written, fmt.Errorf("error creating UUID: %w", err)
		}

		outRec := make([]string, 0, len(values)+1)
		outRec = append(outRec, uuid)
		for _, v := range values {
			outRec = append(outRec, dbx.AsString(v))
		}

		if err := csvFile.Write(outRec); err != nil {
			return chunkPath, newFrom, written, fmt.Errorf("error writing CSV: %w", err)
		}

		written++

		newFrom = to

		// обновляем last по PK
		if id, err := strconv.ParseUint(dbx.AsString(values[0]), 10, 64); err == nil && id > from {
			newFrom = id
		}
	}

	if err := csvFile.Flush(); err != nil {
		return chunkPath, newFrom, written, err
	}

	if err := csvFile.Sync(); err != nil {
		return chunkPath, newFrom, written, err
	}

	if written == 0 {
		_ = os.Remove(chunkPath)
	}

	return chunkPath, newFrom, written, nil
}

// runLoadWorker запускает Load-воркера, который загружает данные из временного файла в целевую БД.
// После успешной загрузки удаляет временный файл
func runLoadWorker(
	ctx context.Context,
	id int,
	dst *sql.DB,
	columns []string,
	cfg config.MigrateConfig,
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

		if err := loadDataInfile(ctx, dst, j.Path, cfg.DstTable, columns); err != nil {
			return fmt.Errorf("%s LOAD DATA: %w", logPrefix, err)
		}

		totalFiles.Add(1)
		totalRows.Add(j.Rows)
		log.Printf("%s loaded %s (+%d rows)", logPrefix, filepath.Base(j.Path), j.Rows)

		if err := os.Remove(j.Path); err != nil {
			return err
		}
	}

	return nil
}

func loadDataInfile(ctx context.Context, db *sql.DB, stagedPath, dstTable string, columns []string) error {
	if len(columns) == 0 {
		return fmt.Errorf("distanation table has no columns")
	}

	vars := make([]string, 0, len(columns))
	vars = append(vars, "@id_hex")
	for i := 1; i < len(columns); i++ {
		vars = append(vars, "@"+columns[i])
	}

	setClauses := []string{"id=UNHEX(@id_hex)"}
	for i := 1; i < len(columns); i++ {
		col := columns[i]
		if strings.EqualFold(col, "ins_ts") {
			setClauses = append(setClauses,
				fmt.Sprintf("%s=STR_TO_DATE(@%s,'%%Y-%%m-%%d %%H:%%i:%%s')", util.Ident(col), col))
		} else {
			setClauses = append(setClauses,
				fmt.Sprintf("%s=NULLIF(@%s,'')", util.Ident(col), col))
		}
	}

	file := strings.ReplaceAll(stagedPath, `\`, `\\`)
	file = strings.ReplaceAll(file, `'`, `\'`)
	loadSQL := fmt.Sprintf(
		`
			LOAD DATA INFILE '%s' INTO TABLE %s
			FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
			LINES TERMINATED BY '\n'
			IGNORE 0 LINES
			(%s)
			SET %s
		`,
		file,
		util.Ident(dstTable),
		strings.Join(vars, ","),
		strings.Join(setClauses, ", "),
	)
	_, err := db.ExecContext(ctx, loadSQL)

	return err
}

// getMinMaxNID расчитывает минимальный и максимальный числовой ID
func getMinMaxNID(
	ctx context.Context,
	srcDb,
	dstDb *sql.DB,
	cfg config.MigrateConfig,
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

// createUuid генерирует uuid на основе даты
func createUuid(date string, loc *time.Location) (string, error) {
	if date == "" {
		return "", errors.New("empty datetime")
	}

	convertedDate, err := time.ParseInLocation(dateLayout, date, loc)
	if err != nil {
		return "", err
	}

	return uuidv7.FromTime(convertedDate)
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
