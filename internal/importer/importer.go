package importer

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"logs-migrator/internal/archive"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"logs-migrator/internal/config"
	"logs-migrator/internal/dbx"
	"logs-migrator/internal/util"
	"logs-migrator/internal/uuidv7"
)

func Run(cfg config.LoadConfig, db *sql.DB) error {
	loc, err := time.LoadLocation(cfg.UUIDTZ)
	if err != nil {
		return fmt.Errorf("invalid tz %q: %w", cfg.UUIDTZ, err)
	}

	secureDir, err := prepareStageDir(db)
	if err != nil {
		return err
	}

	start := time.Now()
	var totalFiles uint64
	var totalRows uint64

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Устанавливает параметры БД для быстрой загрузки: отключает REDO LOG,
	// проверку уникальности индексов и FK и т.д.
	dbx.EnableFastLoad(ctx, db)
	defer func() {
		log.Printf("[INFO] Restoring settings after import")
		dbx.DisableFastLoad(db)
	}()

	// Количество воркеров, которые конвертируют данные и сохраняют временный CSV
	// для последующей загрузки
	stageWorkers := cfg.Workers

	// Количество воркеров, которые загружают данные в таблицу
	loadWorkers := cfg.Workers / 3
	if loadWorkers < 1 {
		loadWorkers = 1
	}

	log.Printf("[DEBUG] Concurrency: stage=%d, load=%d", stageWorkers, loadWorkers)

	// Канал с путями временных фалов
	tmpPaths := make(chan string, cfg.Workers)

	// Канал с джобами
	stagedJobs := make(chan struct{ stagedPath, base string }, cfg.Workers)

	// Канал с ошибками
	errs := make(chan error, 1)

	// Запускаем Stage pool
	var wgStage sync.WaitGroup
	runStagePool(ctx, &wgStage, stageWorkers, cfg, loc, secureDir, tmpPaths, stagedJobs, &totalRows, errs, cancel)

	// Запускаем Load pool
	var wgLoad sync.WaitGroup
	runLoadPool(ctx, &wgLoad, loadWorkers, cfg, db, stagedJobs, &totalFiles, errs, cancel)

	// Запускаем Producer (читает TAR → пишет tmp → отправляет путь)
	prodErr := runProducer(ctx, cfg.TarPath, tmpPaths)

	// Закрываем верхние каналы и ждём пулы
	close(tmpPaths)
	wgStage.Wait()
	close(stagedJobs)
	wgLoad.Wait()

	// Собираем первую ошибку из канала
	var firstErr error
	select {
	case firstErr = <-errs:
	default:
	}
	if firstErr == nil && prodErr != nil && prodErr != context.Canceled {
		firstErr = prodErr
	}

	// Финальная статистика
	if firstErr != nil {
		log.Printf("[IMPORT FAILED]")
		printFinalStat(start, atomic.LoadUint64(&totalFiles), atomic.LoadUint64(&totalRows))
		return firstErr
	}
	log.Printf("[IMPORT SUCCESS]")
	printFinalStat(start, atomic.LoadUint64(&totalFiles), atomic.LoadUint64(&totalRows))
	return nil
}

/*************** Producer ***************/

func runProducer(
	ctx context.Context,
	tarPath string,
	tmpPaths chan<- string,
) error {
	log.Printf("[DEBUG] Starting import: tar=%s", tarPath)

	return archive.IterateTarGz(tarPath, func(e archive.Entry) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		base := filepath.Base(e.Name)
		ext := filepath.Ext(base)
		if !(strings.HasSuffix(base, ".csv") || strings.HasSuffix(base, ".csv.gz")) {
			return nil
		}

		dir := os.TempDir()
		tmpf, err := os.CreateTemp(dir, base+"-*"+ext)
		if err != nil {
			return err
		}

		_, err = io.Copy(bufio.NewWriterSize(tmpf, 1<<20), e.R)
		if cerr := tmpf.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			_ = os.Remove(tmpf.Name())
			return fmt.Errorf("tar copy %s: %w", e.Name, err)
		}

		select {
		case <-ctx.Done():
			_ = os.Remove(tmpf.Name())
			return ctx.Err()
		case tmpPaths <- tmpf.Name():
			return nil
		}
	})
}

/*************** Stage pool ***************/

func runStagePool(
	ctx context.Context,
	wg *sync.WaitGroup,
	workers int,
	cfg config.LoadConfig,
	loc *time.Location,
	secureDir string,
	tmpPaths <-chan string,
	stagedJobs chan<- struct{ stagedPath, base string },
	totalRows *uint64,
	errs chan<- error,
	cancel context.CancelFunc,
) {
	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			for tmp := range tmpPaths {
				base := filepath.Base(tmp)
				rows, staged, err := stageCSVWithUUID(cfg, tmp, loc, secureDir)
				_ = os.Remove(tmp) // tmp не нужен после стейджа

				if err != nil {
					select {
					case errs <- fmt.Errorf("stage %s: %w", base, err):
					default:
					}
					cancel()
					return
				}

				atomic.AddUint64(totalRows, uint64(rows))

				select {
				case <-ctx.Done():
					return
				case stagedJobs <- struct{ stagedPath, base string }{stagedPath: staged, base: base}:
				}
			}
		}(i + 1)
	}
}

/*************** Load pool ***************/

func runLoadPool(
	ctx context.Context,
	wg *sync.WaitGroup,
	workers int,
	cfg config.LoadConfig,
	db *sql.DB,
	stagedJobs <-chan struct{ stagedPath, base string },
	totalFiles *uint64,
	errs chan<- error,
	cancel context.CancelFunc,
) {
	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			for job := range stagedJobs {
				preparedSql := buildLoadSQL(cfg, job.stagedPath)
				start := time.Now()

				if _, err := db.ExecContext(ctx, preparedSql); err != nil {
					_ = os.Remove(job.stagedPath)

					select {
					case errs <- fmt.Errorf("LOAD %s: %w", job.base, err):
					default:
					}

					cancel()
					return
				}

				// Удалем файл из secure_file_priv
				_ = os.Remove(job.stagedPath)

				atomic.AddUint64(totalFiles, 1)

				log.Printf("[IMPORT] %s: loaded in %s", job.base, time.Since(start).Truncate(time.Second))
			}
		}(i + 1)
	}
}

/*************** Staging (single file) ***************/

func stageCSVWithUUID(cfg config.LoadConfig, srcPath string, loc *time.Location, secureDir string) (int, string, error) {
	// Проверяем и открываем исходный файл
	if strings.TrimSpace(secureDir) == "" {
		return 0, "", fmt.Errorf("secure_file_priv directory is empty")
	}
	f, err := os.Open(srcPath)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()

	// Настраиваем ридер
	var r io.Reader = bufio.NewReaderSize(f, 1<<20)

	if strings.HasSuffix(strings.ToLower(srcPath), ".gz") {
		gzr, err := gzip.NewReader(r)
		if err != nil {
			return 0, "", fmt.Errorf("gzip open: %w", err)
		}
		defer gzr.Close()
		r = bufio.NewReaderSize(gzr, 1<<20)
	}

	// Настроеваем CSV-парсер
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true

	// Индекс колонки, в которой лежит дата для UUIDv7
	dtIdx := cfg.UUIDFromIdx - 1
	if dtIdx < 0 {
		return 0, "", fmt.Errorf("UUIDFromIdx parameter is required")
	}

	// Разюиваем названия колонок в мапу
	dstCols := util.SplitCols(cfg.DstColumns)
	if len(dstCols) == 0 || strings.ToLower(dstCols[0]) != "id" {
		return 0, "", fmt.Errorf("dst-columns must start with 'id'")
	}

	// Формируем путь до файла в secure_file_priv и создаем его
	stagedPath := filepath.Join(secureDir, filepath.Base(srcPath)+".uuid.csv")
	tf, err := os.Create(stagedPath)
	if err != nil {
		return 0, "", fmt.Errorf("create staged: %w", err)
	}

	// Настраиваем врайтеры
	bw := bufio.NewWriterSize(tf, 1<<20)
	w := csv.NewWriter(bw)

	rows := 0

	// Построчно вычитываем записи из буфера, генерируем UUID,
	// пишем в буфер врайтера
	for {
		rec, rerr := cr.Read()

		if rerr == io.EOF {
			break
		}

		if rerr != nil {
			_ = tf.Close()
			_ = os.Remove(stagedPath)
			return 0, "", fmt.Errorf("csv read: %w", rerr)
		}

		if dtIdx >= len(rec) {
			_ = tf.Close()
			_ = os.Remove(stagedPath)
			return 0, "", fmt.Errorf("line %d: datetime index out of range (cols=%d)", rows+1, len(rec))
		}

		raw := strings.TrimSpace(rec[dtIdx])
		if raw == "" {
			_ = tf.Close()
			_ = os.Remove(stagedPath)
			return 0, "", fmt.Errorf("line %d: empty datetime", rows+1)
		}

		t, perr := time.ParseInLocation("2006-01-02 15:04:05", raw, loc)
		if perr != nil {
			_ = tf.Close()
			_ = os.Remove(stagedPath)
			return 0, "", fmt.Errorf("line %d: parse %q: %w", rows+1, raw, perr)
		}

		u, uerr := uuidv7.FromTime(t) // hex без дефисов
		if uerr != nil {
			_ = tf.Close()
			_ = os.Remove(stagedPath)
			return 0, "", uerr
		}

		out := make([]string, 0, len(rec)+1)
		out = append(out, u)
		out = append(out, rec...)

		if err := w.Write(out); err != nil {
			_ = tf.Close()
			_ = os.Remove(stagedPath)
			return 0, "", fmt.Errorf("staged write: %w", err)
		}

		rows++

		// Если строк больше 200 000, принудительно сгружаем буфер
		if rows%200_000 == 0 {
			w.Flush()
			if err := w.Error(); err != nil {
				_ = tf.Close()
				_ = os.Remove(stagedPath)
				return 0, "", fmt.Errorf("staged flush: %w", err)
			}
			_ = bw.Flush()

			log.Printf("[DEBUG] %s: staged %d rows to %s", filepath.Base(srcPath), rows, stagedPath)
		}
	}

	w.Flush()

	if err := w.Error(); err != nil {
		_ = tf.Close()
		_ = os.Remove(stagedPath)
		return 0, "", fmt.Errorf("staged final flush: %w", err)
	}

	if err := bw.Flush(); err != nil {
		_ = tf.Close()
		_ = os.Remove(stagedPath)
		return 0, "", err
	}

	if err := tf.Sync(); err != nil {
		_ = tf.Close()
		_ = os.Remove(stagedPath)
		return 0, "", err
	}

	if err := tf.Close(); err != nil {
		_ = os.Remove(stagedPath)
		return 0, "", err
	}

	return rows, stagedPath, nil
}

func buildLoadSQL(cfg config.LoadConfig, stagedPath string) string {
	dstCols := util.SplitCols(cfg.DstColumns)

	// переменные для CSV-колонок
	vars := make([]string, 0, len(dstCols))
	vars = append(vars, "@id_hex")
	for i := 1; i < len(dstCols); i++ {
		vars = append(vars, "@"+dstCols[i])
	}

	// маппинг в SET
	setClauses := []string{"id=UNHEX(@id_hex)"}
	for i := 1; i < len(dstCols); i++ {
		col := dstCols[i]
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

	return fmt.Sprintf(`
LOAD DATA INFILE '%s' INTO TABLE %s
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 0 LINES
(%s)
SET %s
`, file, util.Ident(cfg.DstTable), strings.Join(vars, ","), strings.Join(setClauses, ", "))
}

func prepareStageDir(db *sql.DB) (string, error) {
	var serverPriv sql.NullString

	if err := db.QueryRow("SELECT @@secure_file_priv").Scan(&serverPriv); err != nil {
		return "", fmt.Errorf("read secure_file_priv: %w", err)
	}

	if !serverPriv.Valid || strings.TrimSpace(serverPriv.String) == "" {
		return "", fmt.Errorf("secure_file_priv is NULL/empty; configure it in MySQL/MariaDB and restart")
	}

	secureDir := strings.TrimSpace(serverPriv.String)
	log.Printf("[DEBUG] secure_file_priv=%q (staging files will be written there)", secureDir)

	if err := os.MkdirAll(secureDir, 0o755); err != nil {
		log.Printf("[WARN] cannot create secure_file_priv dir from importer: %v", err)
	}

	return secureDir, nil
}

func printFinalStat(start time.Time, totalFiles, totalRows uint64) {
	dur := time.Since(start)
	if dur <= 0 {
		dur = time.Millisecond
	}

	speed := float64(totalRows) / dur.Seconds()

	log.Println("------------------------------------------------------------")
	log.Printf("[STATS] files: %s\n", util.FormatNumber(totalFiles))
	log.Printf("[STATS] rows: %s\n", util.FormatNumber(totalRows))
	log.Printf("[STATS] duration: %s\n", dur.Truncate(time.Second))
	log.Printf("[STATS] speed: %.0f rows/s\n", speed)
	log.Println("------------------------------------------------------------")
}
