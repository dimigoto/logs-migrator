package exporter

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"logs-migrator/internal/filesink"
	"logs-migrator/internal/util"
	_ "math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"logs-migrator/internal/config"
	"logs-migrator/internal/ranger"
)

func Run(
	ctx context.Context,
	db *sql.DB,
	cfg config.ExportConfig,
	shards []ranger.Range,
	totalRows, totalFiles *uint64,
) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(shards))

	for i, sh := range shards {
		wg.Add(1)

		go func(wid int, from, to int64) {
			defer wg.Done()
			if err := runWorker(ctx, db, cfg, wid, from, to, totalRows, totalFiles); err != nil {
				errCh <- fmt.Errorf("worker %d: %w", wid, err)
			}
		}(i+1, sh.From, sh.To)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

func runWorker(
	ctx context.Context,
	db *sql.DB,
	cfg config.ExportConfig,
	wid int,
	from, to int64,
	totalRows, totalFiles *uint64,
) error {
	log.Printf("[W%d] range [%d..%d]", wid, from, to)

	query := prepareQuery(cfg)
	last := from - 1

	fileSink := filesink.New(
		cfg.OutDir,
		fmt.Sprintf("%s_%02d", cfg.Table, wid),
		cfg.ChunkSize,
	)
	defer func() {
		if fileSink.RowsInChunk() > 0 {
			rows := fileSink.RowsInChunk()
			_ = fileSink.Close()
			atomic.AddUint64(totalFiles, 1)
			atomic.AddUint64(totalRows, uint64(rows))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		args := []any{last, to, cfg.ChunkSize}
		var rows *sql.Rows
		var err error

		rows, err = db.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}

		names, _ := rows.Columns()
		vals := make([]any, len(names))
		ptrs := make([]any, len(names))

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		read := 0

		pkIdx := util.IndexOf(names, cfg.PK)
		if pkIdx == -1 {
			return fmt.Errorf("there is no primary key named %s", cfg.PK)
		}

		for rows.Next() {
			if e := rows.Scan(ptrs...); e != nil {
				_ = rows.Close()
				return e
			}

			rec := make([]string, len(vals))
			for i, v := range vals {
				switch x := v.(type) {
				case nil:
					rec[i] = ""
				case []byte:
					rec[i] = string(x)
				case time.Time:
					rec[i] = x.UTC().Format("2006-01-02 15:04:05")
				default:
					rec[i] = fmt.Sprint(x)
				}
			}

			if pk, _ := strconv.ParseInt(rec[pkIdx], 10, 64); pk > last {
				last = pk
			}

			if e := fileSink.Write(rec); e != nil {
				_ = rows.Close()
				return e
			}

			rotated, rowsClosed, e := fileSink.RotateIfNeeded()
			if e != nil {
				_ = rows.Close()
				return e
			}

			if rotated {
				atomic.AddUint64(totalFiles, 1)
				atomic.AddUint64(totalRows, uint64(rowsClosed))

				if cfg.ThrottleRPS > 0 {
					time.Sleep(time.Duration(float64(cfg.ChunkSize)/float64(cfg.ThrottleRPS)*1000) * time.Millisecond)
				}
			}

			read++
		}

		_ = rows.Close()

		if read == 0 {
			return nil
		}
	}
}

func prepareQuery(cfg config.ExportConfig) string {
	cols := strings.TrimSpace(cfg.Columns)
	if cols == "" {
		cols = "*"
	}

	where := fmt.Sprintf(
		"%s > ? AND %s <= ?",
		util.Ident(cfg.PK),
		util.Ident(cfg.PK),
	)

	if cfg.Where != "" {
		where = "(" + cfg.Where + ") AND " + where
	}

	maxExec := ""
	if cfg.MaxExecMS > 0 {
		maxExec = fmt.Sprintf("SET STATEMENT max_statement_time=%.3f FOR ", float64(cfg.MaxExecMS)/1000)
	}

	return fmt.Sprintf(
		"%sSELECT %s FROM %s WHERE %s ORDER BY %s ASC LIMIT ?",
		maxExec,
		cols,
		util.Ident(cfg.Table),
		where,
		util.Ident(cfg.PK),
	)
}
