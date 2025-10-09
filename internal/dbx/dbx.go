package dbx

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"logs-migrator/internal/util"
	"strings"
	"time"

	"logs-migrator/internal/config"

	_ "github.com/go-sql-driver/mysql"
)

func MustOpen(dsn string, workers int) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}

	db.SetMaxOpenConns(workers + 2)
	db.SetMaxIdleConns(workers)
	db.SetConnMaxLifetime(5 * time.Minute)

	return db
}

func MustPKRange(ctx context.Context, db *sql.DB, cfg config.ExportConfig) (*int64, *int64) {
	where := strings.TrimSpace(cfg.Where)

	q := fmt.Sprintf(
		"SELECT MIN(%s), MAX(%s) FROM %s",
		util.Ident(cfg.PK),
		util.Ident(cfg.PK),
		util.Ident(cfg.Table),
	)

	if where != "" {
		q += " WHERE " + where
	}

	var a, b sql.NullInt64

	if err := db.QueryRowContext(ctx, q).Scan(&a, &b); err != nil {
		log.Fatalf("pk range: %v", err)
	}

	if !a.Valid || !b.Valid {
		return nil, nil
	}

	return &a.Int64, &b.Int64
}

func EnableFastLoad(ctx context.Context, db *sql.DB) {
	log.Printf("[INFO] Enabling fast-load")

	logExec(ctx, db, "SET GLOBAL unique_checks = 0")
	logExec(ctx, db, "SET GLOBAL foreign_key_checks = 0")

	logExec(ctx, db, "SET GLOBAL innodb_flush_log_at_trx_commit = 2")
	logExec(ctx, db, "SET GLOBAL sync_binlog = 0")
	logExec(ctx, db, "SET GLOBAL innodb_io_capacity = 2000")
	logExec(ctx, db, "SET GLOBAL innodb_io_capacity_max = 4000")

	logExec(ctx, db, "ALTER INSTANCE DISABLE INNODB REDO_LOG")

	log.Printf("[INFO] fast-load enabled")
}

func DisableFastLoad(db *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("[INFO] Disabling fast-load")

	logExec(ctx, db, "ALTER INSTANCE ENABLE INNODB REDO_LOG")

	logExec(ctx, db, "SET GLOBAL innodb_flush_log_at_trx_commit = 1")
	logExec(ctx, db, "SET GLOBAL sync_binlog = 1")

	logExec(ctx, db, "SET GLOBAL unique_checks = 1")
	logExec(ctx, db, "SET GLOBAL foreign_key_checks = 1")

	log.Printf("[INFO] fast-load disabled (restored defaults)")
}

func logExec(ctx context.Context, db *sql.DB, query string) {
	if _, err := db.ExecContext(ctx, query); err != nil {
		log.Printf("[WARN] fast-load: %s -> %v", strings.TrimSpace(query), err)
	} else {
		log.Printf("[DEBUG] fast-load applied: %s", strings.TrimSpace(query))
	}
}
