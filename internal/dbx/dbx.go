package dbx

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"logs-migrator/internal/util"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const dateLayout = "2006-01-02 15:04:05"

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

func GetSecureFilePriv(ctx context.Context, db *sql.DB) string {
	var serverPriv sql.NullString

	row := db.QueryRowContext(ctx, "SELECT @@secure_file_priv")
	if err := row.Scan(&serverPriv); err != nil {
		log.Fatalf("read secure_file_priv: %v", err)
	}

	if !serverPriv.Valid || strings.TrimSpace(serverPriv.String) == "" {
		log.Fatalf("secure_file_priv is NULL/empty; configure it in MySQL/MariaDB and restart")
	}

	return strings.TrimSpace(serverPriv.String)
}

func MustMaxPk(ctx context.Context, db *sql.DB, tableName, pkColumnName string) *uint64 {
	query := fmt.Sprintf(
		"SELECT MAX(%s) FROM %s",
		util.Ident(pkColumnName),
		util.Ident(tableName),
	)

	var nid sql.NullInt64
	var empty uint64 = 0

	if err := db.QueryRowContext(ctx, query).Scan(&nid); err != nil {
		log.Fatalf("get max numeric id: %v", err)
	}

	if !nid.Valid {
		return &empty
	}

	result := uint64(nid.Int64)

	return &result
}

func MustPKRange(ctx context.Context, db *sql.DB, tableName, pkColumnName, filter string) (*uint64, *uint64) {
	where := strings.TrimSpace(filter)

	q := fmt.Sprintf(
		"SELECT MIN(%s), MAX(%s) FROM %s",
		util.Ident(pkColumnName),
		util.Ident(pkColumnName),
		util.Ident(tableName),
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

	from := uint64(a.Int64)
	to := uint64(b.Int64)

	return &from, &to
}

func MustTableColumns(ctx context.Context, db *sql.DB, table string) []string {
	q := `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`
	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		log.Fatalf("get columns for %s: %v\n", table, err)
	}
	defer rows.Close()

	var columns []string

	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			log.Fatalf("%v\n", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("%v\n", err)
	}

	if len(columns) == 0 {
		log.Fatalf("no columns found for %s\n", table)
	}

	return columns
}

func BuildSelectByRange(
	tableName string,
	columns []string,
	where string,
) string {
	selectColumns := strings.Join(
		util.IdentAll(columns),
		",",
	)

	if strings.TrimSpace(where) != "" {
		where = " AND (" + where + ")"
	}

	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE `id` > ? AND `id` <= ?%s ORDER BY `id`",
		selectColumns,
		util.Ident(tableName),
		where,
	)
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

func AsString(value any) string {
	switch x := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format(dateLayout)
	default:
		return fmt.Sprint(x)
	}
}

func logExec(ctx context.Context, db *sql.DB, query string) {
	if _, err := db.ExecContext(ctx, query); err != nil {
		log.Printf("[WARN] fast-load: %s -> %v", strings.TrimSpace(query), err)
	} else {
		log.Printf("[DEBUG] fast-load applied: %s", strings.TrimSpace(query))
	}
}
