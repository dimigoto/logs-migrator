package dbx

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"logs-migrator/internal/util"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Список ключевых слов, указывающих на SQL-инъекцию
var dangerousKeywords = []string{
	"DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER",
	"CREATE", "REPLACE", "GRANT", "REVOKE", "EXECUTE", "CALL",
	"LOAD_FILE", "INTO OUTFILE", "INTO DUMPFILE",
}

// OriginalSettings структура, в которую сохраняем оригинальные настройки БД, чтобы восстановить после миграции данных
type OriginalSettings struct {
	UniqueChecks              int
	ForeignKeyChecks          int
	InnodbFlushLogAtTrxCommit int
	SyncBinlog                int
	InnodbIOCapacity          int
	InnodbIOCapacityMax       int
	InnodbBufferPoolSize      uint64
}

func MustOpen(dsn string, workers int, enableLocalInfile bool) *sql.DB {
	// Добавляем allowAllFiles=true для поддержки LOAD DATA LOCAL INFILE
	if enableLocalInfile {
		if strings.Contains(dsn, "?") {
			dsn += "&allowAllFiles=true"
		} else {
			dsn += "?allowAllFiles=true"
		}
		log.Printf("[DEBUG] LOCAL INFILE enabled in DSN")
	}

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

func MustMaxPk(ctx context.Context, db *sql.DB, tableName, pkColumnName string) uint64 {
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
		return empty
	}

	result := uint64(nid.Int64)

	return result
}

func MustPKRange(ctx context.Context, db *sql.DB, tableName, pkColumnName, filter string) (uint64, uint64) {
	var empty uint64 = 0

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
		return empty, empty
	}

	from := uint64(a.Int64)
	to := uint64(b.Int64)

	return from, to
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
	pkColumn string,
	where string,
) string {
	selectColumns := strings.Join(
		util.IdentAll(columns),
		",",
	)

	if strings.TrimSpace(where) != "" {
		where = " AND (" + where + ")"
	}

	pkIdent := util.Ident(pkColumn)

	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s > ? AND %s <= ?%s ORDER BY %s",
		selectColumns,
		util.Ident(tableName),
		pkIdent,
		pkIdent,
		where,
		pkIdent,
	)
}

func EnableFastLoad(ctx context.Context, db *sql.DB, bufferPoolSize uint64, ioCapacity, ioCapacityMax int) *OriginalSettings {
	log.Printf("[INFO] Enabling fast-load")

	// Сохраняем оригинальные значения
	orig := &OriginalSettings{}
	orig.UniqueChecks = getGlobalInt(ctx, db, "unique_checks")
	orig.ForeignKeyChecks = getGlobalInt(ctx, db, "foreign_key_checks")
	orig.InnodbFlushLogAtTrxCommit = getGlobalInt(ctx, db, "innodb_flush_log_at_trx_commit")
	orig.SyncBinlog = getGlobalInt(ctx, db, "sync_binlog")
	orig.InnodbIOCapacity = getGlobalInt(ctx, db, "innodb_io_capacity")
	orig.InnodbIOCapacityMax = getGlobalInt(ctx, db, "innodb_io_capacity_max")
	orig.InnodbBufferPoolSize = getGlobalUint64(ctx, db, "innodb_buffer_pool_size")

	log.Printf("[DEBUG] Original settings saved: unique_checks=%d, foreign_key_checks=%d, innodb_flush_log_at_trx_commit=%d, sync_binlog=%d, innodb_io_capacity=%d, innodb_io_capacity_max=%d, innodb_buffer_pool_size=%d",
		orig.UniqueChecks, orig.ForeignKeyChecks, orig.InnodbFlushLogAtTrxCommit, orig.SyncBinlog, orig.InnodbIOCapacity, orig.InnodbIOCapacityMax, orig.InnodbBufferPoolSize)

	// Применяем оптимизации
	logExec(ctx, db, "SET GLOBAL unique_checks = 0")
	logExec(ctx, db, "SET GLOBAL foreign_key_checks = 0")
	logExec(ctx, db, "SET GLOBAL innodb_flush_log_at_trx_commit = 2")
	logExec(ctx, db, "SET GLOBAL sync_binlog = 0")

	// Применяем пользовательские настройки InnoDB если указаны
	if bufferPoolSize > 0 {
		logExec(ctx, db, fmt.Sprintf("SET GLOBAL innodb_buffer_pool_size = %d", bufferPoolSize))
	}
	if ioCapacity > 0 {
		logExec(ctx, db, fmt.Sprintf("SET GLOBAL innodb_io_capacity = %d", ioCapacity))
	}
	if ioCapacityMax > 0 {
		logExec(ctx, db, fmt.Sprintf("SET GLOBAL innodb_io_capacity_max = %d", ioCapacityMax))
	}

	// Отключаем binlog для сессии
	logExec(ctx, db, "SET SESSION sql_log_bin = 0")

	// Отключаем REDO LOG
	logExec(ctx, db, "ALTER INSTANCE DISABLE INNODB REDO_LOG")

	log.Printf("[INFO] fast-load enabled")
	return orig
}

func DisableFastLoad(db *sql.DB, orig *OriginalSettings) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	log.Printf("[INFO] Disabling fast-load and restoring original settings")

	// Включаем REDO LOG
	logExec(ctx, db, "ALTER INSTANCE ENABLE INNODB REDO_LOG")

	// Включаем binlog обратно
	logExec(ctx, db, "SET SESSION sql_log_bin = 1")

	// Восстанавливаем оригинальные значения
	logExec(ctx, db, fmt.Sprintf("SET GLOBAL innodb_flush_log_at_trx_commit = %d", orig.InnodbFlushLogAtTrxCommit))
	logExec(ctx, db, fmt.Sprintf("SET GLOBAL sync_binlog = %d", orig.SyncBinlog))
	logExec(ctx, db, fmt.Sprintf("SET GLOBAL innodb_io_capacity = %d", orig.InnodbIOCapacity))
	logExec(ctx, db, fmt.Sprintf("SET GLOBAL innodb_io_capacity_max = %d", orig.InnodbIOCapacityMax))
	logExec(ctx, db, fmt.Sprintf("SET GLOBAL innodb_buffer_pool_size = %d", orig.InnodbBufferPoolSize))
	logExec(ctx, db, fmt.Sprintf("SET GLOBAL unique_checks = %d", orig.UniqueChecks))
	logExec(ctx, db, fmt.Sprintf("SET GLOBAL foreign_key_checks = %d", orig.ForeignKeyChecks))

	log.Printf("[INFO] fast-load disabled (original settings restored)")
}

func logExec(ctx context.Context, db *sql.DB, query string) {
	if _, err := db.ExecContext(ctx, query); err != nil {
		log.Printf("[WARN] fast-load: %s -> %v", strings.TrimSpace(query), err)
	} else {
		log.Printf("[DEBUG] fast-load applied: %s", strings.TrimSpace(query))
	}
}

func getGlobalInt(ctx context.Context, db *sql.DB, varName string) int {
	var val int
	query := fmt.Sprintf("SELECT @@GLOBAL.%s", varName)
	if err := db.QueryRowContext(ctx, query).Scan(&val); err != nil {
		log.Printf("[WARN] failed to read %s: %v, using default 1", varName, err)
		return 1
	}
	return val
}

func getGlobalUint64(ctx context.Context, db *sql.DB, varName string) uint64 {
	var val uint64
	query := fmt.Sprintf("SELECT @@GLOBAL.%s", varName)
	if err := db.QueryRowContext(ctx, query).Scan(&val); err != nil {
		log.Printf("[WARN] failed to read %s: %v, using default 0", varName, err)
		return 0
	}
	return val
}

// BuildLoadDataSQL генерирует LOAD DATA INFILE SQL для файловой загрузки в БД
func BuildLoadDataSQL(stagedPath, dstTable, uuidCol string, columns []string, useLocal bool) string {
	if len(columns) == 0 {
		return ""
	}

	// CSV переменные: @id_hex для UUID
	vars := make([]string, 0, len(columns))
	vars = append(vars, "@id_hex")
	for i := 1; i < len(columns); i++ {
		vars = append(vars, "@"+columns[i])
	}

	// Преобразовать шестнадцатеричный UUID в BINARY(16), обработать временные метки (timestamps) и значения NULL
	setClauses := []string{util.Ident(uuidCol) + "=UNHEX(@id_hex)"}
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

	// Экранируем путь к файлу
	file := strings.ReplaceAll(stagedPath, `\`, `\\`)
	file = strings.ReplaceAll(file, `'`, `\'`)

	// Выбираем каким способом будем грузить: LOAD DATA INFILE (server) или LOAD DATA LOCAL INFILE (client)
	loadCmd := "LOAD DATA INFILE"
	if useLocal {
		loadCmd = "LOAD DATA LOCAL INFILE"
	}

	return fmt.Sprintf(
		`%s '%s' INTO TABLE %s
				FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
				LINES TERMINATED BY '\n'
				IGNORE 0 LINES
				(%s)
				SET %s`,
		loadCmd,
		file,
		util.Ident(dstTable),
		strings.Join(vars, ","),
		strings.Join(setClauses, ", "),
	)
}

// ValidateWhereClause проверяем есть ли в фильтре потенциальные SQL-инъекции
func ValidateWhereClause(where string) error {
	if strings.TrimSpace(where) == "" {
		return nil
	}

	upperWhere := strings.ToUpper(where)

	// Проверяем ключевые слова (только как отдельные слова, не как части других слов)
	for _, keyword := range dangerousKeywords {
		// Добавляем word boundary проверку с помощью regexp
		pattern := regexp.MustCompile(`\b` + keyword + `\b`)
		if pattern.MatchString(upperWhere) {
			return fmt.Errorf("WHERE clause contains forbidden keyword: %s", keyword)
		}
	}

	// SQL-инъекция через комментарии
	if strings.Contains(where, "--") || strings.Contains(where, "/*") || strings.Contains(where, "*/") {
		return fmt.Errorf("WHERE clause contains forbidden comment syntax")
	}

	// Множественные SQL-инструкции
	if strings.Contains(where, ";") {
		return fmt.Errorf("WHERE clause contains forbidden semicolon")
	}

	// Попытка экранировать идентификаторы
	backtickPattern := regexp.MustCompile("`;|`\\s*;|;\\s*`")
	if backtickPattern.MatchString(where) {
		return fmt.Errorf("WHERE clause contains suspicious backtick usage")
	}

	return nil
}
