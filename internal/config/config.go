package config

import (
	"flag"
	"log"
	"runtime"

	"logs-migrator/internal/dbx"
)

type Config struct {
	// БД-источник
	SrcDSN    string
	SrcTable  string
	SrcFilter string
	SrcNID    string

	// Целевая БД
	DstDSN   string
	DstTable string
	DstNID   string
	DstUuid  string

	// UUIDv7
	TSColumnIdx int
	UUIDTZ      string

	// Производительность
	StageWorkers int
	LoadWorkers  int
	ChunkSize    int

	// Оптимизация целевой БД, на момент миграции
	InnodbBufferPoolSize uint64
	InnodbIOCapacity     int
	InnodbIOCapacityMax  int

	// Load mode
	UseLocalInfile bool
	UseFastLoad    bool
}

func ParseConfig(args []string) Config {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	var c Config

	fs.StringVar(&c.SrcDSN, "src-dsn", "", "MariaDB DSN for source database (required)")
	fs.StringVar(&c.SrcTable, "src-table", "log", "Source table (default: log)")
	fs.StringVar(&c.SrcFilter, "src-filter", "", "Optional filter for query requests (example: id % 100 = 0)")
	fs.StringVar(&c.SrcNID, "src-nid", "id", "Source table numeric ID column name (default: id)")

	fs.StringVar(&c.DstDSN, "dst-dsn", "", "Percona DSN for destination database (required)")
	fs.StringVar(&c.DstTable, "dst-table", "log", "Destination table (default: log)")
	fs.StringVar(&c.DstNID, "dst-nid", "nid", "Destination table numeric ID column name (default: nid)")
	fs.StringVar(&c.DstUuid, "dst-uuid", "id", "Destination table UUID column name (default: id)")

	fs.IntVar(&c.TSColumnIdx, "ts-idx", 2, "The position of the column in source table that contains the date used to generate the UUIDv7 (default: 2)")
	fs.StringVar(&c.UUIDTZ, "uuid-tz", "UTC", "Destination table (default: UTC)")

	fs.IntVar(&c.StageWorkers, "sw", runtime.NumCPU(), "Parallel stage workers")
	fs.IntVar(&c.LoadWorkers, "lw", runtime.NumCPU(), "Parallel load workers")
	fs.IntVar(&c.ChunkSize, "chunk", 100_000, "Rows per chunk file (default: 100 000)")

	// Database optimization
	var bufferPoolGB float64
	fs.Float64Var(&bufferPoolGB, "innodb-buffer-pool-gb", 0, "InnoDB buffer pool size in GB (0 = don't change, default: 0)")
	fs.IntVar(&c.InnodbIOCapacity, "innodb-io-capacity", 0, "InnoDB IO capacity (0 = don't change, default: 0)")
	fs.IntVar(&c.InnodbIOCapacityMax, "innodb-io-capacity-max", 0, "InnoDB IO capacity max (0 = don't change, default: 0)")

	// Load mode
	fs.BoolVar(&c.UseLocalInfile, "local-infile", false, "Use LOAD DATA LOCAL INFILE (files on client) instead of LOAD DATA INFILE (files on server)")
	fs.BoolVar(&c.UseFastLoad, "fast-load", true, "Enable fast load optimizations: disable unique/FK checks, binlog, redo log (default: true)")

	_ = fs.Parse(args)

	// Convert GB to bytes
	if bufferPoolGB > 0 {
		c.InnodbBufferPoolSize = uint64(bufferPoolGB * 1024 * 1024 * 1024)
	}

	validateConfig(c)

	return c
}

func validateConfig(cfg Config) {
	if cfg.SrcDSN == "" || cfg.DstDSN == "" {
		log.Fatalln("src-dsn and dst-dsn are required")
	}

	// Валидируем врокеры
	if cfg.StageWorkers < 1 {
		log.Fatalln("stage workers must be at least 1")
	}
	if cfg.StageWorkers > 100 {
		log.Fatalf("stage workers must be between 1 and 100, got %d", cfg.StageWorkers)
	}

	if cfg.LoadWorkers < 1 {
		log.Fatalln("load workers must be at least 1")
	}
	if cfg.LoadWorkers > 100 {
		log.Fatalf("load workers must be between 1 and 100, got %d", cfg.LoadWorkers)
	}

	// Валидируем размер чанка
	if cfg.ChunkSize < 1 {
		log.Fatalln("chunk size must be at least 1")
	}
	if cfg.ChunkSize > 10_000_000 {
		log.Fatalf("chunk size must be between 1 and 10,000,000, got %d", cfg.ChunkSize)
	}

	// Валидируем SQL-инъекции
	if err := dbx.ValidateWhereClause(cfg.SrcFilter); err != nil {
		log.Fatalf("invalid source filter: %v", err)
	}

	// Валидируем индекс колонки с TS
	if cfg.TSColumnIdx < 1 {
		log.Fatalln("ts-idx must be at least 1")
	}
}
