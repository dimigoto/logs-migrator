package config

import (
	"flag"
	"log"
	"runtime"
)

type MigrateConfig struct {
	// Source DB
	SrcDSN    string
	SrcTable  string
	SrcFilter string
	SrcNID    string

	// Destination DB
	DstDSN   string
	DstTable string
	DstNID   string

	// UUIDv7
	TSColumnIdx int
	UUIDTZ      string

	// Performance
	StageWorkers int
	LoadWorkers  int
	ChunkSize    int
	ThrottleRPS  int
	MaxExecMS    int
}

func ParseMigrateConfig(args []string) MigrateConfig {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	var c MigrateConfig

	fs.StringVar(&c.SrcDSN, "src-dsn", "", "MariaDB DSN for source database (required)")
	fs.StringVar(&c.SrcTable, "src-table", "log", "Source table (default: log)")
	fs.StringVar(&c.SrcFilter, "src-filter", "", "Optional filter for query requests (example: id % 100 = 0)")
	fs.StringVar(&c.SrcNID, "src-nid", "id", "Source table numeric ID column name (default: id)")

	fs.StringVar(&c.DstDSN, "dst-dsn", "", "Percona DSN for destination database (required)")
	fs.StringVar(&c.DstTable, "dst-table", "log", "Destination table (default: log)")
	fs.StringVar(&c.DstNID, "dst-nid", "nid", "Destination table numeric ID column name (default: nid)")

	fs.IntVar(&c.TSColumnIdx, "ts-idx", 2, "The position of the column in source table that contains the date used to generate the UUIDv7 (default: 2)")
	fs.StringVar(&c.UUIDTZ, "uuid-tz", "America/Los_Angeles", "Destination table (default: America/Los_Angeles)")

	fs.IntVar(&c.StageWorkers, "sw", runtime.NumCPU(), "Parallel stage workers")
	fs.IntVar(&c.LoadWorkers, "lw", runtime.NumCPU(), "Parallel load workers")
	fs.IntVar(&c.ChunkSize, "chunk", 100_000, "Rows per chunk file (default: 100 000)")
	fs.IntVar(&c.ThrottleRPS, "throttle-rows", 0, "Rows/sec per worker (default: 0)")
	fs.IntVar(&c.MaxExecMS, "max-exec-ms", 0, "MAX_EXECUTION_TIME hint in ms (default: 0)")

	_ = fs.Parse(args)

	validateConfig(c)

	return c
}

func validateConfig(cfg MigrateConfig) {
	if cfg.SrcDSN == "" || cfg.DstDSN == "" {
		log.Fatalln("src-dsn and dst-dsn are required")
	}
}
