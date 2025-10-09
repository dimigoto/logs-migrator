package config

import (
	"flag"
)

type ExportConfig struct {
	DSN            string
	Table          string
	PK             string
	Columns        string
	Where          string
	OutDir         string
	Workers        int
	ChunkSize      int
	ThrottleRPS    int
	MaxExecMS      int
	ProgressInline bool
}

func ParseExportConfig(args []string) ExportConfig {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	var c ExportConfig

	fs.StringVar(&c.DSN, "dsn", "", "MySQL DSN")
	fs.StringVar(&c.Table, "table", "", "Table name")
	fs.StringVar(&c.PK, "pk", "id", "Monotonic PK column (INT/BIGINT)")
	fs.StringVar(&c.Columns, "columns", "*", "Columns to export (comma-separated)")
	fs.StringVar(&c.Where, "where", "", "Optional WHERE (without 'WHERE')")
	fs.StringVar(&c.OutDir, "out", "./export", "Output directory")
	fs.IntVar(&c.Workers, "workers", 2, "Parallel workers")
	fs.IntVar(&c.ChunkSize, "chunk", 100000, "Rows per chunk file")
	fs.IntVar(&c.ThrottleRPS, "throttle-rows", 0, "Rows/sec per worker (0=off)")
	fs.IntVar(&c.MaxExecMS, "max-exec-ms", 0, "MAX_EXECUTION_TIME hint (ms)")
	fs.BoolVar(&c.ProgressInline, "progress-inline", true, "Render progress on one updating line")

	_ = fs.Parse(args)

	if c.Workers < 1 {
		c.Workers = 1
	}

	if c.ChunkSize < 1 {
		c.ChunkSize = 100000
	}

	return c
}
