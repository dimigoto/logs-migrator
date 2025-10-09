package config

import (
	"flag"
	"runtime"
)

type LoadConfig struct {
	DSN         string
	TarPath     string
	DstTable    string
	DstColumns  string
	UUIDFromIdx int
	UUIDTZ      string
	Workers     int
}

func ParseImport(args []string) LoadConfig {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	var c LoadConfig

	fs.StringVar(&c.DSN, "dsn", "", "MySQL DSN for NEW database (required)")
	fs.StringVar(&c.TarPath, "tar", "", "Path to export.tar.gz (required)")
	fs.StringVar(&c.DstTable, "dst-table", "", "Destination table (required)")
	fs.StringVar(&c.DstColumns, "dst-columns", "", "Destination columns including id first (required)")
	fs.IntVar(&c.UUIDFromIdx, "uuidv7-from-index", 0, "1-based index of datetime column")
	fs.StringVar(&c.UUIDTZ, "uuidv7-tz", "UTC", "IANA timezone of datetime in CSV")
	fs.IntVar(&c.Workers, "workers", runtime.NumCPU(), "Parallel files importer workers")

	_ = fs.Parse(args)

	if c.Workers < 1 {
		c.Workers = 1
	}

	return c
}
