package main

import (
	"log"
	"os"

	"logs-migrator/internal/config"
	"logs-migrator/internal/dbx"
	"logs-migrator/internal/importer"
)

func load(args []string) {
	cfg := config.ParseImport(args)

	if cfg.DSN == "" || cfg.TarPath == "" || cfg.DstTable == "" || cfg.DstColumns == "" {
		log.Println("Usage:")
		log.Println("  logs-migrator load -dsn DSN -tar export.tar.gz -dst-table log -dst-columns \"id,ins_ts,...\" [flags]")
		os.Exit(2)
	}

	db := dbx.MustOpen(cfg.DSN, cfg.Workers)
	defer db.Close()

	if err := importer.Run(cfg, db); err != nil {
		log.Fatalf("[FATAL] import: %v", err)
	}
}
