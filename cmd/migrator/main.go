package main

import (
	"context"
	"database/sql"
	"log"
	"logs-migrator/internal/config"
	"logs-migrator/internal/dbx"
	"logs-migrator/internal/migrator"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := config.ParseConfig(os.Args[1:])

	// контекст с отменой по сигналу
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	// коннект к БД-источнику
	srcDb := dbx.MustOpen(cfg.SrcDSN, cfg.StageWorkers, false)
	defer func() {
		if err := srcDb.Close(); err != nil {
			log.Printf("[WARN] failed to close source DB connection: %v", err)
		}
	}()
	log.Printf("[INFO] connection to source DB opened")

	// коннект к целевой БД (с поддержкой LOCAL INFILE если нужно)
	dstDb := dbx.MustOpen(cfg.DstDSN, cfg.LoadWorkers, cfg.UseLocalInfile)
	defer func() {
		if err := dstDb.Close(); err != nil {
			log.Printf("[WARN] failed to close destination DB connection: %v", err)
		}
	}()
	log.Printf("[INFO] connection to destination DB opened")

	// Определяем папку для временных файлов
	var secureDir string
	if cfg.UseLocalInfile {
		// Для LOCAL INFILE используем временную папку на клиенте
		secureDir = os.TempDir()
		log.Printf("[INFO] using LOCAL INFILE mode, temp dir: %q", secureDir)
	} else {
		// Для INFILE используем secure_file_priv на сервере
		secureDir = getSecureDir(ctx, dstDb)
		log.Printf("[INFO] using server INFILE mode, secure_file_priv=%q", secureDir)
	}

	if err := migrator.Run(
		ctx,
		srcDb,
		dstDb,
		secureDir,
		cfg,
	); err != nil {
		log.Fatalln(err)
	}
}

func getSecureDir(ctx context.Context, db *sql.DB) string {
	return dbx.GetSecureFilePriv(ctx, db)
}
