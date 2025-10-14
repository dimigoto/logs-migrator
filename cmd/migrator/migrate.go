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

func migrate(args []string) {
	cfg := config.ParseMigrateConfig(args)

	// контекст с отменой по сигналу
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	// коннект к БД-источнику
	srcDb := dbx.MustOpen(cfg.SrcDSN, cfg.StageWorkers)
	defer srcDb.Close()
	log.Printf("[INFO] connection to source DB opened")

	// коннект к целевой БД
	dstDb := dbx.MustOpen(cfg.DstDSN, cfg.LoadWorkers)
	defer dstDb.Close()
	log.Printf("[INFO] connection to destination DB opened")

	// получаем secure_file_priv у целевой БД, чтобы туда складывать чанки для загрузки
	secureDir := getSecureDir(ctx, dstDb)
	log.Printf("[INFO] destination secure_file_priv=%q", secureDir)

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
	secureDir := dbx.GetSecureFilePriv(ctx, db)

	if err := os.MkdirAll(secureDir, 0o755); err != nil {
		log.Printf("[WARN] mkdir secure_file_priv: %v", err)
	}

	return secureDir
}
