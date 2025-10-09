package main

import (
	"context"
	"log"
	"math"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"logs-migrator/internal/archive"
	"logs-migrator/internal/config"
	"logs-migrator/internal/dbx"
	"logs-migrator/internal/exporter"
	"logs-migrator/internal/progress"
	"logs-migrator/internal/ranger"
	"logs-migrator/internal/util"
)

func export(args []string) {
	cfg := config.ParseExportConfig(args) // флаги → конфиг
	log.SetOutput(os.Stderr)

	// контекст с отменой по сигналу
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	// БД
	db := dbx.MustOpen(cfg.DSN, cfg.Workers)
	defer db.Close()

	// диапазон PK
	minPK, maxPK := dbx.MustPKRange(ctx, db, cfg)
	if minPK == nil || maxPK == nil || *maxPK < *minPK {
		log.Println("[INFO] No rows.")
		return
	}

	log.Printf("[INFO] PK range: [%d..%d]", *minPK, *maxPK)

	// шардируем
	shards := ranger.Split(*minPK, *maxPK, cfg.Workers)

	// метрики экспорта
	var totalRows, totalFiles uint64
	start := time.Now()

	// прогресс
	var prog *progress.Reporter
	if cfg.ProgressEvery > 0 {
		prog = progress.New(cfg, *minPK, *maxPK, &totalRows, &totalFiles, start)
		prog.Start(ctx)
	}

	// экспорт
	if err := exporter.Run(ctx, db, cfg, shards, &totalRows, &totalFiles); err != nil {
		cancel()
		log.Fatalf("[FATAL] export: %v", err)
	}

	// корректно завершим прогресс
	cancel()
	if prog != nil {
		prog.WaitAndFinish()
	}

	printFinalStat(start, totalRows, totalFiles)
	archiveAndSafeRemove(cfg.OutDir)
}

func printFinalStat(start time.Time, totalRows, totalFiles uint64) {
	elapsed := time.Since(start)
	rows := atomic.LoadUint64(&totalRows)
	files := atomic.LoadUint64(&totalFiles)
	rps := float64(rows) / math.Max(elapsed.Seconds(), 0.0001)
	avg := uint64(0)
	if files > 0 {
		avg = rows / files
	}

	log.Println("------------------------------------------------------------")
	log.Printf("[STATS] rows: %s\n", util.FormatNumber(rows))
	log.Printf("[STATS] chunks(files): %s\n", util.FormatNumber(files))
	log.Printf("[STATS] avg rows/chunk: %s\n", util.FormatNumber(avg))
	log.Printf("[STATS] elapsed: %s\n", elapsed.Truncate(time.Second))
	log.Printf("[STATS] speed: %.0f rows/s\n", rps)
	log.Println("------------------------------------------------------------")
}

func archiveAndSafeRemove(outDir string) {
	archivePath := outDir + ".tar.gz"
	startZip := time.Now()

	if err := archive.TarGzDir(outDir, archivePath); err != nil {
		log.Printf("[WARN] cannot archive export dir: %v", err)
	} else {
		dur := time.Since(startZip).Truncate(time.Second)
		log.Printf("[INFO] archive created: %s (in %s)", archivePath, dur)

		if err := util.SafeRemoveDir(outDir); err != nil {
			log.Printf("[WARN] export dir not removed: %v", err)
		} else {
			log.Printf("[INFO] removed export dir: %s", outDir)
		}
	}
}
