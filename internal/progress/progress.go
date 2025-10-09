package progress

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"sync/atomic"
	"time"

	"logs-migrator/internal/config"
	"logs-migrator/internal/util"
)

const progressEvery = 1 * time.Second

type Reporter struct {
	cfg         config.ExportConfig
	min, max    int64
	rowsPtr     *uint64
	filesPtr    *uint64
	start       time.Time
	doneCh      chan struct{}
	inline      bool
	progressTkr *time.Ticker
}

func New(cfg config.ExportConfig, min, max int64, rowsPtr, filesPtr *uint64, start time.Time) *Reporter {
	return &Reporter{
		cfg:      cfg,
		min:      min,
		max:      max,
		rowsPtr:  rowsPtr,
		filesPtr: filesPtr,
		start:    start,
		doneCh:   make(chan struct{}),
		inline:   cfg.ProgressInline && isTerminal(),
	}
}

func (r *Reporter) Start(ctx context.Context) {
	r.progressTkr = time.NewTicker(progressEvery)
	go func() {
		defer close(r.doneCh)
		defer r.progressTkr.Stop()
		totalPlanned := float64((r.max - r.min) + 1)

		for {
			select {
			case <-r.progressTkr.C:
				rows := atomic.LoadUint64(r.rowsPtr)
				files := atomic.LoadUint64(r.filesPtr)
				elapsed := time.Since(r.start).Seconds()
				rps := float64(rows) / math.Max(elapsed, 0.001)
				pct := 100.0 * float64(rows) / math.Max(totalPlanned, 1)
				if pct > 100 {
					pct = 100
				}
				eta := ""
				if rps > 0 && totalPlanned > 0 {
					remain := totalPlanned - float64(rows)
					if remain < 0 {
						remain = 0
					}
					etaDur := time.Duration(remain/rps) * time.Second
					eta = etaDur.Truncate(time.Second).String()
				}
				line := fmt.Sprintf("[PROGRESS] rows=%s (%.0f/s) files=%s %.1f%% ETA=%s",
					util.FormatNumber(rows), rps, util.FormatNumber(files), pct, eta)

				if r.inline {
					fmt.Fprintf(os.Stdout, "\r\033[2K%s", line)
				} else {
					log.Print(line)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (r *Reporter) WaitAndFinish() {
	<-r.doneCh
	if r.inline {
		fmt.Fprintln(os.Stdout)
	}
}

func isTerminal() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
