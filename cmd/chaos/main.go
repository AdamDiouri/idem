// Command chaos runs a chaos campaign against idem: the real binaries as
// separate processes, misbehaving clients, and a seeded plan of crashes,
// pauses, restarts and provider faults, followed by an invariant verdict.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/AdamDiouri/idem/internal/chaos"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg := chaos.DefaultConfig()
	flag.Int64Var(&cfg.Seed, "seed", time.Now().UnixNano()%1_000_000, "fault plan seed (printed; reuse it to reproduce the plan)")
	flag.DurationVar(&cfg.Duration, "duration", cfg.Duration, "how long traffic and faults run")
	flag.DurationVar(&cfg.SettleTimeout, "settle-timeout", cfg.SettleTimeout, "how long the system may take to settle afterwards")
	flag.IntVar(&cfg.Workflows, "workflows", cfg.Workflows, "workflows to submit")
	flag.IntVar(&cfg.Workers, "workers", cfg.Workers, "worker processes")
	flag.IntVar(&cfg.APIs, "apis", cfg.APIs, "API replicas (clients and webhooks rotate across them)")
	flag.IntVar(&cfg.Sweepers, "sweepers", cfg.Sweepers, "sweeper processes")
	flag.IntVar(&cfg.Relays, "relays", cfg.Relays, "relay processes delivering outcomes to the fake shop")
	flag.StringVar(&cfg.BinDir, "bin-dir", cfg.BinDir, "directory with the idem, fakeprovider and fakeshop binaries")
	out := flag.String("out", "chaos-runs", "parent directory for run artifacts")
	flag.Parse()

	cfg.RunDir = filepath.Join(*out, fmt.Sprintf("%s-seed%d", time.Now().Format("20060102-150405"), cfg.Seed))
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("chaos campaign", "seed", cfg.Seed, "run_dir", cfg.RunDir)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rep, err := chaos.Run(ctx, cfg, logger)
	if err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			logger.Error("campaign interrupted; processes and container cleaned up")
		} else {
			logger.Error("campaign could not run", "error", err)
		}
		return 2
	}
	fmt.Print(rep.Text())
	fmt.Printf("\nartifacts: %s\n", cfg.RunDir)
	if !rep.Passed {
		return 1
	}
	return 0
}
