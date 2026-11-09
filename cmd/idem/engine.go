package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/AdamDiouri/idem/internal/config"
	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/invariants"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/store"
)

// workerEnv is what worker, sweeper and check share: config, logger, a
// signal-cancelled context and a database pool.
type workerEnv struct {
	cfg    config.WorkerConfig
	logger *slog.Logger
	ctx    context.Context
	pool   *pgxpool.Pool
}

func setupWorkerEnv(cmd *cobra.Command) (*workerEnv, func(), error) {
	cfg, err := config.LoadWorker()
	if err != nil {
		return nil, nil, err
	}
	level, _ := cfg.SlogLevel() // validated by LoadWorker
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	pool, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		stop()
		return nil, nil, err
	}
	return &workerEnv{cfg: cfg, logger: logger, ctx: ctx, pool: pool}, func() { pool.Close(); stop() }, nil
}

func workerCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "worker",
		Short: "Claim and execute workflow steps",
		RunE: func(cmd *cobra.Command, _ []string) error {
			e, cleanup, err := setupWorkerEnv(cmd)
			if err != nil {
				return err
			}
			defer cleanup()

			if e.cfg.Failpoints != "" {
				if err := failpoint.EnableFromEnv(e.cfg.Failpoints); err != nil {
					return err
				}
				e.logger.Warn("failpoints enabled", "spec", e.cfg.Failpoints)
			}

			id := e.cfg.WorkerID
			if id == "" {
				host, _ := os.Hostname()
				id = fmt.Sprintf("%s-%d", host, os.Getpid())
			}
			client := provider.NewClient(e.cfg.ProviderURL, e.cfg.ProviderTimeout)
			return engine.NewWorker(e.pool, engine.WorkerOptions{
				ID:                 id,
				Steps:              []engine.Step{engine.NewChargeStep(client)},
				LeaseTTL:           e.cfg.LeaseTTL,
				PollInterval:       e.cfg.PollInterval,
				Backoff:            engine.Backoff{Base: e.cfg.RetryBase, Cap: e.cfg.RetryCap},
				KeyRetention:       e.cfg.ProviderKeyRetention,
				KeyRetentionMargin: e.cfg.KeyRetentionMargin,
				ShutdownGrace:      e.cfg.ShutdownGrace,
				Logger:             e.logger,
			}).Run(e.ctx)
		},
	}
}

func sweeperCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "sweeper",
		Short: "Reclaim steps whose worker lease expired",
		RunE: func(cmd *cobra.Command, _ []string) error {
			e, cleanup, err := setupWorkerEnv(cmd)
			if err != nil {
				return err
			}
			defer cleanup()
			e.logger.Info("sweeper started", "interval", e.cfg.SweepInterval.String())
			return engine.RunSweeper(e.ctx, e.pool, e.cfg.SweepInterval, e.cfg.StuckAfter, e.logger)
		},
	}
}

func checkCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "Check invariants against the database and the fake provider's effects oracle",
		RunE: func(cmd *cobra.Command, _ []string) error {
			e, cleanup, err := setupWorkerEnv(cmd)
			if err != nil {
				return err
			}
			defer cleanup()

			effects, err := fakeprovider.FetchEffects(e.ctx, e.cfg.ProviderURL)
			if err != nil {
				return err
			}
			violations, err := invariants.Check(e.ctx, e.pool, effects)
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out, "%d effects checked, %d violations\n", len(effects), len(violations))
			for _, v := range violations {
				_, _ = fmt.Fprintln(out, v.String())
			}
			if len(violations) > 0 {
				return fmt.Errorf("%d invariant violations", len(violations))
			}
			return nil
		},
	}
}
