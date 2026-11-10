package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/AdamDiouri/idem/internal/config"
	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/invariants"
	"github.com/AdamDiouri/idem/internal/metrics"
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

// serveMetrics serves fresh metrics on cfg.MetricsAddr until the context ends.
// It returns nil (metrics disabled) when the address is "off".
func (e *workerEnv) serveMetrics() *metrics.Metrics {
	if e.cfg.MetricsAddr == "off" {
		return nil
	}
	m := metrics.New()
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	srv := &http.Server{Addr: e.cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		e.logger.Info("metrics listening", "addr", e.cfg.MetricsAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			e.logger.Error("metrics server", "error", err)
		}
	}()
	go func() {
		<-e.ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()
	return m
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
			steps := []engine.Step{engine.NewChargeStep(client), engine.NewPayoutStep(client), engine.NewRefundStep(client)}
			m := e.serveMetrics() // one listener, shared by both loops
			worker := engine.NewWorker(e.pool, engine.WorkerOptions{
				ID:                 id,
				Steps:              steps,
				Metrics:            m,
				LeaseTTL:           e.cfg.LeaseTTL,
				PollInterval:       e.cfg.PollInterval,
				Backoff:            engine.Backoff{Base: e.cfg.RetryBase, Cap: e.cfg.RetryCap},
				KeyRetention:       e.cfg.ProviderKeyRetention,
				KeyRetentionMargin: e.cfg.KeyRetentionMargin,
				CallbackDeadline:   e.cfg.CallbackDeadline,
				PollBackoff:        engine.Backoff{Base: e.cfg.RetryBase, Cap: e.cfg.CallbackPollCap},
				MaxWait:            e.cfg.CallbackMaxWait,
				MaxPolls:           e.cfg.CallbackMaxPolls,
				ShutdownGrace:      e.cfg.ShutdownGrace,
				Logger:             e.logger,
			})
			// Provider webhooks from the inbox (slice 2.2), next to the step loop.
			inbox := engine.NewInboxProcessor(e.pool, engine.InboxOptions{
				ID:           id + "-inbox",
				Steps:        steps,
				LeaseTTL:     e.cfg.LeaseTTL,
				PollInterval: e.cfg.PollInterval,
				OrphanAfter:  e.cfg.InboxOrphanAfter,
				Retry:        engine.Backoff{Base: e.cfg.RetryBase, Cap: e.cfg.RetryCap},
				Metrics:      m,
				Logger:       e.logger,
			})
			inboxErr := make(chan error, 1)
			go func() { inboxErr <- inbox.Run(e.ctx) }()
			return errors.Join(worker.Run(e.ctx), <-inboxErr)
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
			return engine.RunSweeper(e.ctx, e.pool, engine.SweeperOptions{
				Interval: e.cfg.SweepInterval, StuckAfter: e.cfg.StuckAfter, Logger: e.logger, Metrics: e.serveMetrics(),
			})
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

			attention, err := invariants.NeedsAttention(e.ctx, e.pool)
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out, "%d effects checked, %d violations, %d workflows need attention\n",
				len(effects), len(violations), len(attention))
			for _, id := range attention {
				_, _ = fmt.Fprintln(out, "needs attention:", id)
			}
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
