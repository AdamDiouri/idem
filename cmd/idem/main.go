// Command idem runs the idem payments workflow engine.
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

	"github.com/spf13/cobra"

	"github.com/AdamDiouri/idem/internal/api"
	"github.com/AdamDiouri/idem/internal/config"
	"github.com/AdamDiouri/idem/internal/metrics"
	"github.com/AdamDiouri/idem/internal/store"
	"github.com/AdamDiouri/idem/internal/workflow"
)

const shutdownGrace = 10 * time.Second

func main() {
	root := &cobra.Command{
		Use:           "idem",
		Short:         "Effectively-once payments workflow engine",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(migrateCmd(), apiCmd(), workerCmd(), sweeperCmd(), checkCmd(), adminCmd())

	if err := root.Execute(); err != nil {
		slog.Error("idem failed", "error", err)
		os.Exit(exitCode(err))
	}
}

func migrateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "migrate",
		Short: "Apply pending database migrations",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// migrate needs only the database; it must not require API tokens.
			dsn := os.Getenv("IDEM_DATABASE_URL")
			if dsn == "" {
				return errors.New("IDEM_DATABASE_URL is required")
			}
			logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

			pool, err := store.Open(cmd.Context(), dsn)
			if err != nil {
				return err
			}
			defer pool.Close()

			if err := store.Migrate(cmd.Context(), pool); err != nil {
				return err
			}
			logger.Info("migrations applied")
			return nil
		},
	}
}

func apiCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "api",
		Short: "Serve the HTTP API",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			level, _ := cfg.SlogLevel() // validated by Load
			logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			pool, err := store.Open(ctx, cfg.DatabaseURL)
			if err != nil {
				return err
			}
			defer pool.Close()

			fees := workflow.FeeConfig{RateBps: cfg.FeeRateBps, FixedCents: cfg.FeeFixedCents}
			m := metrics.New() // served on the API's own address at /metrics
			handler := api.NewServer(api.Options{
				Metrics:        m,
				Tokens:         cfg.TokenToClient(),
				Logger:         logger,
				RequestTimeout: cfg.RequestTimeout,
			}, workflow.NewSubmitter(pool, cfg.IdempotencyLockTimeout, fees), pool)

			srv := &http.Server{
				Addr:              cfg.HTTPAddr,
				Handler:           handler,
				ReadHeaderTimeout: 5 * time.Second,
				ReadTimeout:       10 * time.Second,
				WriteTimeout:      cfg.HTTPWriteTimeout,
			}
			return serve(ctx, srv, logger)
		},
	}
}

// serve runs srv until ctx is cancelled, then drains in-flight requests.
func serve(ctx context.Context, srv *http.Server, logger *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		logger.Info("api listening", "addr", srv.Addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	logger.Info("shutting down", "grace", shutdownGrace.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	return nil
}
