// Command idem runs the idem payments workflow engine.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/AdamDiouri/idem/internal/api"
	"github.com/AdamDiouri/idem/internal/config"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/metrics"
	"github.com/AdamDiouri/idem/internal/store"
	"github.com/AdamDiouri/idem/internal/webhook"
	"github.com/AdamDiouri/idem/internal/workflow"
)

func main() {
	os.Exit(runIdem(os.Args[1:], os.Stdout, os.Stderr))
}

// newRootCmd builds the idem command tree.
func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "idem",
		Short:         "Effectively-once payments workflow engine",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(migrateCmd(), apiCmd(), workerCmd(), sweeperCmd(), relayCmd(), checkCmd(), adminCmd())
	markUsageErrors(root)
	return root
}

// runIdem runs idem with args and returns the process exit code. main and the
// tests share it.
func runIdem(args []string, stdout, stderr io.Writer) int {
	root := newRootCmd()
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	err := root.Execute()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "error:", err)
	}
	return exitCode(err)
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

			if cfg.Failpoints != "" {
				if err := failpoint.EnableFromEnv(cfg.Failpoints); err != nil {
					return err
				}
				logger.Warn("failpoints enabled", "spec", cfg.Failpoints)
			}
			var secrets [][]byte
			for _, s := range cfg.ProviderWebhookSecrets {
				secrets = append(secrets, []byte(s))
			}

			fees := workflow.FeeConfig{RateBps: cfg.FeeRateBps, FixedCents: cfg.FeeFixedCents}
			m := metrics.New() // served on the API's own address at /metrics
			m.WatchPool(pool)
			handler := api.NewServer(api.Options{
				Metrics:        m,
				Tokens:         cfg.TokenToClient(),
				Logger:         logger,
				RequestTimeout: cfg.RequestTimeout,
				WebhookSecrets: secrets, // POST /webhooks/provider is off without one
				Inbox:          webhook.NewInbox(pool),
				Workflows:      workflow.NewReader(pool), // GET /workflows/{id}
			}, workflow.NewSubmitter(pool, cfg.IdempotencyLockTimeout, fees), pool)

			if cfg.DebugAddr != "off" {
				dbg := &http.Server{Addr: cfg.DebugAddr, Handler: metrics.InternalHandler(nil), ReadHeaderTimeout: 5 * time.Second}
				go func() {
					logger.Info("pprof listening (internal only)", "addr", cfg.DebugAddr)
					if err := dbg.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
						logger.Error("debug server", "error", err)
					}
				}()
				defer func() { _ = dbg.Close() }()
			}

			srv := &http.Server{
				Addr:              cfg.HTTPAddr,
				Handler:           handler,
				ReadHeaderTimeout: 5 * time.Second,
				ReadTimeout:       10 * time.Second,
				WriteTimeout:      cfg.HTTPWriteTimeout,
			}
			return serve(ctx, srv, logger, cfg.ShutdownGrace)
		},
	}
}

// serve runs srv until ctx is cancelled, then drains in-flight requests for
// up to grace.
func serve(ctx context.Context, srv *http.Server, logger *slog.Logger, grace time.Duration) error {
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

	logger.Info("shutting down", "grace", grace.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	return nil
}
