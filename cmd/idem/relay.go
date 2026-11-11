package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/AdamDiouri/idem/internal/config"
	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/metrics"
	"github.com/AdamDiouri/idem/internal/relay"
	"github.com/AdamDiouri/idem/internal/store"
)

func relayCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "relay",
		Short: "Deliver workflow outcomes to shops (outbox → signed webhooks)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.LoadRelay()
			if err != nil {
				return usageError{err} // a misconfigured client is fixed in the environment, not retried
			}
			level, _ := cfg.SlogLevel() // validated by LoadRelay
			logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
			if cfg.Failpoints != "" {
				if err := failpoint.EnableFromEnv(cfg.Failpoints); err != nil {
					return err
				}
				logger.Warn("failpoints enabled", "spec", cfg.Failpoints)
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			pool, err := store.Open(ctx, cfg.DatabaseURL)
			if err != nil {
				return err
			}
			defer pool.Close()

			id := cfg.WorkerID
			if id == "" {
				host, _ := os.Hostname()
				id = fmt.Sprintf("%s-%d", host, os.Getpid())
			}
			clients := make(map[string]relay.Endpoint, len(cfg.ClientWebhooks))
			for client, url := range cfg.ClientWebhooks {
				clients[client] = relay.Endpoint{URL: url, Secret: []byte(cfg.ClientWebhookSecrets[client])}
			}
			logger.Info("relay configured", "relay_id", id, "clients", slices.Sorted(maps.Keys(clients)))
			// D19: events of clients without a URL are kept as NO_ENDPOINT, not
			// delivered and not alerted (they may poll GET /workflows/{id}).
			if missing, err := clientsWithoutEndpoint(ctx, pool, clients); err != nil {
				logger.Warn("list clients without a webhook URL", "error", err)
			} else if len(missing) > 0 {
				logger.Info("clients without a webhook URL; their events become NO_ENDPOINT", "clients", missing)
			}

			var m *metrics.Metrics
			if cfg.MetricsAddr != "off" {
				m = metrics.New()
				m.WatchPool(pool)
				srv := &http.Server{Addr: cfg.MetricsAddr, Handler: metrics.InternalHandler(m), ReadHeaderTimeout: 5 * time.Second}
				go func() { _ = srv.ListenAndServe() }()
				defer func() { _ = srv.Close() }()
			}
			return relay.New(pool, relay.Options{
				ID: id, Clients: clients, LeaseTTL: cfg.LeaseTTL, Timeout: cfg.Timeout, PollInterval: cfg.PollInterval,
				ShutdownGrace: cfg.ShutdownGrace, MaxAttempts: cfg.MaxAttempts,
				Retry:   engine.Backoff{Base: cfg.RetryBase, Cap: cfg.RetryCap},
				Metrics: m, Logger: logger,
			}).Run(ctx)
		},
	}
}

// clientsWithoutEndpoint lists the clients with workflows but no webhook URL.
func clientsWithoutEndpoint(ctx context.Context, pool *pgxpool.Pool, clients map[string]relay.Endpoint) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT DISTINCT client_id FROM workflows ORDER BY client_id`)
	if err != nil {
		return nil, err
	}
	all, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, c := range all {
		if _, ok := clients[c]; !ok {
			missing = append(missing, c)
		}
	}
	return missing, nil
}
