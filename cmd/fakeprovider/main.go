// Command fakeprovider runs the fake payment provider used by tests, compose and chaos runs.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	addr := os.Getenv("FAKEPROVIDER_ADDR")
	if addr == "" {
		addr = ":9090"
	}

	// How long an accepted payout stays pending before it settles (design §4.1).
	settle := 2 * time.Second
	if v := os.Getenv("FAKEPROVIDER_PAYOUT_SETTLE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			logger.Error("FAKEPROVIDER_PAYOUT_SETTLE must be a non-negative duration", "value", v)
			os.Exit(2)
		}
		settle = d
	}

	// How long timeout faults hang (default 30s).
	var hang time.Duration
	if v := os.Getenv("FAKEPROVIDER_HANG"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			logger.Error("FAKEPROVIDER_HANG must be a positive duration", "value", v)
			os.Exit(2)
		}
		hang = d
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           fakeprovider.New(fakeprovider.Options{PayoutSettle: settle, Hang: hang}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("fake provider listening", "addr", addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("fake provider failed", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}
}
