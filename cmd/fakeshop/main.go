// Command fakeshop runs a fake shop that receives idem's outbound webhooks,
// for compose and chaos runs (M2 design §5.4).
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

	"github.com/AdamDiouri/idem/internal/fakeshop"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	addr := os.Getenv("FAKESHOP_ADDR")
	if addr == "" {
		addr = ":8000"
	}
	secret := os.Getenv("FAKESHOP_SECRET")
	if len(secret) < 16 {
		logger.Error("FAKESHOP_SECRET is required (at least 16 characters): the shop verifies every delivery")
		os.Exit(2)
	}
	slow, ok := duration(logger, "FAKESHOP_SLOW", 2*time.Second)
	lateAck, ok2 := duration(logger, "FAKESHOP_LATE_ACK", 15*time.Second)
	if !ok || !ok2 {
		os.Exit(2)
	}

	shop := fakeshop.New(fakeshop.Options{Secret: []byte(secret), Slow: slow, LateAck: lateAck})
	srv := &http.Server{Addr: addr, Handler: shop.Handler(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() {
		logger.Info("fake shop listening", "addr", addr)
		errCh <- srv.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("fake shop", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}
}

// duration reads a positive duration from env, or def when unset.
func duration(logger *slog.Logger, env string, def time.Duration) (time.Duration, bool) {
	v := os.Getenv(env)
	if v == "" {
		return def, true
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		logger.Error(env+" must be a positive duration", "value", v)
		return 0, false
	}
	return d, true
}
