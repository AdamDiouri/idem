//go:build chaos

package chaos

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Run with: make build && go test -tags chaos -run TestRun_ShortCampaign ./internal/chaos/
func TestRun_ShortCampaign(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.Duration = 30 * time.Second
	cfg.SettleTimeout = 90 * time.Second
	cfg.Workflows = 60
	cfg.BinDir = "../../bin"
	cfg.RunDir = t.TempDir()

	rep, err := Run(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	require.True(t, rep.Passed, rep.Text())
	total := 0
	for _, n := range rep.Restarts {
		total += n
	}
	require.GreaterOrEqual(t, total, 1)
}
