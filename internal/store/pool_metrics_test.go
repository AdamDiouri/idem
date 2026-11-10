package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/metrics"
	"github.com/AdamDiouri/idem/internal/store"
	idemtest "github.com/AdamDiouri/idem/internal/testutil"
)

func gauge(t *testing.T, m *metrics.Metrics, name string) float64 {
	t.Helper()
	families, err := m.Gatherer().Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			mf := f.GetMetric()[0]
			if mf.GetGauge() != nil {
				return mf.GetGauge().GetValue()
			}
			return mf.GetCounter().GetValue()
		}
	}
	t.Fatalf("metric %s not exported", name)
	return 0
}

func openPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	idemtest.NewPool(t, dsn) // a fresh, migrated database
	pool, err := store.Open(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// Slice 2.3: the pool's state is exported, read from pgx at scrape time.
func TestWatchPool_ExportsPoolStats(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	m := metrics.New()
	m.WatchPool(pool)
	a, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer a.Release()
	b, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer b.Release()

	require.Equal(t, 2.0, gauge(t, m, "idem_db_pool_acquired_conns"))
	require.Equal(t, 20.0, gauge(t, m, "idem_db_pool_max_conns"))
	require.GreaterOrEqual(t, gauge(t, m, "idem_db_pool_acquires_total"), 2.0)
	require.GreaterOrEqual(t, gauge(t, m, "idem_db_pool_total_conns"), 2.0)
}

// Waiting for a connection is what the alert watches.
func TestWatchPool_CountsWaits(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	m := metrics.New()
	m.WatchPool(pool)
	var held []*pgxpool.Conn
	for range 20 {
		c, err := pool.Acquire(ctx)
		require.NoError(t, err)
		held = append(held, c)
	}
	go func() { time.Sleep(20 * time.Millisecond); held[0].Release() }()
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	c, err := pool.Acquire(waitCtx) // must wait for the release
	require.NoError(t, err)
	c.Release()
	for _, h := range held[1:] {
		h.Release()
	}

	require.GreaterOrEqual(t, gauge(t, m, "idem_db_pool_empty_acquires_total"), 1.0)
	require.Greater(t, gauge(t, m, "idem_db_pool_acquire_wait_seconds_total"), 0.0)
}

// Minor fix: watching the same pool twice is a no-op, not a panic.
func TestWatchPool_TwiceIsNoop(t *testing.T) {
	pool := openPool(t)
	m := metrics.New()
	m.WatchPool(pool)
	require.NotPanics(t, func() { m.WatchPool(pool) })
	require.Equal(t, 20.0, gauge(t, m, "idem_db_pool_max_conns"))
}
