package store_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/store"
	"github.com/AdamDiouri/idem/internal/testutil"
)

var dsn string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunWithPostgres(m, &dsn))
}

func tableExists(t *testing.T, pool *pgxpool.Pool, name string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)`,
		name).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func pgCode(t *testing.T, err error) string {
	t.Helper()
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "expected *pgconn.PgError, got %T: %v", err, err)
	return pgErr.Code
}

func TestMigrate_UpDownUp(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)

	require.NoError(t, store.MigrateDown(ctx, pool))
	require.False(t, tableExists(t, pool, "workflows"))

	require.NoError(t, store.Migrate(ctx, pool))
	require.True(t, tableExists(t, pool, "workflows"))
	require.True(t, tableExists(t, pool, "steps"))
	require.True(t, tableExists(t, pool, "api_idempotency_keys"))

	require.NoError(t, store.Migrate(ctx, pool), "re-running migrations must be a no-op")
}

func TestSchema_RejectsUnknownWorkflowStatus(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	_, err := pool.Exec(context.Background(),
		`INSERT INTO workflows (id, client_id, type, status, input)
		 VALUES (gen_random_uuid(), 'c', 'payment_payout', 'BOGUS', '{}')`)
	require.Error(t, err)
	require.Equal(t, "23514", pgCode(t, err)) // check_violation
}

func TestSchema_StepKeyIsUnique(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	_, err := pool.Exec(ctx,
		`INSERT INTO workflows (id, client_id, type, status, input)
		 VALUES ('00000000-0000-0000-0000-000000000001', 'c', 'payment_payout', 'PENDING', '{}')`)
	require.NoError(t, err)

	insertStep := `INSERT INTO steps (id, workflow_id, name, generation, status, idempotency_key, max_attempts)
		 VALUES (gen_random_uuid(), '00000000-0000-0000-0000-000000000001', 'charge', $1, 'PENDING', 'same-key', 8)`
	_, err = pool.Exec(ctx, insertStep, 0)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, insertStep, 1)
	require.Error(t, err)
	require.Equal(t, "23505", pgCode(t, err)) // unique_violation
}

func TestIsLockTimeout(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	insertKey := `INSERT INTO api_idempotency_keys (client_id, key, request_fingerprint) VALUES ('c', 'k', 'f')`

	txA, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer txA.Rollback(ctx)
	_, err = txA.Exec(ctx, insertKey)
	require.NoError(t, err)

	ctxB, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	txB, err := pool.Begin(ctxB)
	require.NoError(t, err)
	defer txB.Rollback(ctxB)
	_, err = txB.Exec(ctxB, `SET LOCAL lock_timeout = '100ms'`)
	require.NoError(t, err)
	_, err = txB.Exec(ctxB, insertKey)
	require.Error(t, err)

	require.True(t, store.IsLockTimeout(err))
	require.False(t, store.IsLockTimeout(errors.New("x")))
	require.False(t, store.IsLockTimeout(nil))
}
