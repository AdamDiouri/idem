package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/store"
	"github.com/AdamDiouri/idem/internal/testutil"
)

// insertRun records a finished reconciliation run with one discrepancy, as the
// services would (the restricted login).
func insertRun(t *testing.T, pool *pgxpool.Pool) (run, disc uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	run, disc = uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	_, err := pool.Exec(ctx, `INSERT INTO recon_runs (id, window_from, window_to) VALUES ($1, $2, $3)`,
		run, time.Now().Add(-time.Hour), time.Now())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE recon_runs SET status = 'SUCCEEDED', finished_at = now(), discrepancies = 1 WHERE id = $1`, run)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO recon_discrepancies (id, run_id, workflow_id, kind, effect_type, provider_refs,
		provider_amount, parked, detail, item_key, item_at) VALUES ($1, $2, $3, 'missing_in_ledger', 'payout', '{po_1}', -9825, false, 'x', 'k', now())`,
		disc, run, uuid.Must(uuid.NewV7()))
	require.NoError(t, err)
	return run, disc
}

// Slice 2.6: the job writes its report and reads it back.
func TestRecon_AppRoleWritesAndReads(t *testing.T) {
	pool := testutil.NewPool(t, dsn) // empties every table: once per test
	run, _ := insertRun(t, pool)
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM recon_discrepancies WHERE run_id = $1`, run).Scan(&n))
	require.Equal(t, 1, n)
}

// A report is history: the services can never rewrite or delete it.
func TestRecon_AppRoleCannotRewriteHistory(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	run, disc := insertRun(t, pool)
	for _, stmt := range []struct {
		sql string
		arg uuid.UUID
	}{
		{`DELETE FROM recon_runs WHERE id = $1`, run},
		{`DELETE FROM recon_discrepancies WHERE id = $1`, disc},
		{`UPDATE recon_discrepancies SET kind = 'amount_mismatch' WHERE id = $1`, disc},
	} {
		_, err := pool.Exec(context.Background(), stmt.sql, stmt.arg)
		require.Error(t, err, stmt.sql)
		require.Equal(t, "42501", pgCode(t, err), stmt.sql)
	}
}

func TestRecon_RejectsUnknownKind(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	run, _ := insertRun(t, pool)
	_, err := pool.Exec(context.Background(), `INSERT INTO recon_discrepancies (id, run_id, kind, effect_type, parked, detail, item_key, item_at)
		VALUES ($1, $2, 'bogus', 'charge', false, 'x', 'k', now())`, uuid.Must(uuid.NewV7()), run)
	require.Error(t, err)
	require.Equal(t, "23514", pgCode(t, err))
}

func TestMigrate_00019DownAndUp(t *testing.T) {
	ctx := context.Background()
	testutil.NewPool(t, dsn)
	owner := testutil.NewOwnerPool(t, dsn)
	t.Cleanup(func() { require.NoError(t, store.Migrate(ctx, owner)) })
	require.NoError(t, store.MigrateDownTo(ctx, owner, 18))
	require.False(t, tableExists(t, owner, "recon_runs"))
	require.NoError(t, store.Migrate(ctx, owner))
	require.True(t, tableExists(t, owner, "recon_discrepancies"))
}

// Final review, I2: a discrepancy stays open until a later run that covers it
// no longer finds it; a run over another window (the next hour, or an
// operator's quiet window) resolves nothing.
func TestRecon_OpenUntilACoveringRunNoLongerFindsIt(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	run := func(fromAgo, toAgo time.Duration, finishedAgo time.Duration, items ...string) {
		id := uuid.Must(uuid.NewV7())
		_, err := pool.Exec(ctx, `INSERT INTO recon_runs (id, window_from, window_to) VALUES ($1, now() - $2::interval, now() - $3::interval)`,
			id, fromAgo.String(), toAgo.String())
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `UPDATE recon_runs SET status = 'SUCCEEDED', finished_at = now() - $2::interval WHERE id = $1`, id, finishedAgo.String())
		require.NoError(t, err)
		for _, key := range items {
			_, err := pool.Exec(ctx, `INSERT INTO recon_discrepancies (id, run_id, kind, effect_type, parked, detail, item_key, item_at)
				VALUES (gen_random_uuid(), $1, 'missing_in_ledger', 'payout', false, 'x', $2, now() - interval '150 minutes')`, id, key)
			require.NoError(t, err)
		}
	}
	open := func() int {
		var n int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM recon_open_discrepancies`).Scan(&n))
		return n
	}

	run(3*time.Hour, 2*time.Hour, 50*time.Minute, "wf|payout") // finds the item (it happened 150 minutes ago)
	require.Equal(t, 1, open())
	run(3*time.Hour, 2*time.Hour, 40*time.Minute, "wf|payout") // the loop finds it again: still one open item
	require.Equal(t, 1, open())
	run(time.Hour, 0, 30*time.Minute) // the next hour: does not cover it
	require.Equal(t, 1, open(), "a run over another window resolves nothing")
	run(3*time.Hour, 2*time.Hour, 10*time.Minute) // covers it, no longer finds it (fixed)
	require.Zero(t, open())
}
