package store_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/store"
	"github.com/AdamDiouri/idem/internal/testutil"
)

// Slice 2.3b (review R10, design §10): the application role may append to the
// ledger but never rewrite it, whatever triggers exist.
func TestAppRole_LedgerIsAppendOnly(t *testing.T) {
	ctx := context.Background()
	conn := testutil.NewPool(t, dsn) // the restricted login, as the services use it

	for _, stmt := range []string{
		`UPDATE ledger_entries SET amount_cents = 1`,
		`DELETE FROM ledger_transactions`,
		`TRUNCATE ledger_entries`,
		`UPDATE accounts SET code = code`,
		// The audit trail is append-only too, and migrations belong to the owner.
		`UPDATE admin_actions SET reason = 'rewritten'`,
		`DELETE FROM admin_actions`,
		`SELECT version_id FROM goose_db_version`,
		`DELETE FROM goose_db_version`,
	} {
		_, err := conn.Exec(ctx, stmt)
		require.Error(t, err, stmt)
		require.Equal(t, "42501", pgCode(t, err), stmt) // insufficient_privilege
	}
	_, err := conn.Exec(ctx, `INSERT INTO workflows (id, client_id, type, status, input)
		VALUES (gen_random_uuid(), 'c', 'payment_payout', 'PENDING', '{}')`)
	require.NoError(t, err)
}

// A ledger transaction with no entries is rejected at commit: it would pass
// the balance check (nothing sums to zero) and record nothing.
func TestLedger_TransactionWithoutEntriesRejected(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	_, err := pool.Exec(ctx, `
		INSERT INTO workflows (id, client_id, type, status, input)
		VALUES ('01936f3e-8c2a-7b1e-9f00-0000000000d1', 'c', 'payment_payout', 'RUNNING', '{}');
		INSERT INTO steps (id, workflow_id, name, status, idempotency_key, max_attempts)
		VALUES ('01936f3e-8c2a-7b1e-9f00-0000000000d2', '01936f3e-8c2a-7b1e-9f00-0000000000d1', 'charge', 'RUNNING', 'k-empty', 8);`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO ledger_transactions (id, workflow_id, step_id, kind)
		VALUES (gen_random_uuid(), '01936f3e-8c2a-7b1e-9f00-0000000000d1', '01936f3e-8c2a-7b1e-9f00-0000000000d2', 'charge')`)
	require.Error(t, err)
	require.Equal(t, "23514", pgCode(t, err))
}

// Final review, minor 9: rolling back 00013 takes every right away from
// idem_app with plain REVOKEs the table owner may run (DROP OWNED BY needs the
// role's own privileges, which a managed-Postgres owner may lack).
func TestMigrate_DownFrom00013RevokesEverything(t *testing.T) {
	ctx := context.Background()
	testutil.NewPool(t, dsn)
	owner := testutil.NewOwnerPool(t, dsn)
	t.Cleanup(func() { require.NoError(t, store.Migrate(ctx, owner)) })
	require.NoError(t, store.MigrateDownTo(ctx, owner, 12))

	var n int
	require.NoError(t, owner.QueryRow(ctx, `SELECT count(*) FROM information_schema.role_table_grants
		WHERE grantee = 'idem_app'`).Scan(&n))
	require.Zero(t, n, "no table grants left")
	require.NoError(t, owner.QueryRow(ctx, `SELECT count(*) FROM pg_default_acl d
		WHERE array_to_string(d.defaclacl, ',') LIKE '%idem_app=%'`).Scan(&n))
	require.Zero(t, n, "no default privileges left")

	b, err := os.ReadFile("../../migrations/00013_app_role.sql")
	require.NoError(t, err)
	_, down, _ := strings.Cut(string(b), "-- +goose Down")
	require.NotContains(t, down, "DROP OWNED", "the owner may lack the rights DROP OWNED needs")
}
