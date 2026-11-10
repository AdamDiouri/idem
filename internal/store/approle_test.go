package store_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/store"
	"github.com/AdamDiouri/idem/internal/testutil"
)

// asUser rewrites the owner DSN to log in as user/password.
func asUser(t *testing.T, user, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	u.User = url.UserPassword(user, password)
	return u.String()
}

// Slice 2.3b (review R10, design §10): the application role may append to the
// ledger but never rewrite it, whatever triggers exist.
func TestAppRole_LedgerIsAppendOnly(t *testing.T) {
	ctx := context.Background()
	testutil.NewPool(t, dsn) // clean tables
	owner := testutil.NewOwnerPool(t, dsn)
	require.NoError(t, store.EnsureAppUser(ctx, owner, "approle_test_user", "approle-test-pw"))

	conn, err := pgx.Connect(ctx, asUser(t, "approle_test_user", "approle-test-pw"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })

	for _, stmt := range []string{
		`UPDATE ledger_entries SET amount_cents = 1`,
		`DELETE FROM ledger_transactions`,
		`TRUNCATE ledger_entries`,
		`UPDATE accounts SET code = code`,
	} {
		_, err := conn.Exec(ctx, stmt)
		require.Error(t, err, stmt)
		require.Equal(t, "42501", pgCode(t, err), stmt) // insufficient_privilege
	}
	_, err = conn.Exec(ctx, `INSERT INTO workflows (id, client_id, type, status, input)
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

func TestEnsureAppUser_Idempotent(t *testing.T) {
	ctx := context.Background()
	owner := testutil.NewOwnerPool(t, dsn)
	require.NoError(t, store.EnsureAppUser(ctx, owner, "approle_idem_user", "first-password"))
	require.NoError(t, store.EnsureAppUser(ctx, owner, "approle_idem_user", "second-password"))

	conn, err := pgx.Connect(ctx, asUser(t, "approle_idem_user", "second-password"))
	require.NoError(t, err)
	_ = conn.Close(ctx)
	_, err = pgx.Connect(ctx, asUser(t, "approle_idem_user", "first-password"))
	require.Error(t, err, "the old password no longer works")
}

// Final review, Important 2: the application login must be a plain role. Given
// the owner (or any privileged role) it would take its password over and the
// services would run with the owner's rights, silently voiding the guarantee.
func TestEnsureAppUser_RefusesPrivilegedRoles(t *testing.T) {
	ctx := context.Background()
	owner := testutil.NewOwnerPool(t, dsn)

	err := store.EnsureAppUser(ctx, owner, "idem", "new-password") // the owner, current_user, a superuser
	require.ErrorContains(t, err, "IDEM_APP_DB_USER")

	_, err = owner.Exec(ctx, `DROP ROLE IF EXISTS approle_admin; CREATE ROLE approle_admin LOGIN CREATEROLE`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = owner.Exec(ctx, `DROP ROLE IF EXISTS approle_admin`) })
	require.ErrorContains(t, store.EnsureAppUser(ctx, owner, "approle_admin", "pw-pw-pw"), "IDEM_APP_DB_USER")

	_, err = owner.Exec(ctx, `DROP ROLE IF EXISTS approle_owner; CREATE ROLE approle_owner LOGIN;
		CREATE TABLE approle_owned (id int); ALTER TABLE approle_owned OWNER TO approle_owner`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = owner.Exec(ctx, `DROP TABLE IF EXISTS approle_owned; DROP ROLE IF EXISTS approle_owner`) })
	require.ErrorContains(t, store.EnsureAppUser(ctx, owner, "approle_owner", "pw-pw-pw"), "owns")
}
