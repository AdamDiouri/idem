package ledger_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/ledger"
	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/workflow"
)

var dsn string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunWithPostgres(m, &dsn))
}

// newStep creates a workflow with its charge step and returns their IDs.
func newStep(t *testing.T, pool *pgxpool.Pool) (workflowID, stepID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	s := workflow.NewSubmitter(pool, 5*time.Second, workflow.FeeConfig{RateBps: 150, FixedCents: 25})
	_, err := s.Submit(ctx, workflow.SubmitRequest{
		ClientID:       "c",
		IdempotencyKey: uuid.NewString(),
		Request:        workflow.CreateWorkflowRequest{AmountCents: 10000, Currency: "EUR", CustomerID: "cus_1", MerchantID: "mer_1"},
	})
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT workflow_id, id FROM steps ORDER BY created_at DESC LIMIT 1`).Scan(&workflowID, &stepID))
	return workflowID, stepID
}

func post(t *testing.T, pool *pgxpool.Pool, txn ledger.Txn) error {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	if err := ledger.Post(ctx, db.New(tx), txn); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func pgCode(t *testing.T, err error) string {
	t.Helper()
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "expected *pgconn.PgError, got %T: %v", err, err)
	return pgErr.Code
}

func count(t *testing.T, pool *pgxpool.Pool, query string) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), query).Scan(&n))
	return n
}

func sum(entries []ledger.Entry) int64 {
	var s int64
	for _, e := range entries {
		s += e.AmountCents
	}
	return s
}

func TestChargeEntries(t *testing.T) {
	entries := ledger.ChargeEntries(10000, 175)
	require.Equal(t, []ledger.Entry{
		{Account: ledger.ProviderReceivable, AmountCents: 10000},
		{Account: ledger.MerchantPayable, AmountCents: -9825},
		{Account: ledger.PlatformRevenue, AmountCents: -175},
	}, entries)
	require.Zero(t, sum(entries))

	noFee := ledger.ChargeEntries(10000, 0)
	require.Len(t, noFee, 2)
	require.Zero(t, sum(noFee))
}

func TestPost_Balanced(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf, step := newStep(t, pool)

	require.NoError(t, post(t, pool, ledger.Txn{WorkflowID: wf, StepID: step, Kind: "charge", Entries: ledger.ChargeEntries(10000, 175)}))
	require.Equal(t, 1, count(t, pool, `SELECT count(*) FROM ledger_transactions`))
	require.Equal(t, 3, count(t, pool, `SELECT count(*) FROM ledger_entries`))
	require.Equal(t, 0, count(t, pool, `SELECT COALESCE(sum(amount_cents), 0) FROM ledger_entries`))
	require.Equal(t, 3, count(t, pool, `SELECT count(*) FROM ledger_entries WHERE currency = 'EUR'`))
}

func TestPost_RejectsUnbalancedInGo(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf, step := newStep(t, pool)

	for name, entries := range map[string][]ledger.Entry{
		"unbalanced":   {{Account: ledger.ProviderReceivable, AmountCents: 100}, {Account: ledger.MerchantPayable, AmountCents: -99}},
		"single entry": {{Account: ledger.ProviderReceivable, AmountCents: 100}},
		"zero amount":  {{Account: ledger.ProviderReceivable, AmountCents: 0}, {Account: ledger.MerchantPayable, AmountCents: 0}},
		"empty":        nil,
	} {
		err := post(t, pool, ledger.Txn{WorkflowID: wf, StepID: step, Kind: "charge", Entries: entries})
		require.ErrorIs(t, err, ledger.ErrUnbalanced, name)
	}
	require.Equal(t, 0, count(t, pool, `SELECT count(*) FROM ledger_transactions`))
}

func TestPost_DuplicateStepKindFails(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf, step := newStep(t, pool)
	txn := ledger.Txn{WorkflowID: wf, StepID: step, Kind: "charge", Entries: ledger.ChargeEntries(10000, 175)}

	require.NoError(t, post(t, pool, txn))
	err := post(t, pool, txn)
	require.Error(t, err)
	require.Equal(t, "23505", pgCode(t, err))
	require.Equal(t, 1, count(t, pool, `SELECT count(*) FROM ledger_transactions`))
}

func TestSchema_UnbalancedRejectedAtCommit(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	wf, step := newStep(t, pool)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	txnID := uuid.New()
	_, err = tx.Exec(ctx, `INSERT INTO ledger_transactions (id, workflow_id, step_id, kind) VALUES ($1, $2, $3, 'charge')`, txnID, wf, step)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO ledger_entries (id, transaction_id, account, amount_cents, currency)
		VALUES (gen_random_uuid(), $1, 'provider_receivable', 100, 'EUR')`, txnID)
	require.NoError(t, err, "the balance check is deferred to COMMIT")

	err = tx.Commit(ctx)
	require.Error(t, err)
	require.Equal(t, "23514", pgCode(t, err))
	require.Equal(t, 0, count(t, pool, `SELECT count(*) FROM ledger_transactions`))
}

func TestSchema_LedgerIsAppendOnly(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	wf, step := newStep(t, pool)
	require.NoError(t, post(t, pool, ledger.Txn{WorkflowID: wf, StepID: step, Kind: "charge", Entries: ledger.ChargeEntries(10000, 175)}))

	_, err := pool.Exec(ctx, `UPDATE ledger_entries SET amount_cents = amount_cents * 2`)
	require.Error(t, err)
	require.Equal(t, "42501", pgCode(t, err))

	_, err = pool.Exec(ctx, `DELETE FROM ledger_entries`)
	require.Error(t, err)
	require.Equal(t, "42501", pgCode(t, err))

	_, err = pool.Exec(ctx, `DELETE FROM ledger_transactions`)
	require.Error(t, err)
	require.Equal(t, "42501", pgCode(t, err))
}
