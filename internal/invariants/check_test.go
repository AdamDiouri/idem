package invariants_test

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/invariants"
	"github.com/AdamDiouri/idem/internal/ledger"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/workflow"
)

var dsn string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunWithPostgres(m, &dsn))
}

type env struct {
	pool   *pgxpool.Pool
	fake   *fakeprovider.Provider
	client *provider.Client
}

func setup(t *testing.T) env {
	t.Helper()
	fake := fakeprovider.New(fakeprovider.Options{Hang: time.Second})
	srv := httptest.NewServer(fake.Handler())
	t.Cleanup(srv.Close)
	return env{pool: testutil.NewPool(t, dsn), fake: fake, client: provider.NewClient(srv.URL, time.Second)}
}

func newWorkflow(t *testing.T, pool *pgxpool.Pool) (workflowID, stepID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	s := workflow.NewSubmitter(pool, 5*time.Second, workflow.FeeConfig{RateBps: 150, FixedCents: 25})
	_, err := s.Submit(ctx, workflow.SubmitRequest{
		ClientID:       "c",
		IdempotencyKey: uuid.NewString(),
		Request:        workflow.CreateWorkflowRequest{AmountCents: 10000, Currency: "EUR", CustomerID: "cus_1", MerchantID: "mer_1"},
	})
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx, `SELECT workflow_id, id FROM steps ORDER BY created_at DESC LIMIT 1`).Scan(&workflowID, &stepID))
	return workflowID, stepID
}

func (e env) charge(t *testing.T, wf uuid.UUID, key string) {
	t.Helper()
	out := e.client.CreateCharge(context.Background(), key,
		provider.ChargeRequest{AmountCents: 10000, Currency: "EUR", CustomerID: "cus_1", WorkflowID: wf})
	require.Equal(t, provider.Success, out.Class, "err: %v", out.Err)
}

func (e env) post(t *testing.T, wf, step uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	require.NoError(t, ledger.Post(ctx, db.New(tx), ledger.Txn{WorkflowID: wf, StepID: step, Kind: "charge", Entries: ledger.ChargeEntries(10000, 175)}))
	require.NoError(t, tx.Commit(ctx))
}

func (e env) check(t *testing.T) []invariants.Violation {
	t.Helper()
	v, err := invariants.Check(context.Background(), e.pool, e.fake.Effects())
	require.NoError(t, err)
	return v
}

func TestCheck_Clean(t *testing.T) {
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-1")
	e.charge(t, wf, "key-1") // replay: still one effect
	e.post(t, wf, step)
	require.Empty(t, e.check(t))
}

// F27: an engine bug that mints a fresh key on retry charges twice. The oracle
// counts effects per workflow, not per key, so I1 catches it.
func TestCheck_I1_DuplicateEffect_F27(t *testing.T) {
	e := setup(t)
	wf, _ := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-1")
	e.charge(t, wf, "key-2")

	v := e.check(t)
	require.Len(t, v, 1)
	require.Equal(t, "I1", v[0].Invariant)
	require.Equal(t, wf.String(), v[0].WorkflowID)
	require.Contains(t, v[0].Detail, "key-1")
	require.Contains(t, v[0].Detail, "key-2")
}

func TestCheck_I1_DifferentWorkflowsOK(t *testing.T) {
	e := setup(t)
	wf1, _ := newWorkflow(t, e.pool)
	wf2, _ := newWorkflow(t, e.pool)
	e.charge(t, wf1, "key-1")
	e.charge(t, wf2, "key-2")
	require.Empty(t, e.check(t))
}

func TestCheck_I2_Unbalanced(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)

	// Bypass the balance trigger to plant corruption the checker must find.
	tx, err := e.pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `SET LOCAL session_replication_role = replica`)
	require.NoError(t, err)
	txnID := uuid.New()
	_, err = tx.Exec(ctx, `INSERT INTO ledger_transactions (id, workflow_id, step_id, kind) VALUES ($1, $2, $3, 'charge')`, txnID, wf, step)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO ledger_entries (id, transaction_id, account, amount_cents, currency)
		VALUES (gen_random_uuid(), $1, 'provider_receivable', 100, 'EUR')`, txnID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	v := e.check(t)
	require.Len(t, v, 2)
	require.Equal(t, "I2", v[0].Invariant)
	require.Equal(t, "", v[0].WorkflowID, "global imbalance sorts first")
	require.Equal(t, "I2", v[1].Invariant)
	require.Equal(t, wf.String(), v[1].WorkflowID)
	require.Contains(t, v[1].Detail, txnID.String())
}
