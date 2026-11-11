package invariants_test

import (
	"context"
	"net/http/httptest"
	"os"
	"strings"
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

	v := only(e.check(t), "I1") // no postings either, so I3 also fires
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
	require.Empty(t, only(e.check(t), "I1"))
}

func TestCheck_I2_Unbalanced(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)

	// Bypass the balance trigger to plant corruption the checker must find
	// (as the owner: the application role may not).
	tx, err := testutil.NewOwnerPool(t, dsn).Begin(ctx)
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

	v := only(e.check(t), "I2") // a posting without an effect also breaks I3
	require.Len(t, v, 2)
	require.Equal(t, "I2", v[0].Invariant)
	require.Equal(t, "", v[0].WorkflowID, "global imbalance sorts first")
	require.Equal(t, "I2", v[1].Invariant)
	require.Equal(t, wf.String(), v[1].WorkflowID)
	require.Contains(t, v[1].Detail, txnID.String())
}

func only(vs []invariants.Violation, invariant string) []invariants.Violation {
	var out []invariants.Violation
	for _, v := range vs {
		if v.Invariant == invariant {
			out = append(out, v)
		}
	}
	return out
}

func (e env) payout(t *testing.T, wf uuid.UUID, key string) {
	t.Helper()
	req := provider.PayoutRequest{AmountCents: 9825, Currency: "EUR", MerchantID: "mer_1", WorkflowID: wf}
	out := e.client.CreatePayout(context.Background(), key, req)
	require.Equal(t, provider.Accepted, out.Class, "err: %v", out.Err)
	// Payouts are async (M2): the money moves when the payout settles.
	paid := e.client.GetPayout(context.Background(), out.ProviderRef, req)
	require.Equal(t, provider.Success, paid.Class, "err: %v", paid.Err)
}

func (e env) postKind(t *testing.T, wf, step uuid.UUID, kind string, entries []ledger.Entry) {
	t.Helper()
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	require.NoError(t, ledger.Post(ctx, db.New(tx), ledger.Txn{WorkflowID: wf, StepID: step, Kind: kind, Entries: entries}))
	require.NoError(t, tx.Commit(ctx))
}

// setStatus moves wf to status as a real transition would: a terminal status
// also gets its outbox event (D13), so only the invariant under test fires.
func (e env) setStatus(t *testing.T, wf uuid.UUID, status string) {
	t.Helper()
	testutil.SetWorkflowStatus(t, e.pool, wf, status)
}

func requireViolation(t *testing.T, vs []invariants.Violation, invariant string, wf uuid.UUID, detail string) {
	t.Helper()
	for _, v := range vs {
		if v.Invariant == invariant && v.WorkflowID == wf.String() && strings.Contains(v.Detail, detail) {
			return
		}
	}
	t.Fatalf("no %s violation for %s mentioning %q in %v", invariant, wf, detail, vs)
}

func TestCheck_I3_EffectWithoutPosting(t *testing.T) {
	e := setup(t)
	wf, _ := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-1")
	requireViolation(t, e.check(t), "I3", wf, "charge")
}

func TestCheck_I3_PostingWithoutEffect(t *testing.T) {
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.post(t, wf, step)
	requireViolation(t, e.check(t), "I3", wf, "charge")
}

func TestCheck_I3_AmountMismatch(t *testing.T) {
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-1") // 10000 at the provider
	e.postKind(t, wf, step, "charge", ledger.ChargeEntries(9999, 175))
	requireViolation(t, e.check(t), "I3", wf, "9999")
}

func TestCheck_I5_PayoutWithoutCharge(t *testing.T) {
	e := setup(t)
	wf, _ := newWorkflow(t, e.pool)
	e.payout(t, wf, "po-1")
	requireViolation(t, e.check(t), "I5", wf, "payout")
}

func TestCheck_I7_SucceededWithoutPayout(t *testing.T) {
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-1")
	e.post(t, wf, step)
	e.setStatus(t, wf, "SUCCEEDED")
	v := e.check(t)
	requireViolation(t, v, "I7", wf, "SUCCEEDED")
	require.Len(t, v, 1)
}

func TestCheck_I7_FailedWithEffect(t *testing.T) {
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-1")
	e.post(t, wf, step)
	e.setStatus(t, wf, "FAILED")
	requireViolation(t, e.check(t), "I7", wf, "FAILED")
}

func TestCheck_I7_CompensatedWithPayout(t *testing.T) {
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-1")
	e.post(t, wf, step)
	e.payout(t, wf, "po-1")
	e.postKind(t, wf, step, "payout", ledger.PayoutEntries(10000, 175))
	e.setStatus(t, wf, "COMPENSATED")
	requireViolation(t, e.check(t), "I7", wf, "COMPENSATED")
}

func TestCheck_AllInvariantsCleanFlow(t *testing.T) {
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-1")
	e.post(t, wf, step)
	e.payout(t, wf, "po-1")
	e.postKind(t, wf, step, "payout", ledger.PayoutEntries(10000, 175))
	e.setStatus(t, wf, "SUCCEEDED")
	require.Empty(t, e.check(t))
}

// Review finding: a refund aimed at ANOTHER workflow's charge must be caught —
// that customer was refunded while their merchant was also paid.
func TestCheck_I3_RefundOfAnotherWorkflowsCharge(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	wfA, _ := newWorkflow(t, e.pool)
	wfB, _ := newWorkflow(t, e.pool)
	chargeA := e.client.CreateCharge(ctx, "ch-a", provider.ChargeRequest{AmountCents: 10000, Currency: "EUR", CustomerID: "cus_1", WorkflowID: wfA})
	require.Equal(t, provider.Success, chargeA.Class)
	refund := e.client.CreateRefund(ctx, "re-b", provider.RefundRequest{AmountCents: 10000, Currency: "EUR", ChargeID: chargeA.ProviderRef, WorkflowID: wfB})
	require.Equal(t, provider.Success, refund.Class)

	requireViolation(t, e.check(t), "I3", wfB, "another workflow")
}

func TestCheckLiveness_ReportsStuck(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	wf, _ := newWorkflow(t, e.pool) // PENDING

	v, err := invariants.CheckLiveness(ctx, e.pool)
	require.NoError(t, err)
	require.Len(t, v, 1)
	require.Equal(t, "I4", v[0].Invariant)
	require.Equal(t, wf.String(), v[0].WorkflowID)
	require.Contains(t, v[0].Detail, "PENDING")

	e.setStatus(t, wf, "SUCCEEDED")
	v, err = invariants.CheckLiveness(ctx, e.pool)
	require.NoError(t, err)
	require.Empty(t, v)
}

// Slice 2.1, I4 extended: after quiescence no step may still wait for a callback.
func TestCheckLiveness_ReportsAwaitingCallback(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.setStatus(t, wf, "RUNNING")
	_, err := e.pool.Exec(ctx, `UPDATE steps SET status = 'AWAITING_CALLBACK', provider_ref = 'po_1', callback_deadline = now() WHERE id = $1`, step)
	require.NoError(t, err)

	v, err := invariants.CheckLiveness(ctx, e.pool)
	require.NoError(t, err)
	require.NotEmpty(t, v)
	var details []string
	for _, x := range v {
		details = append(details, x.Detail)
	}
	require.Contains(t, strings.Join(details, "\n"), "AWAITING_CALLBACK")
}

func TestCheck_I6_KeyWithoutWorkflow(t *testing.T) {
	e := setup(t)
	_, err := e.pool.Exec(context.Background(),
		`INSERT INTO api_idempotency_keys (client_id, key, request_fingerprint) VALUES ('c', 'orphan-key', 'f')`)
	require.NoError(t, err)
	v := only(e.check(t), "I6")
	require.Len(t, v, 1)
	require.Contains(t, v[0].Detail, "orphan-key")
}

func TestCheck_I6_WorkflowWithoutKey(t *testing.T) {
	e := setup(t)
	id := uuid.New()
	_, err := e.pool.Exec(context.Background(),
		`INSERT INTO workflows (id, client_id, type, status, input) VALUES ($1, 'c', 'payment_payout', 'PENDING', '{}')`, id)
	require.NoError(t, err)
	requireViolation(t, e.check(t), "I6", id, "no idempotency key")
}

func TestCheck_I6_SubmittedWorkflowsClean(t *testing.T) {
	e := setup(t)
	newWorkflow(t, e.pool)
	newWorkflow(t, e.pool)
	require.Empty(t, only(e.check(t), "I6"))
}

// Review finding I7: liveness also covers steps — a finished or parked
// workflow must not leave a step RUNNING or IN_DOUBT behind.
func TestCheckLiveness_ReportsLeftoverSteps(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.setStatus(t, wf, "NEEDS_ATTENTION")
	_, err := e.pool.Exec(ctx, `UPDATE steps SET status = 'IN_DOUBT' WHERE id = $1`, step)
	require.NoError(t, err)

	v, err := invariants.CheckLiveness(ctx, e.pool)
	require.NoError(t, err)
	requireViolation(t, v, "I4", wf, "IN_DOUBT")
}

// Slice 2.2, I10: the fake provider never truly contradicts itself, so any
// CONFLICT means idem misread it.
func TestCheck_I10_ConflictIsViolation(t *testing.T) {
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	_, err := e.pool.Exec(context.Background(), `INSERT INTO webhook_inbox (provider_event_id, type, payload, status, step_id, note)
		VALUES ('evt_c', 'payout.paid', '{}', 'CONFLICT', $1, 'provider says paid po_1, idem recorded not paid')`, step)
	require.NoError(t, err)
	v := only(e.check(t), "I10")
	require.Len(t, v, 1)
	require.Equal(t, wf.String(), v[0].WorkflowID)
	require.Contains(t, v[0].Detail, "evt_c")
}

// Slice 2.2, I4 extended: after quiescence no inbox event is still pending.
func TestCheckLiveness_ReportsPendingInbox(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	wf, _ := newWorkflow(t, e.pool)
	e.setStatus(t, wf, "SUCCEEDED")
	_, err := e.pool.Exec(ctx, `INSERT INTO webhook_inbox (provider_event_id, type, payload) VALUES ('evt_p', 'payout.paid', '{}')`)
	require.NoError(t, err)
	v, err := invariants.CheckLiveness(ctx, e.pool)
	require.NoError(t, err)
	require.Len(t, v, 1)
	require.Equal(t, "I4", v[0].Invariant)
	require.Contains(t, v[0].Detail, "evt_p")
}

// Slice 2.3b: I6 in the other direction. The constraint stops this today; the
// check is the guard if the constraint is ever dropped.
func TestCheck_I6_TwoKeysOneWorkflow(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	wf, _ := newWorkflow(t, e.pool)
	owner := testutil.NewOwnerPool(t, dsn) // schema changes need the owner
	_, err := owner.Exec(ctx, `ALTER TABLE api_idempotency_keys DROP CONSTRAINT IF EXISTS api_idempotency_keys_workflow_unique`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = owner.Exec(ctx, `DELETE FROM api_idempotency_keys WHERE key = 'second-key'`)
		_, err := owner.Exec(ctx, `ALTER TABLE api_idempotency_keys ADD CONSTRAINT api_idempotency_keys_workflow_unique UNIQUE (workflow_id)`)
		require.NoError(t, err)
	})
	_, err = e.pool.Exec(ctx, `INSERT INTO api_idempotency_keys (client_id, key, request_fingerprint, workflow_id, response_body)
		VALUES ('c', 'second-key', 'f', $1, '{}')`, wf)
	require.NoError(t, err)
	requireViolation(t, e.check(t), "I6", wf, "2 idempotency keys")
}

// parkStep parks wf with a DEAD step of the given name and reason.
func (e env) parkStep(t *testing.T, wf uuid.UUID, name, reason string) {
	t.Helper()
	_, err := e.pool.Exec(context.Background(), `INSERT INTO steps (id, workflow_id, name, status, idempotency_key, max_attempts, dead_reason)
		VALUES (gen_random_uuid(), $1, $2, 'DEAD', $3, 8, $4)`, wf, name, uuid.NewString(), reason)
	require.NoError(t, err)
	e.setStatus(t, wf, "NEEDS_ATTENTION")
}

// Slice 2.3b: a payout refused after an ambiguous attempt definitively moved
// nothing, so the ledger must still match the provider for its workflow.
func TestCheck_I3_ParkedNoEffectIsNotExempt(t *testing.T) {
	e := setup(t)
	wf, _ := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-1") // no posting
	e.parkStep(t, wf, "payout", "permanent_after_in_doubt")
	requireViolation(t, e.check(t), "I3", wf, "charge")
}

func TestCheck_I7_ParkedPermanentAfterDoubtShape(t *testing.T) {
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-1")
	e.post(t, wf, step)
	e.payout(t, wf, "po-1") // the provider paid after all
	e.parkStep(t, wf, "payout", "permanent_after_in_doubt")
	requireViolation(t, e.check(t), "I7", wf, "NEEDS_ATTENTION (payout permanent_after_in_doubt)")
}

func TestCheck_I7_PartialRefund(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	ch := e.client.CreateCharge(ctx, "ch-1", provider.ChargeRequest{AmountCents: 1000, Currency: "EUR", CustomerID: "cus_1", WorkflowID: wf})
	require.Equal(t, provider.Success, ch.Class)
	e.postKind(t, wf, step, "charge", ledger.ChargeEntries(1000, 0))
	re := e.client.CreateRefund(ctx, "re-1", provider.RefundRequest{AmountCents: 400, Currency: "EUR", ChargeID: ch.ProviderRef, WorkflowID: wf})
	require.Equal(t, provider.Success, re.Class, "err: %v", re.Err)
	e.postKind(t, wf, step, "refund", ledger.RefundEntries(400, 0))
	e.setStatus(t, wf, "COMPENSATED")
	requireViolation(t, e.check(t), "I7", wf, "refund 400 ≠ charge 1000")
}

// Regression: a payout parked in doubt may have moved money without a
// posting; until a human reconciles it, I3 does not judge it.
func TestCheck_I3_ParkedInDoubtStaysExempt(t *testing.T) {
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-1")
	e.post(t, wf, step)
	e.payout(t, wf, "po-1") // moved, never posted
	e.parkStep(t, wf, "payout", "attempts_exhausted_in_doubt")
	require.Empty(t, e.check(t))
}
