package engine_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/workflow"
)

func (h harness) chargeStep() engine.Step { return engine.NewChargeStep(h.client) }
func (h harness) payoutStep() engine.Step { return engine.NewPayoutStep(h.client) }
func (h harness) refundStep() engine.Step { return engine.NewRefundStep(h.client) }

func (h harness) allSteps() []engine.Step {
	return []engine.Step{h.chargeStep(), h.payoutStep(), h.refundStep()}
}

// drain runs w until nothing is claimable.
func (h harness) drain(t *testing.T, w *engine.Worker) {
	t.Helper()
	for range 100 {
		if !h.runOnce(t, w).Claimed {
			return
		}
	}
	t.Fatal("drain: still claiming after 100 runs")
}

func submitTo(t *testing.T, h harness, customer, merchant string) uuid.UUID {
	t.Helper()
	s := workflow.NewSubmitter(h.pool, 5*time.Second, workflow.FeeConfig{RateBps: 150, FixedCents: 25})
	res, err := s.Submit(context.Background(), workflow.SubmitRequest{
		ClientID:       "c",
		IdempotencyKey: uuid.NewString(),
		Request:        workflow.CreateWorkflowRequest{AmountCents: 10000, Currency: "EUR", CustomerID: customer, MerchantID: merchant},
	})
	require.NoError(t, err)
	var created workflow.Created
	require.NoError(t, jsonUnmarshal(res.Body, &created))
	return created.WorkflowID
}

func (h harness) workflowStatus(t *testing.T, wf uuid.UUID) string {
	t.Helper()
	return scalar[string](t, h.pool, `SELECT status FROM workflows WHERE id = $1`, wf)
}

func (h harness) stepStatus(t *testing.T, wf uuid.UUID, name string) string {
	t.Helper()
	return scalar[string](t, h.pool, `SELECT status FROM steps WHERE workflow_id = $1 AND name = $2`, wf, name)
}

func (h harness) effectsOf(wf uuid.UUID, typ string) []fakeprovider.Effect {
	var out []fakeprovider.Effect
	for _, e := range h.fake.Effects() {
		if e.WorkflowID == wf.String() && e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

func (h harness) accountSums(t *testing.T, wf uuid.UUID) map[string]int64 {
	t.Helper()
	rows, err := h.pool.Query(context.Background(), `
		SELECT e.account, SUM(e.amount_cents)::bigint FROM ledger_entries e
		JOIN ledger_transactions t ON t.id = e.transaction_id
		WHERE t.workflow_id = $1 GROUP BY e.account`, wf)
	require.NoError(t, err)
	defer rows.Close()
	sums := map[string]int64{}
	for rows.Next() {
		var account string
		var total int64
		require.NoError(t, rows.Scan(&account, &total))
		sums[account] = total
	}
	require.NoError(t, rows.Err())
	return sums
}

func (h harness) setStepMaxAttempts(t *testing.T, wf uuid.UUID, name string, n int) {
	t.Helper()
	_, err := h.pool.Exec(context.Background(), `UPDATE steps SET max_attempts = $3 WHERE workflow_id = $1 AND name = $2`, wf, name, n)
	require.NoError(t, err)
}

func TestFlow_HappyPath(t *testing.T) {
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))

	require.Equal(t, "SUCCEEDED", h.workflowStatus(t, wf))
	require.Len(t, h.effectsOf(wf, "charge"), 1)
	payouts := h.effectsOf(wf, "payout")
	require.Len(t, payouts, 1)
	require.Equal(t, int64(9825), payouts[0].AmountCents)
	require.Empty(t, h.effectsOf(wf, "refund"))
	require.Equal(t, map[string]int64{"provider_receivable": 175, "merchant_payable": 0, "platform_revenue": -175}, h.accountSums(t, wf))
	h.assertNoViolations(t)
}

func TestFlow_PayoutFailsRefundCompensates_F20(t *testing.T) {
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_invalid_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep(), h.payoutStep()))

	require.Equal(t, "FAILED_PERMANENT", h.stepStatus(t, wf, "payout"))
	require.Equal(t, "COMPENSATING", h.workflowStatus(t, wf))
	chargeRef := scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf)
	require.Equal(t, chargeRef, scalar[string](t, h.pool, `SELECT refund_charge_ref FROM steps WHERE workflow_id = $1 AND name = 'refund'`, wf))
	require.Equal(t, 20, scalar[int](t, h.pool, `SELECT max_attempts FROM steps WHERE workflow_id = $1 AND name = 'refund'`, wf))

	h.drain(t, h.workerWith("w1", nil, h.refundStep()))
	require.Equal(t, "COMPENSATED", h.workflowStatus(t, wf))
	refunds := h.effectsOf(wf, "refund")
	require.Len(t, refunds, 1)
	require.Equal(t, int64(10000), refunds[0].AmountCents, "the customer gets the full amount back, fee included")
	require.Empty(t, h.effectsOf(wf, "payout"))
	for account, total := range h.accountSums(t, wf) {
		require.Zero(t, total, "account %s must net to zero after compensation", account)
	}
	h.assertNoViolations(t)
}

func TestFlow_RefundKeepsFailing_F21(t *testing.T) {
	h := newHarness(t)
	h.setFaults(fakeprovider.Rule{Endpoint: "refunds", Mode: fakeprovider.ModeErrorBeforeEffect})
	wf := submitTo(t, h, "cus_1", "mer_invalid_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep(), h.payoutStep()))
	h.setStepMaxAttempts(t, wf, "refund", 3)

	h.drain(t, h.workerWith("w1", nil, h.refundStep()))
	require.Equal(t, "DEAD", h.stepStatus(t, wf, "refund"))
	require.Equal(t, engine.DeadAttemptsExhaustedInDoubt,
		scalar[string](t, h.pool, `SELECT dead_reason FROM steps WHERE workflow_id = $1 AND name = 'refund'`, wf))
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
	h.assertNoViolations(t)
}

func TestFlow_RefundRejectedNeedsAttention(t *testing.T) {
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_invalid_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep(), h.payoutStep()))
	_, err := h.pool.Exec(context.Background(), `UPDATE steps SET refund_charge_ref = 'ch_missing' WHERE workflow_id = $1 AND name = 'refund'`, wf)
	require.NoError(t, err)

	h.drain(t, h.workerWith("w1", nil, h.refundStep()))
	require.Equal(t, "FAILED_PERMANENT", h.stepStatus(t, wf, "refund"))
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf), "charged and not refunded: a human must act")
	require.Empty(t, h.effectsOf(wf, "refund"))
	h.assertNoViolations(t)
}

// The payout is the pivot: if we cannot tell whether the merchant was paid, we
// must not refund the customer too (the platform would lose the amount twice).
func TestFlow_PayoutDeadDoesNotCompensate(t *testing.T) {
	h := newHarness(t)
	h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModeErrorBeforeEffect})
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	h.setStepMaxAttempts(t, wf, "payout", 2)

	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
	require.Equal(t, "DEAD", h.stepStatus(t, wf, "payout"))
	require.Equal(t, engine.DeadAttemptsExhaustedInDoubt,
		scalar[string](t, h.pool, `SELECT dead_reason FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
	require.Equal(t, 0, h.count(t, `SELECT count(*) FROM steps WHERE workflow_id = $1 AND name = 'refund'`, wf), "no automatic refund")
	require.Empty(t, h.effectsOf(wf, "refund"))
	h.assertNoViolations(t)
}

// Review finding: the dangerous variant of the pivot — the merchant really was
// paid, but we cannot tell. DEAD, NEEDS_ATTENTION, no refund; and the
// invariants accept it (the ledger may lag the provider until a human acts).
func TestFlow_PayoutDeadAfterMoneyMoved(t *testing.T) {
	h := newHarness(t)
	h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModeTimeoutAfterEffect})
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	h.setStepMaxAttempts(t, wf, "payout", 1)

	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
	require.Len(t, h.effectsOf(wf, "payout"), 1, "the merchant was paid")
	require.Equal(t, "DEAD", h.stepStatus(t, wf, "payout"))
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
	require.Equal(t, 0, h.count(t, `SELECT count(*) FROM steps WHERE workflow_id = $1 AND name = 'refund'`, wf), "no automatic refund")
	h.assertNoViolations(t)
}

// Review finding: a parked workflow moves no more money.
func TestFlow_ParkedWorkflowMovesNoMoney(t *testing.T) {
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	_, err := h.pool.Exec(context.Background(), `UPDATE workflows SET status = 'NEEDS_ATTENTION' WHERE id = $1`, wf)
	require.NoError(t, err)

	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
	require.Empty(t, h.effectsOf(wf, "payout"))
	require.Equal(t, "PENDING", h.stepStatus(t, wf, "payout"))
	h.assertNoViolations(t)
}

func TestFlow_PayoutCrashRecovery(t *testing.T) {
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))

	failpoint.Enable(engine.FPAfterProviderCall, failpoint.CrashOnce())
	require.True(t, crashes(func() { _, _ = h.workerWith("wA", nil, h.payoutStep()).RunOnce(context.Background()) }))
	require.Len(t, h.effectsOf(wf, "payout"), 1, "the merchant was paid before the crash")

	time.Sleep(pastLease)
	require.Equal(t, int64(1), h.reclaim(t))
	h.drain(t, h.workerWith("wB", nil, h.allSteps()...))

	require.Equal(t, "SUCCEEDED", h.workflowStatus(t, wf))
	require.Len(t, h.effectsOf(wf, "payout"), 1)
	require.Equal(t, 1, h.count(t, `SELECT count(*) FROM ledger_transactions WHERE workflow_id = $1 AND kind = 'payout'`, wf))
	h.assertNoViolations(t)
}

func TestFlow_ManyWorkflowsUnderFaults(t *testing.T) {
	h := newHarness(t)
	h.fake.SetFaults(fakeprovider.Faults{Seed: 7, Rules: []fakeprovider.Rule{
		{Endpoint: "charges", Mode: fakeprovider.ModeTimeoutAfterEffect, Probability: 0.2},
		{Endpoint: "payouts", Mode: fakeprovider.ModeErrorAfterEffect, Probability: 0.2},
		{Endpoint: "refunds", Mode: fakeprovider.ModeRateLimit, Probability: 0.2},
	}})

	const n = 30
	for i := range n {
		customer, merchant := "cus_1", "mer_1"
		if i%7 == 0 {
			customer = fmt.Sprintf("cus_decline_%d", i)
		}
		if i%3 == 0 {
			merchant = fmt.Sprintf("mer_invalid_%d", i)
		}
		submitTo(t, h, customer, merchant)
	}

	for i := range 3 {
		_, _ = runInBackground(t, h.workerWith(fmt.Sprintf("w%d", i), func(o *engine.WorkerOptions) {
			o.Backoff.Cap = 0 // ignore Retry-After in tests
		}, h.allSteps()...).Run)
	}
	_, _ = runInBackground(t, func(ctx context.Context) error {
		return engine.RunSweeper(ctx, h.pool, 50*time.Millisecond, time.Hour, slog.New(slog.DiscardHandler))
	})

	require.Eventually(t, func() bool {
		return h.count(t, `SELECT count(*) FROM workflows WHERE status IN ('SUCCEEDED','FAILED','COMPENSATED','NEEDS_ATTENTION')`) == n
	}, 30*time.Second, 50*time.Millisecond)
	// The outcome mix is fixed by construction (i%7 declines, i%3 invalid
	// merchants); faults must only delay, never change, an outcome.
	require.Equal(t, 5, h.count(t, `SELECT count(*) FROM workflows WHERE status = 'FAILED'`))
	require.Equal(t, 8, h.count(t, `SELECT count(*) FROM workflows WHERE status = 'COMPENSATED'`))
	require.Equal(t, 17, h.count(t, `SELECT count(*) FROM workflows WHERE status = 'SUCCEEDED'`))
	require.Equal(t, 0, h.count(t, `SELECT count(*) FROM workflows WHERE status = 'NEEDS_ATTENTION'`))
	h.assertNoViolations(t)
}
