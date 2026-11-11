package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/webhook"
	"github.com/AdamDiouri/idem/internal/workflow"
)

func (h harness) admin() *engine.Admin {
	return engine.NewAdmin(h.pool, h.allSteps(), "tester")
}

// parkedStep returns the parked workflow entry for wf.
func (h harness) parkedStep(t *testing.T, wf uuid.UUID) engine.ParkedWorkflow {
	t.Helper()
	parked, err := h.admin().ListParked(context.Background(), "")
	require.NoError(t, err)
	for _, p := range parked {
		if p.WorkflowID == wf {
			return p
		}
	}
	t.Fatalf("workflow %s is not parked", wf)
	return engine.ParkedWorkflow{}
}

func (h harness) noRetryAfter() *engine.Worker {
	return h.workerWith("w1", func(o *engine.WorkerOptions) { o.Backoff.Cap = 0 }, h.allSteps()...)
}

// parkPayoutAfterDoubt parks an invalid-merchant payout by the pivot rule.
func parkPayoutAfterDoubt(t *testing.T, h harness) uuid.UUID {
	t.Helper()
	wf := submitTo(t, h, "cus_1", "mer_invalid_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModeErrorBeforeEffect, Times: 1})
	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
	h.setFaults()
	return wf
}

// parkCharge parks a charge whose attempts ran out under `mode` (no money moved).
func parkCharge(t *testing.T, h harness, mode string) uuid.UUID {
	t.Helper()
	h.setFaults(fakeprovider.Rule{Endpoint: "charges", Mode: mode})
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.setStepMaxAttempts(t, wf, "charge", 2)
	h.drain(t, h.noRetryAfter())
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
	h.setFaults()
	return wf
}

func (h harness) auditRows(t *testing.T, wf uuid.UUID) int {
	t.Helper()
	return h.count(t, `SELECT count(*) FROM admin_actions WHERE workflow_id = $1`, wf)
}

func TestAdmin_ListParked(t *testing.T) {
	h := newHarness(t)
	payoutWF := parkPayoutAfterDoubt(t, h)
	chargeWF := parkCharge(t, h, fakeprovider.ModeErrorBeforeEffect)

	p := h.parkedStep(t, payoutWF)
	require.Equal(t, "payout", p.Step)
	require.Equal(t, "DEAD", p.StepStatus)
	require.Equal(t, engine.DeadPermanentAfterInDoubt, p.DeadReason)
	require.Equal(t, "mer_invalid_1", p.MerchantID)
	require.Equal(t, int64(10000), p.AmountCents)

	c := h.parkedStep(t, chargeWF)
	require.Equal(t, "charge", c.Step)
	require.Equal(t, engine.DeadAttemptsExhaustedInDoubt, c.DeadReason)
	require.Equal(t, int32(2), c.Attempts)

	only, err := h.admin().ListParked(context.Background(), engine.DeadPermanentAfterInDoubt)
	require.NoError(t, err)
	require.Len(t, only, 1)
	require.Equal(t, payoutWF, only[0].WorkflowID)
}

func TestAdmin_RefusesNonParkedWork(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep())) // charge SUCCEEDED, payout PENDING, workflow RUNNING
	charge := scalar[uuid.UUID](t, h.pool, `SELECT id FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf)
	payout := scalar[uuid.UUID](t, h.pool, `SELECT id FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf)
	a := h.admin()

	for name, step := range map[string]uuid.UUID{"succeeded step": charge, "pending step": payout} {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, a.RetryStep(ctx, step, h.version(t, step), "r"), engine.ErrNotActionable)
			_, err := a.NewGeneration(ctx, step, h.version(t, step), "r", true)
			require.ErrorIs(t, err, engine.ErrNotActionable)
			require.ErrorIs(t, a.ConfirmStep(ctx, step, h.version(t, step), "po_x", "r"), engine.ErrNotActionable)
		})
	}
	require.ErrorIs(t, a.Compensate(ctx, wf, h.version(t, wf), "r", true), engine.ErrNotActionable)
	require.ErrorIs(t, a.Fail(ctx, wf, h.version(t, wf), "r", true), engine.ErrNotActionable)

	require.Equal(t, "RUNNING", h.workflowStatus(t, wf))
	require.Equal(t, "PENDING", h.stepStatus(t, wf, "payout"))
	require.Zero(t, h.auditRows(t, wf))
}

func TestAdmin_RequiresReason(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := parkCharge(t, h, fakeprovider.ModeErrorBeforeEffect)
	step := h.parkedStep(t, wf).StepID

	require.Error(t, h.admin().RetryStep(ctx, step, h.version(t, step), "   "))
	require.Equal(t, "DEAD", h.stepStatus(t, wf, "charge"))
	require.Zero(t, h.auditRows(t, wf))
}

func TestAdmin_RetryStepKeepsDoubtSticky(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := parkCharge(t, h, fakeprovider.ModeErrorBeforeEffect) // 5xx: ambiguous → attempts_exhausted_in_doubt
	step := h.parkedStep(t, wf).StepID
	epoch := scalar[int64](t, h.pool, `SELECT lease_epoch FROM steps WHERE id = $1`, step)

	require.NoError(t, h.admin().RetryStep(ctx, step, h.version(t, step), "provider outage over"))
	require.Equal(t, "IN_DOUBT", h.stepStatus(t, wf, "charge"), "money may have moved: doubt stays sticky")
	// Attempt history is kept (attempt numbers must stay unique); the budget is extended instead.
	require.Equal(t, 2, scalar[int](t, h.pool, `SELECT attempt_count FROM steps WHERE id = $1`, step))
	require.Equal(t, 2+8, scalar[int](t, h.pool, `SELECT max_attempts FROM steps WHERE id = $1`, step))
	require.Equal(t, epoch+1, scalar[int64](t, h.pool, `SELECT lease_epoch FROM steps WHERE id = $1`, step))
	require.Equal(t, "RUNNING", h.workflowStatus(t, wf))

	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
	require.Equal(t, "SUCCEEDED", h.workflowStatus(t, wf))
	require.Len(t, h.effectsOf(wf, "charge"), 1)
	h.assertNoViolations(t)
}

func TestAdmin_RetryStepNoEffectGoesPending(t *testing.T) {
	h := newHarness(t)
	wf := parkCharge(t, h, fakeprovider.ModeRateLimit)
	require.Equal(t, engine.DeadAttemptsExhaustedNoEffect, h.parkedStep(t, wf).DeadReason)

	require.NoError(t, h.admin().RetryStep(context.Background(), h.parkedStep(t, wf).StepID, h.version(t, wf), "rate limit lifted"))
	require.Equal(t, "PENDING", h.stepStatus(t, wf, "charge"))
}

func TestAdmin_NewGenerationGuards(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := parkCharge(t, h, fakeprovider.ModeErrorBeforeEffect)
	step := h.parkedStep(t, wf).StepID

	_, err := h.admin().NewGeneration(ctx, step, h.version(t, step), "verified", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "confirm")
	require.Equal(t, 1, h.count(t, `SELECT count(*) FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf))
	require.Zero(t, h.auditRows(t, wf))
}

func TestAdmin_NewGenerationSendsNewKey(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := parkCharge(t, h, fakeprovider.ModeErrorBeforeEffect) // no money moved
	old := h.parkedStep(t, wf).StepID

	id, err := h.admin().NewGeneration(ctx, old, h.version(t, old), "provider confirmed no charge for the old key", true)
	require.NoError(t, err)
	require.NotEqual(t, old, id)
	require.Equal(t, 1, scalar[int](t, h.pool, `SELECT generation FROM steps WHERE id = $1`, id))
	require.Equal(t, workflow.StepIdempotencyKey(wf, "charge", 1), scalar[string](t, h.pool, `SELECT idempotency_key FROM steps WHERE id = $1`, id))
	require.Equal(t, "DEAD", scalar[string](t, h.pool, `SELECT status FROM steps WHERE id = $1`, old), "the old step stays as history")

	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
	require.Equal(t, "SUCCEEDED", h.workflowStatus(t, wf))
	charges := h.effectsOf(wf, "charge")
	require.Len(t, charges, 1)
	require.Equal(t, workflow.StepIdempotencyKey(wf, "charge", 1), charges[0].IdempotencyKey)
	h.assertNoViolations(t)
}

func TestAdmin_ConfirmStepPostsOnceAndAdvances(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModeTimeoutAfterEffect})
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	h.setStepMaxAttempts(t, wf, "payout", 1)
	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
	paid := h.effectsOf(wf, "payout")
	require.Len(t, paid, 1, "the merchant really was paid")
	step := h.parkedStep(t, wf).StepID

	require.NoError(t, h.admin().ConfirmStep(ctx, step, h.version(t, step), paid[0].ID, "provider dashboard shows the payout"))
	require.Equal(t, "SUCCEEDED", h.stepStatus(t, wf, "payout"))
	require.Equal(t, paid[0].ID, scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE id = $1`, step))
	require.Equal(t, 1, h.count(t, `SELECT count(*) FROM ledger_transactions WHERE workflow_id = $1 AND kind = 'payout'`, wf))
	require.Equal(t, "SUCCEEDED", h.workflowStatus(t, wf))

	require.ErrorIs(t, h.admin().ConfirmStep(ctx, step, h.version(t, step), paid[0].ID, "again"), engine.ErrNotActionable)
	require.Equal(t, 1, h.count(t, `SELECT count(*) FROM ledger_transactions WHERE workflow_id = $1 AND kind = 'payout'`, wf))
	h.assertNoViolations(t) // I3 now checks this workflow again
}

func TestAdmin_CompensateParkedPayout(t *testing.T) {
	h := newHarness(t)
	wf := parkPayoutAfterDoubt(t, h)

	require.NoError(t, h.admin().Compensate(context.Background(), wf, h.version(t, wf), "provider confirms no payout; account closed", true))
	require.Equal(t, "COMPENSATING", h.workflowStatus(t, wf))
	chargeRef := scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf)
	require.Equal(t, chargeRef, scalar[string](t, h.pool, `SELECT refund_charge_ref FROM steps WHERE workflow_id = $1 AND name = 'refund'`, wf))

	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
	require.Equal(t, "COMPENSATED", h.workflowStatus(t, wf))
	for account, total := range h.accountSums(t, wf) {
		require.Zero(t, total, account)
	}
	h.assertNoViolations(t)
}

func TestAdmin_CompensateRefusesNonPayout(t *testing.T) {
	h := newHarness(t)
	wf := parkCharge(t, h, fakeprovider.ModeErrorBeforeEffect)
	require.ErrorIs(t, h.admin().Compensate(context.Background(), wf, h.version(t, wf), "r", true), engine.ErrNotActionable)
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
}

func TestAdmin_FailParkedCharge(t *testing.T) {
	h := newHarness(t)
	wf := parkCharge(t, h, fakeprovider.ModeErrorBeforeEffect)

	require.NoError(t, h.admin().Fail(context.Background(), wf, h.version(t, wf), "provider confirms the customer was never charged", true))
	require.Equal(t, "FAILED", h.workflowStatus(t, wf))
	h.assertNoViolations(t) // I7: FAILED ⇒ no effects
}

func TestAdmin_AuditRows(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := parkCharge(t, h, fakeprovider.ModeErrorBeforeEffect)
	step := h.parkedStep(t, wf).StepID

	require.NoError(t, h.admin().RetryStep(ctx, step, h.version(t, step), "outage over"))
	require.Equal(t, 1, h.auditRows(t, wf))
	var actor, action, reason string
	var stepID uuid.UUID
	require.NoError(t, h.pool.QueryRow(ctx,
		`SELECT actor, action, step_id, reason FROM admin_actions WHERE workflow_id = $1`, wf).Scan(&actor, &action, &stepID, &reason))
	require.Equal(t, "tester", actor)
	require.Equal(t, "retry_step", action)
	require.Equal(t, step, stepID)
	require.Equal(t, "outage over", reason)
}

// Review finding 1: compensating a payout that may have paid the merchant (or
// failing a charge that may have charged the customer) is irreversible; like
// new-generation it needs explicit confirmation that no money moved.
func TestAdmin_CompensateAndFailInDoubtRequireConfirmation(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	payoutWF := parkPayoutAfterDoubt(t, h)
	err := h.admin().Compensate(ctx, payoutWF, h.version(t, payoutWF), "looks unpaid", false)
	require.ErrorIs(t, err, engine.ErrConfirmationRequired)
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, payoutWF))
	require.Zero(t, h.auditRows(t, payoutWF))

	chargeWF := parkCharge(t, h, fakeprovider.ModeErrorBeforeEffect) // attempts_exhausted_in_doubt
	require.ErrorIs(t, h.admin().Fail(ctx, chargeWF, h.version(t, chargeWF), "looks uncharged", false), engine.ErrConfirmationRequired)
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, chargeWF))

	noEffectWF := parkCharge(t, h, fakeprovider.ModeRateLimit) // provably no money moved: no confirmation needed
	require.NoError(t, h.admin().Fail(ctx, noEffectWF, h.version(t, noEffectWF), "giving up", false))
}

// parkRefusedRefundAfterDoubt parks a refund whose first attempt was ambiguous
// and whose retry was refused (a non-pivot step: FAILED_PERMANENT, no dead_reason).
func parkRefusedRefundAfterDoubt(t *testing.T, h harness) uuid.UUID {
	t.Helper()
	wf := submitTo(t, h, "cus_1", "mer_invalid_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep(), h.payoutStep()))
	h.setFaults(
		fakeprovider.Rule{Endpoint: "refunds", Mode: fakeprovider.ModeErrorBeforeEffect, Times: 1},
		fakeprovider.Rule{Endpoint: "refunds", Mode: fakeprovider.ModeDecline, Times: 1},
	)
	h.drain(t, h.workerWith("w1", nil, h.refundStep()))
	require.Equal(t, "FAILED_PERMANENT", h.stepStatus(t, wf, "refund"))
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
	h.setFaults()
	return wf
}

// Review finding 2: doubt is sticky from the attempt history, not only from
// dead_reason (a refused refund has none).
func TestAdmin_RetryRefusedRefundAfterDoubtStaysInDoubt(t *testing.T) {
	h := newHarness(t)
	wf := parkRefusedRefundAfterDoubt(t, h)
	require.NoError(t, h.admin().RetryStep(context.Background(), h.parkedStep(t, wf).StepID, h.version(t, wf), "retry"))
	require.Equal(t, "IN_DOUBT", h.stepStatus(t, wf, "refund"))
}

// Review findings 2 and 3: a refused refund can be confirmed when the money
// was in fact returned (an earlier ambiguous attempt, or a refund made outside
// idem), which closes the workflow COMPENSATED with a consistent ledger.
func TestAdmin_ConfirmRefusedRefund(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := parkRefusedRefundAfterDoubt(t, h)
	chargeRef := scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf)
	// Support refunded the customer directly at the provider.
	out := h.client.CreateRefund(ctx, "support-refund-1",
		provider.RefundRequest{AmountCents: 10000, Currency: "EUR", ChargeID: chargeRef, WorkflowID: wf})
	require.Equal(t, provider.Success, out.Class, "err: %v", out.Err)

	require.NoError(t, h.admin().ConfirmStep(ctx, h.parkedStep(t, wf).StepID, h.version(t, wf), out.ProviderRef, "refunded by support"))
	require.Equal(t, "COMPENSATED", h.workflowStatus(t, wf))
	for account, total := range h.accountSums(t, wf) {
		require.Zero(t, total, account)
	}
	h.assertNoViolations(t)
}

// Review finding 4: a provider reference already recorded on another step
// (e.g. another workflow's charge) must be refused.
func TestAdmin_ConfirmRejectsProviderRefAlreadyUsed(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	other := submitTo(t, h, "cus_9", "mer_9")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	otherRef := scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE workflow_id = $1 AND name = 'charge'`, other)

	wf := parkCharge(t, h, fakeprovider.ModeErrorBeforeEffect)
	err := h.admin().ConfirmStep(ctx, h.parkedStep(t, wf).StepID, h.version(t, wf), otherRef, "typo")
	require.ErrorIs(t, err, engine.ErrNotActionable)
	require.Contains(t, err.Error(), "already recorded")
	require.Equal(t, "DEAD", h.stepStatus(t, wf, "charge"))
}

// version returns the current version of the workflow that id (a workflow or
// one of its steps) belongs to: what `idem admin list` shows an operator.
func (h harness) version(t *testing.T, id uuid.UUID) int64 {
	t.Helper()
	return scalar[int64](t, h.pool, `SELECT w.version FROM workflows w WHERE w.id = $1
		OR w.id = (SELECT workflow_id FROM steps WHERE id = $1)`, id)
}

// Deferred minor 1: an action decided on an old listing must be refused once
// the parked workflow changed. Operator A sees "no money moved"; operator B
// retries, the retry really charges the customer and parks again, in doubt.
// A's --confirm-no-effect was true when A checked, and is now false.
func TestAdmin_RefusesStaleVersion(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := parkCharge(t, h, fakeprovider.ModeRateLimit)
	seen := h.parkedStep(t, wf)
	require.Equal(t, engine.DeadAttemptsExhaustedNoEffect, seen.DeadReason)

	require.NoError(t, h.admin().RetryStep(ctx, seen.StepID, seen.Version, "B: rate limit lifted"))
	h.setStepMaxAttempts(t, wf, "charge", 3) // one more attempt
	h.setFaults(fakeprovider.Rule{Endpoint: "charges", Mode: fakeprovider.ModeTimeoutAfterEffect})
	h.drain(t, h.noRetryAfter())
	h.setFaults()
	now := h.parkedStep(t, wf)
	require.Equal(t, seen.StepID, now.StepID, "the same step, parked again")
	require.Equal(t, engine.DeadAttemptsExhaustedInDoubt, now.DeadReason)
	require.Len(t, h.effectsOf(wf, "charge"), 1, "the customer really was charged")
	require.NotEqual(t, seen.Version, now.Version)

	a := h.admin()
	require.ErrorIs(t, a.Fail(ctx, wf, seen.Version, "A: provider showed no charge", true), engine.ErrStale)
	require.ErrorIs(t, a.RetryStep(ctx, seen.StepID, seen.Version, "A"), engine.ErrStale)
	require.ErrorIs(t, a.ConfirmStep(ctx, seen.StepID, seen.Version, "ch_x", "A"), engine.ErrStale)
	_, err := a.NewGeneration(ctx, seen.StepID, seen.Version, "A", true)
	require.ErrorIs(t, err, engine.ErrStale)
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
	require.Equal(t, 1, h.auditRows(t, wf), "only B's retry")

	// With the current version, the decision is made on the current facts.
	require.ErrorIs(t, a.Fail(ctx, wf, now.Version, "A, re-checked", false), engine.ErrConfirmationRequired)
}

// Slice 2.2 (D18): provider news about a parked step is shown to the operator.
func TestAdmin_ListShowsProviderEvidence(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := parkUnresolvedPayout(t, h)
	h.fake.SettleNow()
	ref := scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf)
	h.storeEvent(t, wf, webhook.EventPayoutPaid, "")
	require.Equal(t, "evidence", h.inboxOnce(t, h.inboxProcessor()))

	require.Equal(t, "provider says paid "+ref, h.parkedStep(t, wf).Evidence)
}

// Slice 2.2 (D16): a real conflict pages until a human resolves it, audited.
func TestAdmin_ResolveConflict(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, time.Hour)
	wf := parkUnresolvedPayout(t, h)
	p := h.parkedStep(t, wf)
	require.NoError(t, h.admin().Compensate(ctx, wf, p.Version, "provider shows no payout", true))
	h.fake.SettleNow()
	id := h.storeEvent(t, wf, webhook.EventPayoutPaid, "")
	require.Equal(t, "conflict", h.inboxOnce(t, h.inboxProcessor()))

	conflicts, err := h.admin().ListConflicts(ctx)
	require.NoError(t, err)
	require.Len(t, conflicts, 1)
	require.Equal(t, id, conflicts[0].EventID)
	require.Equal(t, wf, conflicts[0].WorkflowID)
	require.Contains(t, conflicts[0].Note, "provider says paid")

	require.Error(t, h.admin().ResolveConflict(ctx, id, "  "), "a reason is required")
	before := h.auditRows(t, wf)
	require.NoError(t, h.admin().ResolveConflict(ctx, id, "clawed back from the merchant"))
	require.Equal(t, before+1, h.auditRows(t, wf))
	require.Equal(t, "resolve_conflict", scalar[string](t, h.pool, `SELECT action FROM admin_actions WHERE workflow_id = $1 ORDER BY at DESC LIMIT 1`, wf))
	status, _ := h.inboxRow(t, id)
	require.Equal(t, "CONFLICT_RESOLVED", status)
	require.ErrorIs(t, h.admin().ResolveConflict(ctx, id, "again"), engine.ErrNotActionable)
	conflicts, err = h.admin().ListConflicts(ctx)
	require.NoError(t, err)
	require.Empty(t, conflicts)
}

// Slice 2.3b (review R9): one definition of "an earlier send may have moved
// money". The admin check used to leave out attempts recorded DEAD, which the
// claim and inbox queries count.
func TestMayHaveMoved_DeadSendAttemptCounts(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := parkCharge(t, h, fakeprovider.ModeRateLimit) // provably no effect so far
	step := h.parkedStep(t, wf).StepID
	plant(t, `
		UPDATE steps SET status = 'FAILED_PERMANENT', dead_reason = NULL WHERE id = $1;`, step)
	_, err := h.pool.Exec(ctx, `
		UPDATE step_attempts SET outcome = 'DEAD'
		WHERE step_id = $1 AND kind = 'send' AND attempt_no = (SELECT min(attempt_no) FROM step_attempts WHERE step_id = $1)`, step)
	require.NoError(t, err)

	err = h.admin().Fail(ctx, wf, h.version(t, wf), "looks uncharged", false)
	require.ErrorIs(t, err, engine.ErrConfirmationRequired)
}

// reconRun records a successful reconciliation run (as recon.Run would) and
// returns its ID; discrepancies are (workflow, kind, type, parked).
func (h harness) reconRun(t *testing.T, ago time.Duration, ds ...[4]string) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	id := uuid.Must(uuid.NewV7())
	_, err := h.pool.Exec(ctx, `INSERT INTO recon_runs (id, window_from, window_to) VALUES ($1, now() - interval '2 hours', now() - interval '1 hour')`, id)
	require.NoError(t, err)
	_, err = h.pool.Exec(ctx, `UPDATE recon_runs SET status = 'SUCCEEDED', finished_at = now() - $2::interval, discrepancies = $3 WHERE id = $1`,
		id, ago.String(), len(ds))
	require.NoError(t, err)
	for _, d := range ds {
		_, err := h.pool.Exec(ctx, `INSERT INTO recon_discrepancies (id, run_id, workflow_id, kind, effect_type, provider_refs, provider_amount, parked, detail)
			VALUES (gen_random_uuid(), $1, $2, $3, $4, '{po_1}', -9825, $5, 'the provider moved -9825 (payout po_1) with no ledger posting')`,
			id, d[0], d[1], d[2], d[3] == "true")
		require.NoError(t, err)
	}
	return id
}

// Slice 2.6: the latest successful run, by default.
func TestAdmin_ListReconciliationLatestRun(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_1")
	old := h.reconRun(t, time.Hour, [4]string{wf.String(), "amount_mismatch", "charge", "false"})
	latest := h.reconRun(t, time.Minute, [4]string{wf.String(), "missing_in_ledger", "payout", "false"})

	run, ds, err := h.admin().ListReconciliation(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, latest, run.ID)
	require.Len(t, ds, 1)
	require.Equal(t, "missing_in_ledger", ds[0].Kind)
	require.Equal(t, int64(-9825), *ds[0].ProviderAmount)

	run, ds, err = h.admin().ListReconciliation(ctx, &old)
	require.NoError(t, err)
	require.Equal(t, old, run.ID)
	require.Equal(t, "amount_mismatch", ds[0].Kind)
}

func TestAdmin_ListReconciliationWithoutRuns(t *testing.T) {
	h := newHarness(t)
	_, _, err := h.admin().ListReconciliation(context.Background(), nil)
	require.ErrorIs(t, err, engine.ErrNotActionable)
}

// A parked workflow's discrepancies are evidence next to it: they tell the
// operator whether the provider moved the money (confirm-step) or not
// (compensate, fail).
func TestAdmin_ListParkedShowsReconEvidence(t *testing.T) {
	h := newHarness(t)
	wf := parkPayoutAfterDoubt(t, h)
	h.reconRun(t, time.Minute, [4]string{wf.String(), "missing_in_ledger", "payout", "true"})
	p := h.parkedStep(t, wf)
	require.Equal(t, "missing_in_ledger payout -9825", p.Reconciliation)
}
