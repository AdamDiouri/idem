package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/fakeprovider"
)

// Slice 2.1 (M2 design §4): payouts are accepted (202), wait in
// AWAITING_CALLBACK, and are completed by polling after the callback deadline.
// There are no webhooks yet, so polling alone must finish every payout (F18).

func asyncHarness(t *testing.T, settle time.Duration) harness {
	t.Helper()
	return newHarnessWith(t, fakeprovider.Options{Hang: 5 * time.Second, PayoutSettle: settle})
}

func polling(deadline time.Duration, maxPolls int32) func(*engine.WorkerOptions) {
	return func(o *engine.WorkerOptions) { o.CallbackDeadline, o.MaxPolls = deadline, maxPolls }
}

func (h harness) payoutAttemptKinds(t *testing.T, wf uuid.UUID) []string {
	t.Helper()
	rows, err := h.pool.Query(context.Background(), `
		SELECT a.kind FROM step_attempts a JOIN steps s ON s.id = a.step_id
		WHERE s.workflow_id = $1 AND s.name = 'payout' ORDER BY s.generation, a.attempt_no`, wf)
	require.NoError(t, err)
	defer rows.Close()
	var kinds []string
	for rows.Next() {
		var k string
		require.NoError(t, rows.Scan(&k))
		kinds = append(kinds, k)
	}
	require.NoError(t, rows.Err())
	return kinds
}

func (h harness) payoutPostings(t *testing.T, wf uuid.UUID) int {
	t.Helper()
	return h.count(t, `SELECT count(*) FROM ledger_transactions WHERE workflow_id = $1 AND kind = 'payout'`, wf)
}

// runUntil runs w until the workflow reaches want (payouts settle on a clock).
func (h harness) runUntil(t *testing.T, w *engine.Worker, wf uuid.UUID, want string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, _ = w.RunOnce(context.Background())
		return h.workflowStatus(t, wf) == want
	}, 10*time.Second, 20*time.Millisecond)
}

func TestFlow_PayoutWaitsThenPollingPays_F18(t *testing.T) {
	h := asyncHarness(t, 150*time.Millisecond)
	wf := submitTo(t, h, "cus_1", "mer_1")
	w := h.workerWith("w1", polling(300*time.Millisecond, 8), h.allSteps()...)
	h.drain(t, w) // charge, then the payout is accepted and waits

	require.Equal(t, "AWAITING_CALLBACK", h.stepStatus(t, wf, "payout"))
	require.Equal(t, "RUNNING", h.workflowStatus(t, wf))
	ref := scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf)
	require.Regexp(t, `^po_`, ref)
	require.True(t, scalar[bool](t, h.pool, `SELECT callback_deadline > now() FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	require.Zero(t, h.payoutPostings(t, wf), "nothing is posted until the payout is paid")

	h.runUntil(t, w, wf, "SUCCEEDED")
	require.Equal(t, 1, h.payoutPostings(t, wf))
	payouts := h.effectsOf(wf, "payout")
	require.Len(t, payouts, 1)
	require.Equal(t, ref, payouts[0].ID)
	kinds := h.payoutAttemptKinds(t, wf)
	require.Equal(t, "send", kinds[0])
	require.Contains(t, kinds[1:], "poll")
	require.NotContains(t, kinds[1:], "send", "a waiting payout is never re-sent")
	h.assertNoViolations(t)
}

func TestFlow_AsyncPayoutFailureRefunds(t *testing.T) {
	h := asyncHarness(t, 0)
	h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModePayoutFail, Times: 1})
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))

	require.Equal(t, "COMPENSATED", h.workflowStatus(t, wf))
	require.Equal(t, "FAILED_PERMANENT", h.stepStatus(t, wf, "payout"))
	require.Empty(t, h.effectsOf(wf, "payout"))
	require.Len(t, h.effectsOf(wf, "refund"), 1)
	for account, total := range h.accountSums(t, wf) {
		require.Zero(t, total, account)
	}
	h.assertNoViolations(t)
}

// Review focus 4: the provider accepted the payout but the response was lost.
// The re-send replays the same 202 and payout ID, so idem waits on the same
// payout instead of creating a second one.
func TestFlow_AcceptedThenTimeoutWaitsOnSamePayout(t *testing.T) {
	h := asyncHarness(t, 0)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModeTimeoutAfterEffect, Times: 1})
	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))

	require.Equal(t, "SUCCEEDED", h.workflowStatus(t, wf))
	payouts := h.effectsOf(wf, "payout")
	require.Len(t, payouts, 1, "one payout, however many times it was sent")
	require.Equal(t, payouts[0].ID, scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	h.assertNoViolations(t)
}

// Review focus 1: a crashed poll sent nothing, so a later clean failure must
// refund the customer, not park the workflow.
func TestFlow_PollCrashIsNotDoubt(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, 0)
	h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModePayoutFail, Times: 1})
	wf := submitTo(t, h, "cus_1", "mer_1")
	w := h.workerWith("w1", polling(time.Hour, 8), h.allSteps()...)
	h.drain(t, w) // charge succeeded; payout accepted, waiting
	_, err := h.pool.Exec(ctx, `UPDATE steps SET run_after = now(), callback_deadline = now() WHERE workflow_id = $1 AND name = 'payout'`, wf)
	require.NoError(t, err)

	_, ok, err := engine.Claim(ctx, h.pool, "crashy", []string{"payout"}, 10*time.Millisecond) // the poll's worker dies
	require.NoError(t, err)
	require.True(t, ok)
	time.Sleep(30 * time.Millisecond)
	require.EqualValues(t, 1, h.reclaim(t))
	require.Equal(t, "AWAITING_CALLBACK", h.stepStatus(t, wf, "payout"), "not IN_DOUBT: the poll sent nothing")

	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
	require.Equal(t, "COMPENSATED", h.workflowStatus(t, wf), "a clean failure refunds; the crashed poll is not doubt")
	h.assertNoViolations(t)
}

// Review focus 2 and 5: a provider that never settles ends in a bounded DEAD,
// and polls never count against the send budget.
func TestFlow_UnsettledPayoutGoesDeadAfterMaxPolls(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", polling(0, 3), h.allSteps()...))

	require.Equal(t, "DEAD", h.stepStatus(t, wf, "payout"))
	require.Equal(t, engine.DeadCallbackUnresolved,
		scalar[string](t, h.pool, `SELECT dead_reason FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
	require.Equal(t, []string{"send", "poll", "poll", "poll"}, h.payoutAttemptKinds(t, wf))
	require.Equal(t, 1, scalar[int](t, h.pool, `SELECT attempt_count FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	require.Zero(t, h.count(t, `SELECT count(*) FROM steps WHERE attempt_count > max_attempts + 1`))
	require.True(t, engine.MoneyMayHaveMoved(engine.DeadCallbackUnresolved))
	h.assertNoViolations(t)
}

// parkUnresolvedPayout parks a payout whose polls ran out while the provider
// still said pending; the provider settles it (paid) settle after acceptance.
func parkUnresolvedPayout(t *testing.T, h harness) uuid.UUID {
	t.Helper()
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", polling(0, 2), h.allSteps()...))
	require.Equal(t, engine.DeadCallbackUnresolved,
		scalar[string](t, h.pool, `SELECT dead_reason FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	return wf
}

// Review focus 3: after callback_unresolved, the operator confirms the payout
// with its own po_ reference. That is not a reuse of another step's reference.
func TestAdmin_ConfirmCallbackUnresolvedWithOwnRef(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, 500*time.Millisecond)
	wf := parkUnresolvedPayout(t, h)
	require.Eventually(t, func() bool { return len(h.effectsOf(wf, "payout")) == 1 }, 5*time.Second, 20*time.Millisecond,
		"the provider pays it later")
	p := h.parkedStep(t, wf)
	ref := scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE id = $1`, p.StepID)

	require.NoError(t, h.admin().ConfirmStep(ctx, p.StepID, p.Version, ref, "provider dashboard: paid"))
	require.Equal(t, "SUCCEEDED", h.workflowStatus(t, wf))
	require.Equal(t, 1, h.payoutPostings(t, wf))
	h.assertNoViolations(t)
}

// Review focus 5: retrying callback_unresolved re-sends (same key, so the
// provider replays the same payout) and polling resumes. Sends after polls
// must not collide on attempt numbers.
func TestAdmin_RetryCallbackUnresolvedResumesPolling(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, 500*time.Millisecond)
	wf := parkUnresolvedPayout(t, h)
	p := h.parkedStep(t, wf)
	ref := scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE id = $1`, p.StepID)

	require.NoError(t, h.admin().RetryStep(ctx, p.StepID, p.Version, "provider says it is still processing"))
	require.Equal(t, "IN_DOUBT", h.stepStatus(t, wf, "payout"), "money may have moved: doubt is sticky")
	// Zero backoff polls every tick: give it a budget that outlasts the settlement.
	h.runUntil(t, h.workerWith("w1", polling(0, 1000), h.allSteps()...), wf, "SUCCEEDED")

	payouts := h.effectsOf(wf, "payout")
	require.Len(t, payouts, 1)
	require.Equal(t, ref, payouts[0].ID, "the re-send replayed the same payout")
	kinds := h.payoutAttemptKinds(t, wf)
	require.Equal(t, []string{"send", "poll", "poll", "send"}, kinds[:4])
	require.Equal(t, h.count(t, `SELECT count(*) FROM step_attempts WHERE step_id = $1`, p.StepID),
		h.count(t, `SELECT count(DISTINCT attempt_no) FROM step_attempts WHERE step_id = $1`, p.StepID))
	h.assertNoViolations(t)
}
