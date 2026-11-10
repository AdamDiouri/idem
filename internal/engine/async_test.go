package engine_test

import (
	"context"
	"log/slog"
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
	// Zero backoff polls every tick, so this test (about settling through a
	// re-send) uses a large budget; the budget itself is pinned by
	// TestAdmin_RetryCallbackUnresolvedGetsAFreshPollBudget.
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

// Final review, Important 1: retry-step after callback_unresolved must give the
// re-sent (replayed) payout a fresh poll budget. The budget used to be total
// polls, so the first poll after the retry parked it again.
func TestAdmin_RetryCallbackUnresolvedGetsAFreshPollBudget(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, time.Hour) // the provider stays slow
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", polling(0, 2), h.allSteps()...))
	p := h.parkedStep(t, wf)
	require.NoError(t, h.admin().RetryStep(ctx, p.StepID, p.Version, "provider says it is still processing"))

	w := h.workerWith("w1", polling(0, 2), h.allSteps()...) // the same budget as before
	require.Equal(t, "awaiting_callback", h.runOnce(t, w).Outcome, "the re-send is accepted again")
	require.Equal(t, "awaiting_callback", h.runOnce(t, w).Outcome, "first poll after the retry: still within budget")
	require.Equal(t, "dead", h.runOnce(t, w).Outcome, "second poll: the fresh budget of 2 is used up")
	require.Equal(t, engine.DeadCallbackUnresolved,
		scalar[string](t, h.pool, `SELECT dead_reason FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
}

// Final review, Important 2: a step that already holds an accepted payout's
// reference resolves doubt by polling that payout, not by key lookup. The
// lookup misread a settled "failed" as ambiguous and burned the retry budget.
func TestFlow_InDoubtAcceptedPayoutNearRetentionPollsByRef(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, time.Hour)
	h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModePayoutFail, Times: 1})
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", polling(0, 1), h.allSteps()...)) // accepted, one poll (pending), parked
	p := h.parkedStep(t, wf)
	require.Equal(t, engine.DeadCallbackUnresolved, p.DeadReason)
	require.NoError(t, h.admin().RetryStep(ctx, p.StepID, p.Version, "weekend later"))
	// The provider has long since settled it failed, and the key is old.
	_, err := h.pool.Exec(ctx, `UPDATE steps SET first_attempted_at = now() - interval '30 hours' WHERE id = $1`, p.StepID)
	require.NoError(t, err)

	w := h.workerWith("w1", func(o *engine.WorkerOptions) {
		o.CallbackDeadline, o.MaxPolls = 0, 8
		o.KeyRetention, o.KeyRetentionMargin = 24*time.Hour, time.Hour
	}, h.allSteps()...)
	h.fake.SettleNow() // the bank rejected it while the step was parked
	h.runOnce(t, w)

	require.Equal(t, "DEAD", h.stepStatus(t, wf, "payout"))
	require.Equal(t, engine.DeadPermanentAfterInDoubt,
		scalar[string](t, h.pool, `SELECT dead_reason FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf),
		"failed, after the operator's doubt: parked with the true reason, not attempts_exhausted_in_doubt")
	require.Zero(t, h.fake.Calls()["GET /payouts"], "no lookup by key: the payout reference is known")
}

// Fix before merge: a payout that ran out of attempts with provably no effect
// (rate limited every time) moved no money. After an operator's retry, a clean
// async failure must refund the customer, as in M1, not park as doubt.
func TestFlow_NoEffectExhaustionIsNotDoubt(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, 0)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	h.setStepMaxAttempts(t, wf, "payout", 2)
	h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModeRateLimit})
	h.drain(t, h.noRetryAfter())
	p := h.parkedStep(t, wf)
	require.Equal(t, engine.DeadAttemptsExhaustedNoEffect, p.DeadReason)

	h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModePayoutFail, Times: 1})
	require.NoError(t, h.admin().RetryStep(ctx, p.StepID, p.Version, "rate limit lifted"))
	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))

	require.Equal(t, "COMPENSATED", h.workflowStatus(t, wf), "no attempt ever moved money: a clean failure refunds")
	h.assertNoViolations(t)
}

// Fix before merge: the same key always returns the same payout, so confirming
// a step that holds its provider reference with a DIFFERENT one is almost
// certainly a typo, and posting it would record a payout that isn't this one.
func TestAdmin_ConfirmRefusesAnotherRefForAcceptedPayout(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, time.Hour)
	wf := parkUnresolvedPayout(t, h)
	p := h.parkedStep(t, wf)
	own := scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE id = $1`, p.StepID)

	err := h.admin().ConfirmStep(ctx, p.StepID, p.Version, "po_typo", "dashboard says paid")
	require.ErrorIs(t, err, engine.ErrNotActionable)
	require.ErrorContains(t, err, own, "the message names the reference to use")
	require.Equal(t, "DEAD", h.stepStatus(t, wf, "payout"))
	require.Zero(t, h.auditRows(t, wf))
}

// Fix before merge: an operator looking at callback_unresolved needs to see
// how many polls ran, not only sends.
func TestAdmin_ListShowsPolls(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := parkUnresolvedPayout(t, h) // 1 send, 2 polls
	p := h.parkedStep(t, wf)
	require.Equal(t, int32(1), p.Attempts)
	require.Equal(t, int32(2), p.Polls)
}

// Fix before merge: poll logs say which poll it was and when the next one is.
func TestWorker_LogsPollProgress(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	rec := &recordingHandler{}
	submitTo(t, h, "cus_1", "mer_1")
	w := h.workerWith("w1", func(o *engine.WorkerOptions) { o.Logger = slog.New(rec) }, h.allSteps()...)
	h.runOnce(t, w) // charge
	h.runOnce(t, w) // payout accepted
	h.runOnce(t, w) // first poll: still pending
	require.True(t, rec.find("waiting for the provider's result", "poll_count", "1"))
	require.True(t, rec.find("waiting for the provider's result", "polls_since_send", "1"))
}
