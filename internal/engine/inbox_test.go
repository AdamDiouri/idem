package engine_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/webhook"
)

// Slice 2.2 (M2 design §4.4): provider webhooks are applied from the inbox
// through the step's own CAS path; news that disagrees with a decided result,
// or concerns a parked step, is re-checked with the provider first.

func (h harness) inboxProcessor() *engine.InboxProcessor {
	return engine.NewInboxProcessor(h.pool, engine.InboxOptions{
		ID: "inbox-1", Steps: h.allSteps(), LeaseTTL: leaseTTL, OrphanAfter: time.Hour,
	})
}

// waitingPayout drives a workflow until its payout is accepted and waiting
// (no poll is due for an hour).
func waitingPayout(t *testing.T, h harness, faults ...fakeprovider.Rule) uuid.UUID {
	t.Helper()
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	h.setFaults(faults...)
	h.drain(t, h.workerWith("w1", polling(time.Hour, 0), h.allSteps()...))
	h.setFaults()
	require.Equal(t, "AWAITING_CALLBACK", h.stepStatus(t, wf, "payout"))
	return wf
}

// storeEvent puts a provider event about wf's latest payout in the inbox, as
// the API would after verifying it. ref overrides the payout ID when set.
func (h harness) storeEvent(t *testing.T, wf uuid.UUID, typ, ref string) string {
	t.Helper()
	var key string
	var stored *string
	require.NoError(t, h.pool.QueryRow(context.Background(), `SELECT idempotency_key, provider_ref FROM steps
		WHERE workflow_id = $1 AND name = 'payout' ORDER BY generation DESC LIMIT 1`, wf).Scan(&key, &stored))
	if ref == "" && stored != nil {
		ref = *stored
	}
	e := webhook.ProviderEvent{ID: "evt_" + uuid.NewString(), Type: typ, Created: time.Now().Unix()}
	e.Data.PayoutID, e.Data.IdempotencyKey, e.Data.AmountCents, e.Data.Currency = ref, key, 9825, "EUR"
	e.Data.Status = "paid"
	if typ == webhook.EventPayoutFailed {
		e.Data.Status, e.Data.FailureCode = "failed", "account_closed"
	}
	e.Data.Metadata.WorkflowID = wf.String()
	b, err := json.Marshal(e)
	require.NoError(t, err)
	inserted, err := webhook.NewInbox(h.pool).Store(context.Background(), e.ID, e.Type, b)
	require.NoError(t, err)
	require.True(t, inserted)
	return e.ID
}

func (h harness) inboxRow(t *testing.T, id string) (status, note string) {
	t.Helper()
	require.NoError(t, h.pool.QueryRow(context.Background(),
		`SELECT status, COALESCE(note, '') FROM webhook_inbox WHERE provider_event_id = $1`, id).Scan(&status, &note))
	return status, note
}

func (h harness) inboxOnce(t *testing.T, p *engine.InboxProcessor) string {
	t.Helper()
	res, err := p.RunOnce(context.Background())
	require.NoError(t, err)
	require.True(t, res.Claimed, "expected an inbox event to process")
	return res.Outcome
}

// makeDue lets a deferred event be retried now.
func (h harness) makeDue(t *testing.T) {
	t.Helper()
	_, err := h.pool.Exec(context.Background(), `UPDATE webhook_inbox SET next_attempt_at = now() WHERE status = 'PENDING'`)
	require.NoError(t, err)
}

func TestInbox_PaidWebhookCompletesPayout(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := waitingPayout(t, h)
	h.fake.SettleNow()
	id := h.storeEvent(t, wf, webhook.EventPayoutPaid, "")

	require.Equal(t, "applied", h.inboxOnce(t, h.inboxProcessor()))
	require.Equal(t, "SUCCEEDED", h.stepStatus(t, wf, "payout"))
	require.Equal(t, "SUCCEEDED", h.workflowStatus(t, wf))
	require.Equal(t, 1, h.payoutPostings(t, wf))
	require.Contains(t, h.payoutAttemptKinds(t, wf), "webhook")
	status, _ := h.inboxRow(t, id)
	require.Equal(t, "PROCESSED", status)
	h.assertNoViolations(t)
}

func TestInbox_FailedWebhookRefunds(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := waitingPayout(t, h, fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModePayoutFail, Times: 1})
	h.fake.SettleNow()
	h.storeEvent(t, wf, webhook.EventPayoutFailed, "")

	require.Equal(t, "applied", h.inboxOnce(t, h.inboxProcessor()))
	require.Equal(t, "FAILED_PERMANENT", h.stepStatus(t, wf, "payout"))
	require.Equal(t, "COMPENSATING", h.workflowStatus(t, wf))
	require.Equal(t, 1, h.count(t, `SELECT count(*) FROM steps WHERE workflow_id = $1 AND name = 'refund'`, wf))
}

func TestInbox_FailedAfterAmbiguousSendParks(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := waitingPayout(t, h,
		fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModeErrorBeforeEffect, Times: 1},
		fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModePayoutFail, Times: 1})
	h.fake.SettleNow()
	h.storeEvent(t, wf, webhook.EventPayoutFailed, "")

	require.Equal(t, "applied", h.inboxOnce(t, h.inboxProcessor()))
	require.Equal(t, engine.DeadPermanentAfterInDoubt,
		scalar[string](t, h.pool, `SELECT dead_reason FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf), "pivot rule: never refund on a guess")
}

func TestInbox_DuplicateEventIsNoop_F15(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := waitingPayout(t, h)
	h.fake.SettleNow()
	h.storeEvent(t, wf, webhook.EventPayoutPaid, "")
	second := h.storeEvent(t, wf, webhook.EventPayoutPaid, "") // the provider's retry, a new event id
	p := h.inboxProcessor()

	require.Equal(t, "applied", h.inboxOnce(t, p))
	require.Equal(t, "noop", h.inboxOnce(t, p))
	require.Equal(t, 1, h.payoutPostings(t, wf))
	status, note := h.inboxRow(t, second)
	require.Equal(t, "PROCESSED", status)
	require.Equal(t, "duplicate", note)
}

// F16: the webhook arrives while our own send is still in flight (step RUNNING,
// no provider_ref recorded yet): it is matched by idempotency key and deferred.
func TestInbox_WebhookBeforeOurCommitIsDeferred_F16(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, time.Hour)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	s, ok, err := engine.Claim(ctx, h.pool, "w1", []string{"payout"}, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	accepted := h.client.CreatePayout(ctx, s.IdempotencyKey,
		provider.PayoutRequest{AmountCents: 9825, Currency: "EUR", MerchantID: "mer_1", WorkflowID: wf})
	require.Equal(t, provider.Accepted, accepted.Class)
	h.fake.SettleNow()
	h.storeEvent(t, wf, webhook.EventPayoutPaid, accepted.ProviderRef)
	p := h.inboxProcessor()

	require.Equal(t, "deferred", h.inboxOnce(t, p), "our commit has not landed")
	require.NoError(t, engine.Complete(ctx, h.pool, s,
		engine.Result{Kind: engine.ResultAwaitingCallback, ProviderRef: accepted.ProviderRef, RetryDelay: time.Hour}, h.payoutStep()))
	h.makeDue(t)
	require.Equal(t, "applied", h.inboxOnce(t, p))
	require.Equal(t, "SUCCEEDED", h.workflowStatus(t, wf))
	require.Equal(t, 1, h.payoutPostings(t, wf))
}

// Review focus 1 (F30): a webhook while a poll of the same payout is in flight.
func TestInbox_WebhookDuringPollIsDeferredThenNoop(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, time.Hour)
	wf := waitingPayout(t, h)
	h.fake.SettleNow()
	_, err := h.pool.Exec(ctx, `UPDATE steps SET run_after = now(), callback_deadline = now() WHERE workflow_id = $1 AND name = 'payout'`, wf)
	require.NoError(t, err)
	poll, ok, err := engine.Claim(ctx, h.pool, "w1", []string{"payout"}, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	h.storeEvent(t, wf, webhook.EventPayoutPaid, "")
	p := h.inboxProcessor()

	require.Equal(t, "deferred", h.inboxOnce(t, p))
	require.NoError(t, engine.Complete(ctx, h.pool, poll,
		engine.Result{Kind: engine.ResultSucceeded, ProviderRef: poll.ProviderRef}, h.payoutStep()))
	h.makeDue(t)
	require.Equal(t, "noop", h.inboxOnce(t, p))
	require.Equal(t, 1, h.payoutPostings(t, wf))
	h.assertNoViolations(t)
}

// F17: a stale contradiction (the provider's current state agrees with idem).
func TestInbox_StaleContradictionIgnored_F17(t *testing.T) {
	h := asyncHarness(t, 0)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
	require.Equal(t, "SUCCEEDED", h.workflowStatus(t, wf))
	id := h.storeEvent(t, wf, webhook.EventPayoutFailed, "")

	require.Equal(t, "stale", h.inboxOnce(t, h.inboxProcessor()))
	status, note := h.inboxRow(t, id)
	require.Equal(t, "IGNORED", status)
	require.Equal(t, "stale", note)
	require.Equal(t, "SUCCEEDED", h.workflowStatus(t, wf), "never flip a terminal state")
}

// F17: a real contradiction. idem recorded a failure (here: a poll misread,
// simulated by completing the poll as failed) while the provider paid.
func TestInbox_RealContradictionIsConflict_F17(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, time.Hour)
	wf := waitingPayout(t, h)
	h.fake.SettleNow() // the provider paid it
	_, err := h.pool.Exec(ctx, `UPDATE steps SET run_after = now(), callback_deadline = now() WHERE workflow_id = $1 AND name = 'payout'`, wf)
	require.NoError(t, err)
	poll, ok, err := engine.Claim(ctx, h.pool, "w1", []string{"payout"}, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, engine.Complete(ctx, h.pool, poll,
		engine.Result{Kind: engine.ResultFailedPermanent, Error: "misread"}, h.payoutStep())) // idem believes "not paid"
	id := h.storeEvent(t, wf, webhook.EventPayoutPaid, "")

	require.Equal(t, "conflict", h.inboxOnce(t, h.inboxProcessor()))
	status, note := h.inboxRow(t, id)
	require.Equal(t, "CONFLICT", status)
	require.Contains(t, note, "provider says paid")
	require.Equal(t, "FAILED_PERMANENT", h.stepStatus(t, wf, "payout"), "never flip")
}

// Final review, Critical: a stale or reordered "failed" event for a waiting
// payout the provider actually paid must not refund the customer. A webhook is
// a prompt to ask; idem applies what the provider says now.
func TestInbox_StaleFailedEventOnWaitingStepAppliesProviderState(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := waitingPayout(t, h)
	h.fake.SettleNow() // paid
	h.storeEvent(t, wf, webhook.EventPayoutFailed, "")

	require.Equal(t, "applied", h.inboxOnce(t, h.inboxProcessor()))
	require.Equal(t, "SUCCEEDED", h.stepStatus(t, wf, "payout"), "what the provider says now: paid")
	require.Zero(t, h.count(t, `SELECT count(*) FROM steps WHERE workflow_id = $1 AND name = 'refund'`, wf), "no refund")
	require.Equal(t, 1, h.payoutPostings(t, wf))
	h.assertNoViolations(t)
}

// An event about a waiting payout the provider has not settled yet (it cannot
// be true yet) is deferred, not applied.
func TestInbox_EventAheadOfProviderIsDeferred(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := waitingPayout(t, h) // still pending at the provider
	h.storeEvent(t, wf, webhook.EventPayoutPaid, "")

	require.Equal(t, "deferred", h.inboxOnce(t, h.inboxProcessor()))
	require.Equal(t, "AWAITING_CALLBACK", h.stepStatus(t, wf, "payout"))
}

// Final review, Important: a payout parked with no reference of its own (every
// send timed out, though the provider accepted and paid it) still gets
// evidence, re-checked by the event's payout ID.
func TestInbox_ParkedStepWithoutRefGetsEvidence(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	h.setStepMaxAttempts(t, wf, "payout", 1)
	h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModeTimeoutAfterEffect, Times: 1})
	h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
	h.setFaults()
	require.Equal(t, engine.DeadAttemptsExhaustedInDoubt, h.parkedStep(t, wf).DeadReason)
	require.True(t, scalar[bool](t, h.pool, `SELECT provider_ref IS NULL FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	h.fake.SettleNow()
	paid := h.effectsOf(wf, "payout")
	require.Len(t, paid, 1)
	id := h.storeEvent(t, wf, webhook.EventPayoutPaid, paid[0].ID)

	require.Equal(t, "evidence", h.inboxOnce(t, h.inboxProcessor()))
	status, note := h.inboxRow(t, id)
	require.Equal(t, "EVIDENCE", status)
	require.Equal(t, "provider says paid "+paid[0].ID, note)
}

// D18: news about a parked step is evidence for a human, not a resolution.
func TestInbox_ParkedStepGetsEvidence(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := parkUnresolvedPayout(t, h)
	h.fake.SettleNow()
	ref := scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf)
	id := h.storeEvent(t, wf, webhook.EventPayoutPaid, "")

	require.Equal(t, "evidence", h.inboxOnce(t, h.inboxProcessor()))
	status, note := h.inboxRow(t, id)
	require.Equal(t, "EVIDENCE", status)
	require.Equal(t, "provider says paid "+ref, note)
	require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf), "still a human decision")
	require.Equal(t, "DEAD", h.stepStatus(t, wf, "payout"))
}

// The operator refunded the customer; then the provider says the merchant was
// paid: money moved both ways. Loud.
func TestInbox_PaidAfterOperatorCompensatedIsConflict(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, time.Hour)
	wf := parkUnresolvedPayout(t, h)
	p := h.parkedStep(t, wf)
	require.NoError(t, h.admin().Compensate(ctx, wf, p.Version, "provider shows no payout", true))
	h.fake.SettleNow()
	h.storeEvent(t, wf, webhook.EventPayoutPaid, "")

	require.Equal(t, "conflict", h.inboxOnce(t, h.inboxProcessor()))
}

// Review focus 5: an operator acts between the processor's re-check and its
// apply. The processor must defer and re-evaluate, never act on old state.
func TestInbox_StateChangedDuringRecheckDefers(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, time.Hour)
	wf := parkUnresolvedPayout(t, h)
	h.fake.SettleNow()
	id := h.storeEvent(t, wf, webhook.EventPayoutPaid, "")
	p := h.inboxProcessor()
	engine.SetInboxAfterRead(p, func() {
		pk := h.parkedStep(t, wf)
		require.NoError(t, h.admin().Compensate(ctx, wf, pk.Version, "operator, concurrently", true))
	})

	require.Equal(t, "deferred", h.inboxOnce(t, p))
	engine.SetInboxAfterRead(p, nil)
	h.makeDue(t)
	require.Equal(t, "conflict", h.inboxOnce(t, p), "re-evaluated: paid after the operator refunded")
	status, _ := h.inboxRow(t, id)
	require.Equal(t, "CONFLICT", status)
}

// Review focus 4: an event about a payout idem never recorded.
func TestInbox_OrphanIgnoredAfterThreshold(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, time.Hour)
	e := webhook.ProviderEvent{ID: "evt_orphan", Type: webhook.EventPayoutPaid}
	e.Data.PayoutID, e.Data.IdempotencyKey, e.Data.Status = "po_unknown", "key_unknown", "paid"
	b, _ := json.Marshal(e)
	_, err := webhook.NewInbox(h.pool).Store(ctx, e.ID, e.Type, b)
	require.NoError(t, err)
	p := h.inboxProcessor()

	require.Equal(t, "deferred", h.inboxOnce(t, p), "maybe our commit is late")
	_, err = h.pool.Exec(ctx, `UPDATE webhook_inbox SET received_at = now() - interval '2 hours', next_attempt_at = now()`)
	require.NoError(t, err)
	require.Equal(t, "orphaned", h.inboxOnce(t, p))
	status, note := h.inboxRow(t, "evt_orphan")
	require.Equal(t, "IGNORED", status)
	require.Equal(t, "orphan", note)
}

// F29: the processor crashes before its transaction commits. Nothing is
// written; after the lease expires the event is applied exactly once.
func TestInbox_CrashBeforeCommitAppliesOnce_F29(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := waitingPayout(t, h)
	h.fake.SettleNow()
	h.storeEvent(t, wf, webhook.EventPayoutPaid, "")
	p := engine.NewInboxProcessor(h.pool, engine.InboxOptions{
		ID: "inbox-1", Steps: h.allSteps(), LeaseTTL: 50 * time.Millisecond, OrphanAfter: time.Hour,
	})
	failpoint.Enable(engine.FPInboxBeforeCommit, failpoint.CrashOnce())

	require.True(t, crashes(func() { _, _ = p.RunOnce(context.Background()) }))
	require.Equal(t, "AWAITING_CALLBACK", h.stepStatus(t, wf, "payout"), "rolled back")
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, "applied", h.inboxOnce(t, p))
	require.Equal(t, 1, h.payoutPostings(t, wf))
}

func TestInbox_MalformedPayloadIgnored(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	_, err := webhook.NewInbox(h.pool).Store(context.Background(), "evt_bad", "payout.paid", []byte(`{"id":"evt_bad"}`))
	require.NoError(t, err)
	require.Equal(t, "malformed", h.inboxOnce(t, h.inboxProcessor()))
	status, note := h.inboxRow(t, "evt_bad")
	require.Equal(t, "IGNORED", status)
	require.Equal(t, "malformed", note)
}

// Minor fix: a valid event of a type idem does not handle is "unsupported",
// not "malformed".
func TestInbox_UnsupportedEventType(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	_, err := webhook.NewInbox(h.pool).Store(context.Background(), "evt_created", "payout.created",
		[]byte(`{"id":"evt_created","type":"payout.created","data":{"payout_id":"po_1","status":"pending"}}`))
	require.NoError(t, err)
	require.Equal(t, "unsupported", h.inboxOnce(t, h.inboxProcessor()))
	status, note := h.inboxRow(t, "evt_created")
	require.Equal(t, "IGNORED", status)
	require.Equal(t, "unsupported event type payout.created", note)
}

// Minor fix: an event whose amount or currency does not match the payout is
// a provider bug or a misdirected event; it is ignored and logged, never applied.
func TestInbox_AmountMismatchIgnored(t *testing.T) {
	h := asyncHarness(t, time.Hour)
	wf := waitingPayout(t, h)
	h.fake.SettleNow()
	id := h.storeEvent(t, wf, webhook.EventPayoutPaid, "")
	_, err := h.pool.Exec(context.Background(), `UPDATE webhook_inbox SET payload = replace(convert_from(payload, 'UTF8'), '"amount_cents":9825', '"amount_cents":1')::bytea WHERE provider_event_id = $1`, id)
	require.NoError(t, err)

	require.Equal(t, "mismatch", h.inboxOnce(t, h.inboxProcessor()))
	status, note := h.inboxRow(t, id)
	require.Equal(t, "IGNORED", status)
	require.Contains(t, note, "does not match the payout")
	require.Equal(t, "AWAITING_CALLBACK", h.stepStatus(t, wf, "payout"))
}

// Minor fix: when the step moves between the read and the apply without any
// re-check having run, the defer reason says so (not "during re-check").
func TestInbox_ChangeWithoutRecheckSaysSo(t *testing.T) {
	ctx := context.Background()
	h := asyncHarness(t, time.Hour)
	wf := waitingPayout(t, h)
	h.fake.SettleNow()
	_, err := h.pool.Exec(ctx, `UPDATE steps SET run_after = now(), callback_deadline = now() WHERE workflow_id = $1 AND name = 'payout'`, wf)
	require.NoError(t, err)
	poll, ok, err := engine.Claim(ctx, h.pool, "w1", []string{"payout"}, time.Minute) // RUNNING: no re-check
	require.NoError(t, err)
	require.True(t, ok)
	id := h.storeEvent(t, wf, webhook.EventPayoutFailed, "")
	p := h.inboxProcessor()
	engine.SetInboxAfterRead(p, func() {
		require.NoError(t, engine.Complete(ctx, h.pool, poll, engine.Result{Kind: engine.ResultSucceeded, ProviderRef: poll.ProviderRef}, h.payoutStep()))
	})

	require.Equal(t, "deferred", h.inboxOnce(t, p))
	require.Equal(t, "step changed since it was read",
		scalar[string](t, h.pool, `SELECT last_error FROM webhook_inbox WHERE provider_event_id = $1`, id))
}
