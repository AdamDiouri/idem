package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/metrics"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/webhook"
)

// FPInboxBeforeCommit fires just before an inbox event's apply commits.
const FPInboxBeforeCommit = "inbox_before_commit"

// errInboxFenced: the processor lost its inbox lease; it wrote nothing.
var errInboxFenced = errors.New("inbox lease lost: result discarded")

// Inbox outcomes (idem_webhooks_applied_total{outcome}).
const (
	inboxApplied     = "applied"
	inboxNoop        = "noop"
	inboxDeferred    = "deferred"
	inboxOrphaned    = "orphaned"
	inboxStale       = "stale"
	inboxEvidence    = "evidence"
	inboxConflict    = "conflict"
	inboxMalformed   = "malformed"
	inboxUnsupported = "unsupported"
	inboxMismatch    = "mismatch"
	inboxFenced      = "fenced"
)

// InboxOptions configures an InboxProcessor.
type InboxOptions struct {
	ID           string
	Steps        []Step
	LeaseTTL     time.Duration
	PollInterval time.Duration
	// OrphanAfter: an event that still matches no step this long after it was
	// received is IGNORED as an orphan (default 1h).
	OrphanAfter time.Duration
	// Retry spaces deferred events (zero: retry at once).
	Retry   Backoff
	Metrics *metrics.Metrics
	Logger  *slog.Logger

	afterRead func() // test seam: runs after the read phase (snapshot, re-check), before the apply
}

// InboxResult is what one RunOnce did.
type InboxResult struct {
	Claimed bool
	EventID string
	Outcome string
}

// InboxProcessor applies provider webhooks from the inbox (M2 design §4.4).
// A webhook is a prompt to ask, never the truth itself: events arrive late,
// duplicated and out of order, so before acting the processor re-checks the
// payout with the provider (GET) and acts on what the provider says now. For
// a waiting step that result is applied through the step's own CAS path and
// hooks; news that disagrees with a decided result, or concerns a parked step,
// becomes stale, evidence (D18) or a conflict (D16). Terminal states never flip.
type InboxProcessor struct {
	pool   *pgxpool.Pool
	opts   InboxOptions
	steps  map[string]Step
	logger *slog.Logger
}

// NewInboxProcessor returns a processor for opts.Steps.
func NewInboxProcessor(pool *pgxpool.Pool, opts InboxOptions) *InboxProcessor {
	if opts.OrphanAfter <= 0 {
		opts.OrphanAfter = time.Hour
	}
	p := &InboxProcessor{pool: pool, opts: opts, steps: map[string]Step{}, logger: opts.Logger}
	if p.logger == nil {
		p.logger = slog.Default()
	}
	for _, s := range opts.Steps {
		p.steps[s.Name()] = s
	}
	return p
}

// eventStep is the step an event is about, with what deciding needs.
type eventStep struct {
	db.StepForEventRow
}

// plan is what to do with an event, given the step's state.
type plan int

const (
	planDefer plan = iota
	planApply
	planNoop
	planRecheck
)

func classify(st eventStep, paid bool) plan {
	switch st.Status {
	case string(ResultAwaitingCallback):
		return planApply
	case string(ResultSucceeded):
		if paid {
			return planNoop
		}
		return planRecheck
	case string(ResultFailedPermanent):
		if !paid {
			return planNoop
		}
		return planRecheck
	case string(ResultDead):
		return planRecheck
	default: // PENDING, RUNNING, IN_DOUBT: our own commit (or a poll) is in flight
		return planDefer
	}
}

// Run processes events until ctx is cancelled.
func (p *InboxProcessor) Run(ctx context.Context) error {
	p.logger.Info("inbox processor started", "worker_id", p.opts.ID)
	for ctx.Err() == nil {
		res, err := p.RunOnce(ctx)
		if err != nil {
			p.logger.Error("inbox event", "event_id", res.EventID, "error", err)
		}
		if err != nil || !res.Claimed {
			select {
			case <-ctx.Done():
			case <-time.After(p.opts.PollInterval):
			}
		}
	}
	return nil
}

// RunOnce claims one due event and processes it.
func (p *InboxProcessor) RunOnce(ctx context.Context) (InboxResult, error) {
	row, err := db.New(p.pool).ClaimInboxEvent(ctx, db.ClaimInboxEventParams{Owner: p.opts.ID, LeaseMs: p.opts.LeaseTTL.Milliseconds()})
	if errors.Is(err, pgx.ErrNoRows) {
		return InboxResult{}, nil
	}
	if err != nil {
		return InboxResult{}, fmt.Errorf("claim inbox event: %w", err)
	}
	res := InboxResult{Claimed: true, EventID: row.ProviderEventID}
	log := p.logger.With("event_id", row.ProviderEventID, "event_type", row.Type, "attempt", row.Attempts, "worker_id", p.opts.ID)
	res.Outcome, err = p.process(ctx, row, log)
	if errors.Is(err, errInboxFenced) {
		res.Outcome, err = inboxFenced, nil
		log.Warn("inbox lease lost; result discarded")
	}
	p.opts.Metrics.ObserveInbox(res.Outcome)
	return res, err
}

func (p *InboxProcessor) process(ctx context.Context, row db.ClaimInboxEventRow, log *slog.Logger) (string, error) {
	var ev webhook.ProviderEvent
	paid, verdict := parseEvent(row.Payload, &ev)
	switch verdict {
	case inboxMalformed:
		return inboxMalformed, p.finish(ctx, db.New(p.pool), row, "IGNORED", "malformed", nil)
	case inboxUnsupported:
		return inboxUnsupported, p.finish(ctx, db.New(p.pool), row, "IGNORED", "unsupported event type "+ev.Type, nil)
	}

	snap, err := db.New(p.pool).StepForEvent(ctx, db.StepForEventParams{PayoutID: ev.Data.PayoutID, IdempotencyKey: ev.Data.IdempotencyKey})
	if errors.Is(err, pgx.ErrNoRows) {
		if time.Duration(row.AgeMs)*time.Millisecond >= p.opts.OrphanAfter {
			log.Warn("provider event matches no step; ignored as an orphan", "payout_id", ev.Data.PayoutID)
			return inboxOrphaned, p.finish(ctx, db.New(p.pool), row, "IGNORED", "orphan", nil)
		}
		return inboxDeferred, p.deferEvent(ctx, db.New(p.pool), row, p.opts.Retry.Delay(row.Attempts), "no matching step yet")
	}
	if err != nil {
		return "", fmt.Errorf("find step for event: %w", err)
	}
	before := eventStep{snap}
	if note := before.mismatch(ev); note != "" {
		log.Warn("provider event does not match its payout; ignored", "detail", note, "step_id", before.ID)
		return inboxMismatch, p.finish(ctx, db.New(p.pool), row, "IGNORED", note, &before.ID)
	}

	// A re-check asks the provider for the truth, outside any transaction.
	var recheck *provider.Outcome
	if pl := classify(before, paid); pl == planApply || pl == planRecheck {
		out, ok := p.recheck(ctx, before, ev.Data.PayoutID)
		if !ok {
			return inboxDeferred, p.deferEvent(ctx, db.New(p.pool), row, p.opts.Retry.Delay(row.Attempts),
				"re-check with the provider: "+errString(out.Err))
		}
		recheck = &out
	}
	if p.opts.afterRead != nil {
		p.opts.afterRead()
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin inbox apply: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	locked, err := q.LockStepForEvent(ctx, snap.ID)
	if err != nil {
		return "", fmt.Errorf("lock step for event: %w", err)
	}
	now := eventStep{db.StepForEventRow(locked)}

	outcome := ""
	switch classify(now, paid) {
	case planDefer:
		outcome, err = inboxDeferred, p.deferEvent(ctx, q, row, p.opts.Retry.Delay(row.Attempts), "step "+now.Status+": our own commit or a poll is in flight")
	case planNoop:
		outcome, err = inboxNoop, p.finish(ctx, q, row, "PROCESSED", "duplicate", &now.ID)
	case planApply:
		if recheck == nil || !sameState(before, now) {
			outcome, err = inboxDeferred, p.deferEvent(ctx, q, row, p.opts.Retry.Base, "step changed since it was read")
			break
		}
		// Apply what the provider says now, not what the event says: a stale
		// or reordered "failed" must never refund a paid payout.
		outcome, err = inboxApplied, p.apply(ctx, q, row, now, *recheck)
	case planRecheck:
		if recheck == nil {
			// The step moved into a decided state after we read it, so no
			// re-check ran: read again.
			outcome, err = inboxDeferred, p.deferEvent(ctx, q, row, p.opts.Retry.Base, "step changed since it was read")
			break
		}
		if !sameState(before, now) {
			// The step moved since we looked: never act on old state (review focus 5).
			outcome, err = inboxDeferred, p.deferEvent(ctx, q, row, p.opts.Retry.Base, "state changed during re-check")
			break
		}
		outcome, err = p.resolveNews(ctx, q, row, now, *recheck, log)
	}
	if err != nil {
		return outcome, err
	}
	failpoint.Hit(FPInboxBeforeCommit)
	if err := tx.Commit(ctx); err != nil {
		return outcome, fmt.Errorf("commit inbox event: %w", err)
	}
	if outcome == inboxConflict {
		log.Error("provider contradicts idem: money may have moved both ways", "workflow_id", now.WorkflowID, "step_id", now.ID)
	} else {
		log.Info("provider event processed", "outcome", outcome, "workflow_id", now.WorkflowID, "step_id", now.ID)
	}
	return outcome, nil
}

// apply records the provider's result on a waiting step, exactly as a poll
// with that answer would: CAS, attempt (kind webhook), hooks.
func (p *InboxProcessor) apply(ctx context.Context, q *db.Queries, row db.ClaimInboxEventRow, st eventStep, out provider.Outcome) error {
	step, ok := p.steps[st.Name]
	if !ok {
		return fmt.Errorf("no step %q for event %s", st.Name, row.ProviderEventID)
	}
	s, err := st.claimed()
	if err != nil {
		return err
	}
	r := decidePoll(s, out, Backoff{}, PollLimits{}, step.Pivot())

	n, err := q.ApplyCallbackToStep(ctx, db.ApplyCallbackToStepParams{
		Status: string(r.Kind), ProviderRef: optional(r.ProviderRef), DeadReason: optional(r.DeadReason),
		LastError: optional(r.Error), ID: s.ID,
	})
	if err != nil {
		return fmt.Errorf("apply callback: %w", err)
	}
	if n == 0 {
		return errInboxFenced // impossible under the row lock; never write on a surprise
	}
	if err := q.InsertWebhookAttempt(ctx, db.InsertWebhookAttemptParams{
		StepID: s.ID, AttemptNo: st.AttemptCount + st.PollCount + 1, LeaseEpoch: st.LeaseEpoch + 1,
		WorkerID: p.opts.ID, Outcome: optional(string(r.Kind)), Error: optional(r.Error),
	}); err != nil {
		return fmt.Errorf("record webhook attempt: %w", err)
	}
	if err := applyResult(ctx, q, s, r, step); err != nil {
		return err
	}
	return p.finish(ctx, q, row, "PROCESSED", "applied", &st.ID)
}

// resolveNews handles provider news that disagrees with what idem decided, or
// concerns a parked step, using the provider's re-checked current state.
func (p *InboxProcessor) resolveNews(ctx context.Context, q *db.Queries, row db.ClaimInboxEventRow, st eventStep, re provider.Outcome, _ *slog.Logger) (string, error) {
	providerPaid := re.Class == provider.Success
	says := "provider says paid " + derefOr(st.ProviderRef, re.ProviderRef)
	if !providerPaid {
		says = "provider says failed: " + strings.TrimPrefix(errString(re.Err), "payout failed: ")
	}

	parked := st.Status == string(ResultDead) && st.WorkflowStatus == wfNeedsAttention && st.Newest
	if parked {
		// D18: a parked step leaves NEEDS_ATTENTION only through a human.
		return inboxEvidence, p.finish(ctx, q, row, "EVIDENCE", says, &st.ID)
	}

	// What idem recorded: paid only if the step succeeded. A failed step, or
	// a dead one an operator already resolved (compensate, fail, new
	// generation), means idem believes the merchant was not paid this way.
	idemPaid := st.Status == string(ResultSucceeded)
	if providerPaid == idemPaid {
		if st.Status == string(ResultDead) {
			return inboxNoop, p.finish(ctx, q, row, "PROCESSED", "agrees with operator", &st.ID)
		}
		return inboxStale, p.finish(ctx, q, row, "IGNORED", "stale", &st.ID)
	}
	recorded := "not paid"
	if idemPaid {
		recorded = "paid"
	}
	return inboxConflict, p.finish(ctx, q, row, "CONFLICT", says+", idem recorded "+recorded, &st.ID)
}

// recheck asks the provider for the payout's current state. It uses the step's
// own reference, or the event's when the step never recorded one (every send
// timed out though the provider accepted it); GetPayout checks the answer's
// ID and amount against the step. ok is false unless the answer is final.
func (p *InboxProcessor) recheck(ctx context.Context, st eventStep, eventRef string) (provider.Outcome, bool) {
	step, ok := p.steps[st.Name]
	poller, canPoll := step.(Poller)
	if !ok || !canPoll {
		return provider.Outcome{Class: provider.Ambiguous, Err: errors.New("step cannot be polled")}, false
	}
	s, err := st.claimed()
	if err != nil {
		return provider.Outcome{Class: provider.Ambiguous, Err: err}, false
	}
	if s.ProviderRef == "" {
		s.ProviderRef = eventRef
	}
	if s.ProviderRef == "" {
		return provider.Outcome{Class: provider.Ambiguous, Err: errors.New("no payout reference to re-check")}, false
	}
	out := poller.Poll(ctx, s)
	return out, out.Class == provider.Success || out.Class == provider.Permanent
}

func (p *InboxProcessor) finish(ctx context.Context, q *db.Queries, row db.ClaimInboxEventRow, status, note string, stepID *uuid.UUID) error {
	n, err := q.FinishInboxEvent(ctx, db.FinishInboxEventParams{
		Status: status, Note: optional(note), StepID: stepID, ProviderEventID: row.ProviderEventID, LeaseEpoch: row.LeaseEpoch,
	})
	if err != nil {
		return fmt.Errorf("finish inbox event: %w", err)
	}
	if n == 0 {
		return errInboxFenced
	}
	return nil
}

func (p *InboxProcessor) deferEvent(ctx context.Context, q *db.Queries, row db.ClaimInboxEventRow, delay time.Duration, why string) error {
	n, err := q.DeferInboxEvent(ctx, db.DeferInboxEventParams{
		DelayMs: delay.Milliseconds(), LastError: optional(why), ProviderEventID: row.ProviderEventID, LeaseEpoch: row.LeaseEpoch,
	})
	if err != nil {
		return fmt.Errorf("defer inbox event: %w", err)
	}
	if n == 0 {
		return errInboxFenced
	}
	return nil
}

// parseEvent decodes a payout event; paid reports its result. verdict is ""
// for a usable event, inboxUnsupported for a well-formed event of a type idem
// does not act on, and inboxMalformed otherwise.
func parseEvent(payload []byte, ev *webhook.ProviderEvent) (paid bool, verdict string) {
	if json.Unmarshal(payload, ev) != nil || ev.Type == "" || (ev.Data.PayoutID == "" && ev.Data.IdempotencyKey == "") {
		return false, inboxMalformed
	}
	switch ev.Type {
	case webhook.EventPayoutPaid:
		if ev.Data.Status == "paid" {
			return true, ""
		}
	case webhook.EventPayoutFailed:
		if ev.Data.Status == "failed" {
			return false, ""
		}
	default:
		return false, inboxUnsupported
	}
	return false, inboxMalformed // a paid/failed event whose status says otherwise
}

// mismatch describes how the event's amount or currency differs from the
// payout it matched (a provider bug or a misdirected event), or "".
func (st eventStep) mismatch(ev webhook.ProviderEvent) string {
	if st.Name != stepPayout {
		return ""
	}
	s, err := st.claimed()
	if err != nil {
		return ""
	}
	want := payoutRequest(s)
	if ev.Data.AmountCents == want.AmountCents && (ev.Data.Currency == "" || ev.Data.Currency == want.Currency) {
		return ""
	}
	return fmt.Sprintf("event says %d %s, which does not match the payout (%d %s)",
		ev.Data.AmountCents, ev.Data.Currency, want.AmountCents, want.Currency)
}

// sameState reports whether the step is as it was when the re-check began.
func sameState(a, b eventStep) bool {
	return a.Status == b.Status && derefOr(a.DeadReason, "") == derefOr(b.DeadReason, "") &&
		a.WorkflowStatus == b.WorkflowStatus && a.Newest == b.Newest && a.LeaseEpoch == b.LeaseEpoch
}

// claimed builds the ClaimedStep the hooks and the poller expect.
func (st eventStep) claimed() (ClaimedStep, error) {
	s := ClaimedStep{
		ID: st.ID, WorkflowID: st.WorkflowID, Name: st.Name, IdempotencyKey: st.IdempotencyKey,
		LeaseEpoch: st.LeaseEpoch + 1, Attempt: st.AttemptCount, MaxAttempts: st.MaxAttempts,
		PriorStatus: st.Status, ProviderRef: derefOr(st.ProviderRef, ""), RefundChargeRef: derefOr(st.RefundChargeRef, ""),
		SendDoubt: st.SendDoubt,
	}
	if err := json.Unmarshal(st.Input, &s.Input); err != nil {
		return ClaimedStep{}, fmt.Errorf("decode workflow input: %w", err)
	}
	return s, nil
}

func derefOr(s *string, def string) string {
	if s == nil || *s == "" {
		return def
	}
	return *s
}
