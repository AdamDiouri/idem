// Package engine executes workflow steps: claim under a lease, call the
// provider with no transaction open, then complete with a fenced
// compare-and-swap (design §9.1). Crashed or paused workers are recovered by
// reclaiming expired leases.
package engine

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/workflow"
)

// Failpoint names at every boundary of a step's execution (design §14.2).
const (
	FPAfterClaim         = "after_claim"
	FPBeforeProviderCall = "before_provider_call"
	FPAfterProviderCall  = "after_provider_call"
	FPBeforeCommit       = "before_commit"
	FPAfterCommit        = "after_commit"
)

// FailpointNames lists every failpoint the engine hits, for config validation.
var FailpointNames = []string{FPAfterClaim, FPBeforeProviderCall, FPAfterProviderCall, FPBeforeCommit, FPAfterCommit,
	FPInboxBeforeCommit}

// ErrFenced means the worker lost its lease before completing: another owner
// has the step now. The caller must discard its result and write nothing.
var ErrFenced = errors.New("lease lost: completion fenced")

// ClaimedStep is a step owned by this worker until its lease expires.
type ClaimedStep struct {
	ID             uuid.UUID
	WorkflowID     uuid.UUID
	Name           string
	IdempotencyKey string // stored at step creation; never recomputed (D6)
	LeaseEpoch     int64  // fencing token
	Attempt        int32  // send attempts so far, this one included (the retry budget)
	AttemptNo      int32  // this attempt's number in step_attempts: sends + polls
	PollCount      int32  // polls so far, this one included
	PollsSinceSend int32  // polls since the latest send, this one included
	// WaitedFor is how long since the provider accepted the request (the latest
	// send ended): the main poll budget (PollLimits.MaxWait).
	WaitedFor   time.Duration
	PriorStatus string // PENDING, IN_DOUBT, or AWAITING_CALLBACK for a poll
	MaxAttempts int32
	ProviderRef string // set once the provider accepted or performed the request
	// SendDoubt: an earlier SEND attempt may have moved money (ambiguous,
	// abandoned or exhausted). The pivot rule reads it; polls never set it.
	SendDoubt       bool
	KeyAge          time.Duration // since the first attempt, by the DB clock
	RefundChargeRef string        // refunds only: the charge being reversed
	Input           workflow.PaymentInput
}

// ResultKind is the step's next status; also the attempt's outcome.
type ResultKind string

// Step outcomes recorded by Complete.
const (
	ResultSucceeded       ResultKind = "SUCCEEDED"
	ResultFailedPermanent ResultKind = "FAILED_PERMANENT"
	ResultInDoubt         ResultKind = "IN_DOUBT"
	// ResultRetryNoEffect: this attempt provably had no effect; retry later.
	// The step returns to PENDING, or stays IN_DOUBT if it ever was (sticky).
	ResultRetryNoEffect ResultKind = "RETRY_NO_EFFECT"
	// ResultDead: give up; the workflow needs a human (dead-letter state).
	ResultDead ResultKind = "DEAD"
	// ResultAwaitingCallback: the provider accepted the request and will settle
	// it later; poll at RetryDelay (the callback deadline). Async payouts.
	ResultAwaitingCallback ResultKind = "AWAITING_CALLBACK"
)

// Dead reasons, stored in steps.dead_reason.
const (
	DeadAttemptsExhaustedNoEffect = "attempts_exhausted_no_effect"
	DeadAttemptsExhaustedInDoubt  = "attempts_exhausted_in_doubt"
	DeadKeyExpiredUnknown         = "key_expired_unknown"
	// DeadPermanentAfterInDoubt: a pivot step was refused after an earlier
	// attempt was ambiguous; that earlier attempt may have moved the money.
	DeadPermanentAfterInDoubt = "permanent_after_in_doubt"
	// DeadCallbackUnresolved: an accepted request was still unsettled (or
	// unpollable) after every allowed poll. It may have moved money.
	DeadCallbackUnresolved = "callback_unresolved"
	// DeadHookFailed: the provider reported success or a refusal, but the
	// step's hook (ledger posting, saga move) kept failing. The provider DID
	// act; fix the bug, then retry or confirm the step.
	DeadHookFailed = "hook_failed"
	// DeadHookFailedNoEffect: the same, but the provider REFUSED the request
	// and no earlier send was in doubt: nothing moved, nothing is posted.
	DeadHookFailedNoEffect = "hook_failed_no_effect"
)

// MoneyMayHaveMoved reports whether a dead reason leaves the external effect unknown.
func MoneyMayHaveMoved(deadReason string) bool {
	return deadReason != DeadAttemptsExhaustedNoEffect && deadReason != DeadHookFailedNoEffect
}

// Result is what a worker learned from executing a step.
type Result struct {
	Kind        ResultKind
	ProviderRef string
	HTTPStatus  int
	Error       string
	RetryDelay  time.Duration // IN_DOUBT and RETRY_NO_EFFECT
	DeadReason  string        // DEAD only
	// AttemptOutcome, when set, is what this attempt itself learned, recorded
	// instead of Kind on its step_attempts row: a provably no-effect attempt
	// that exhausted the budget is DEAD for the step but not doubt in the
	// history the pivot rule reads.
	AttemptOutcome ResultKind
}

// Step is one kind of workflow step. Execute runs with no transaction open; the
// hooks run inside the completion transaction, so their writes (ledger
// postings, next steps, workflow status) commit atomically with the step's
// state change or not at all.
type Step interface {
	Name() string
	// Pivot reports whether this step is the saga's point of no return (the
	// payout). A pivot that is refused after an ambiguous attempt is parked for a
	// human instead of failing, so no compensation runs on a guess.
	Pivot() bool
	Execute(ctx context.Context, s ClaimedStep) provider.Outcome
	// Lookup asks the provider what happened to the request sent with
	// s.IdempotencyKey, without sending it again. found is false when the
	// provider has no record (Class Permanent) or the lookup failed (Ambiguous).
	Lookup(ctx context.Context, s ClaimedStep) (out provider.Outcome, found bool)
	OnSuccess(ctx context.Context, q *db.Queries, s ClaimedStep, providerRef string) error
	OnPermanentFailure(ctx context.Context, q *db.Queries, s ClaimedStep) error
}

// Poller is a Step whose provider accepts requests asynchronously (the payout).
// Poll asks for the current state of the request identified by s.ProviderRef;
// it sends nothing, so its answer never creates doubt (design D17).
type Poller interface {
	Poll(ctx context.Context, s ClaimedStep) provider.Outcome
}
