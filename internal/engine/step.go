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
var FailpointNames = []string{FPAfterClaim, FPBeforeProviderCall, FPAfterProviderCall, FPBeforeCommit, FPAfterCommit}

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
	Attempt        int32
	PriorStatus    string // PENDING or IN_DOUBT
	Input          workflow.PaymentInput
}

// ResultKind is the step's next status; also the attempt's outcome.
type ResultKind string

// Step outcomes recorded by Complete.
const (
	ResultSucceeded       ResultKind = "SUCCEEDED"
	ResultFailedPermanent ResultKind = "FAILED_PERMANENT"
	ResultInDoubt         ResultKind = "IN_DOUBT"
)

// Result is what a worker learned from executing a step.
type Result struct {
	Kind        ResultKind
	ProviderRef string
	HTTPStatus  int
	Error       string
	RetryDelay  time.Duration // IN_DOUBT only
}

// Step is one kind of workflow step. Execute runs with no transaction open; the
// hooks run inside the completion transaction, so their writes (ledger
// postings, next steps, workflow status) commit atomically with the step's
// state change or not at all.
type Step interface {
	Name() string
	Execute(ctx context.Context, s ClaimedStep) provider.Outcome
	OnSuccess(ctx context.Context, q *db.Queries, s ClaimedStep, providerRef string) error
	OnPermanentFailure(ctx context.Context, q *db.Queries, s ClaimedStep) error
}
