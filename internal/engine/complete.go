package engine

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/store/db"
)

// Complete records r for s in one transaction, fenced by s.LeaseEpoch: if the
// lease was lost (reclaimed or re-claimed by another worker) nothing is written
// and ErrFenced is returned. On success or permanent failure the step's hook
// runs inside the same transaction.
func Complete(ctx context.Context, pool *pgxpool.Pool, s ClaimedStep, r Result, step Step) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin complete: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // also runs when a simulated crash unwinds
	q := db.New(tx)

	rows, err := q.CompleteStep(ctx, db.CompleteStepParams{
		Status:      stepStatus(s, r.Kind),
		ProviderRef: optional(r.ProviderRef),
		LastError:   optional(r.Error),
		DeadReason:  optional(r.DeadReason),
		FailureCode: optional(r.FailureCode),
		RetryMs:     r.RetryDelay.Milliseconds(),
		ID:          s.ID,
		LeaseEpoch:  s.LeaseEpoch,
	})
	if err != nil {
		return fmt.Errorf("complete step: %w", err)
	}
	if rows == 0 {
		return ErrFenced
	}

	if err := applyResult(ctx, q, s, r, step); err != nil {
		return err
	}

	var httpStatus *int32
	if r.HTTPStatus != 0 {
		v := int32(r.HTTPStatus) //nolint:gosec // HTTP status codes fit in int32
		httpStatus = &v
	}
	outcome := r.Kind
	if r.AttemptOutcome != "" {
		outcome = r.AttemptOutcome
	}
	if err := q.FinishStepAttempt(ctx, db.FinishStepAttemptParams{
		Outcome:    string(outcome),
		HttpStatus: httpStatus,
		Error:      optional(r.Error),
		StepID:     s.ID,
		AttemptNo:  s.AttemptNo,
	}); err != nil {
		return fmt.Errorf("finish attempt: %w", err)
	}

	failpoint.Hit(FPBeforeCommit)
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit complete: %w", err)
	}
	failpoint.Hit(FPAfterCommit)
	return nil
}

// stepStatus maps a result to the step's next status. A definite no-effect
// answer only clears doubt that this attempt created: a step whose earlier
// send may have moved money stays IN_DOUBT (sticky doubt).
func stepStatus(s ClaimedStep, kind ResultKind) string {
	if kind != ResultRetryNoEffect {
		return string(kind)
	}
	if s.MoneyMayHaveMoved() {
		return string(ResultInDoubt)
	}
	return "PENDING"
}

// HookError is a failure (or panic) of a step's success or failure hook. The
// completion rolled back; the worker counts it (RecordHookFailure).
type HookError struct{ Err error }

func (e *HookError) Error() string { return e.Err.Error() }
func (e *HookError) Unwrap() error { return e.Err }

// runHook turns a panic in step hook code into an error, so the completion
// transaction rolls back instead of killing the worker (design §6, F25), and
// marks hook failures as *HookError. Simulated crashes are re-panicked.
func runHook(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if failpoint.IsCrash(r) {
				panic(r)
			}
			err = &HookError{Err: fmt.Errorf("hook panic: %v", r)}
		}
	}()
	if err := f(); err != nil {
		return &HookError{Err: err}
	}
	return nil
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// applyResult runs what follows a step's compare-and-swap, inside the same
// transaction: the step's success or failure hook (ledger posting, saga move)
// or the workflow's parking. Worker completions and provider webhooks (the
// inbox) share it, so both paths move money and the saga identically.
func applyResult(ctx context.Context, q *db.Queries, s ClaimedStep, r Result, step Step) error {
	switch r.Kind {
	case ResultSucceeded:
		if err := runHook(func() error { return step.OnSuccess(ctx, q, s, r.ProviderRef) }); err != nil {
			return fmt.Errorf("%s on success: %w", s.Name, err)
		}
	case ResultFailedPermanent:
		if err := runHook(func() error { return step.OnPermanentFailure(ctx, q, s) }); err != nil {
			return fmt.Errorf("%s on permanent failure: %w", s.Name, err)
		}
	case ResultInDoubt, ResultRetryNoEffect, ResultAwaitingCallback:
		// No hook: nothing is known to have happened (or not yet), so nothing
		// else may change.
	case ResultDead:
		if err := transitionWorkflow(ctx, q, s.WorkflowID, []string{wfRunning, wfCompensating}, wfNeedsAttention); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown result kind %q", r.Kind)
	}
	return nil
}
