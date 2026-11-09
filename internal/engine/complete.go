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
		Status:      string(r.Kind),
		ProviderRef: optional(r.ProviderRef),
		LastError:   optional(r.Error),
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

	switch r.Kind {
	case ResultSucceeded:
		if err := step.OnSuccess(ctx, q, s, r.ProviderRef); err != nil {
			return fmt.Errorf("%s on success: %w", s.Name, err)
		}
	case ResultFailedPermanent:
		if err := step.OnPermanentFailure(ctx, q, s); err != nil {
			return fmt.Errorf("%s on permanent failure: %w", s.Name, err)
		}
	case ResultInDoubt:
		// No hook: we do not know what happened, so nothing else may change.
	default:
		return fmt.Errorf("unknown result kind %q", r.Kind)
	}

	var httpStatus *int32
	if r.HTTPStatus != 0 {
		v := int32(r.HTTPStatus) //nolint:gosec // HTTP status codes fit in int32
		httpStatus = &v
	}
	if err := q.FinishStepAttempt(ctx, db.FinishStepAttemptParams{
		Outcome:    string(r.Kind),
		HttpStatus: httpStatus,
		Error:      optional(r.Error),
		StepID:     s.ID,
		AttemptNo:  s.Attempt,
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

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
