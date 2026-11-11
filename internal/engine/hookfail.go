package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/store/db"
)

// DefaultHookMaxFailures is used when WorkerOptions.HookMaxFailures is zero.
const DefaultHookMaxFailures = 3

// RecordHookFailure counts a failed hook for s, which the caller still holds
// (fenced on s.LeaseEpoch: ErrFenced if not); failed is the result whose hook
// failed. The provider already answered, so the step does not wait for its
// lease to expire:
//   - below max: a poll goes back to waiting. A send the provider performed
//     becomes IN_DOUBT (sticky: money moved, nothing is posted yet); a send
//     the provider refused moved nothing and goes back to PENDING, so the
//     failed hook does not turn into doubt. Each retries after delay and
//     replays the same idempotency key.
//   - at max: the step is DEAD with hook_failed (or hook_failed_no_effect for
//     a refusal with no earlier doubt) and its workflow needs attention (dead
//     is true). Retrying a bug forever helps no one.
func RecordHookFailure(ctx context.Context, pool *pgxpool.Pool, s ClaimedStep, failed Result, hookErr error, maxFailures int, delay time.Duration) (dead bool, err error) {
	if maxFailures <= 0 {
		maxFailures = DefaultHookMaxFailures
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin record hook failure: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)

	failures, err := q.IncrementHookFailures(ctx, db.IncrementHookFailuresParams{ID: s.ID, LeaseEpoch: s.LeaseEpoch})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrFenced
	}
	if err != nil {
		return false, fmt.Errorf("count hook failure: %w", err)
	}

	// A send the provider refused moved nothing; unless an earlier send was in
	// doubt, its attempt must not read as doubt either (the pivot rule acts on
	// that history), below the limit or at it.
	poll := s.PriorStatus == string(ResultAwaitingCallback)
	refused := failed.Kind == ResultFailedPermanent && !poll
	noEffect := refused && !s.MoneyMayHaveMoved()

	r := Result{Kind: ResultInDoubt, RetryDelay: delay, Error: hookErr.Error()}
	switch {
	case int(failures) >= maxFailures && noEffect:
		r = Result{Kind: ResultDead, DeadReason: DeadHookFailedNoEffect, Error: hookErr.Error(),
			AttemptOutcome: ResultRetryNoEffect}
	case int(failures) >= maxFailures:
		r = Result{Kind: ResultDead, DeadReason: DeadHookFailed, Error: hookErr.Error()}
	case poll:
		r.Kind = ResultAwaitingCallback
	case refused:
		r.Kind = ResultRetryNoEffect
	}
	// Keep what the provider said: the operator confirms with this reference.
	r.ProviderRef, r.HTTPStatus = failed.ProviderRef, failed.HTTPStatus
	if _, err := q.CompleteStep(ctx, db.CompleteStepParams{
		ProviderRef: optional(r.ProviderRef),
		Status:      stepStatus(s, r.Kind), LastError: optional(r.Error), DeadReason: optional(r.DeadReason),
		RetryMs: r.RetryDelay.Milliseconds(), ID: s.ID, LeaseEpoch: s.LeaseEpoch,
	}); err != nil {
		return false, fmt.Errorf("record hook failure: %w", err)
	}
	if err := applyResult(ctx, q, s, r, nil); err != nil { // DEAD: the workflow needs attention
		return false, err
	}
	outcome := r.Kind
	if r.AttemptOutcome != "" {
		outcome = r.AttemptOutcome
	}
	var httpStatus *int32
	if r.HTTPStatus != 0 {
		v := int32(r.HTTPStatus) //nolint:gosec // HTTP status codes fit in int32
		httpStatus = &v
	}
	if err := q.FinishStepAttempt(ctx, db.FinishStepAttemptParams{
		HttpStatus: httpStatus,
		Outcome:    string(outcome), Error: optional(r.Error), StepID: s.ID, AttemptNo: s.AttemptNo,
	}); err != nil {
		return false, fmt.Errorf("finish attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit hook failure: %w", err)
	}
	return r.Kind == ResultDead, nil
}

// transientSQLStates are errors Postgres raises under contention: retrying is
// the cure, so they say nothing about the hook itself.
var transientSQLStates = map[string]bool{
	"40P01": true, // deadlock_detected
	"40001": true, // serialization_failure
	"55P03": true, // lock_not_available
	"57014": true, // query_canceled (statement or lock timeout)
}

// countableHookError reports whether err is a hook's own failure (a bug, a
// constraint the posting broke) rather than the infrastructure failing under
// it (a deadline, a lost connection): only the former is counted; the latter
// takes the lease-expiry path as before.
func countableHookError(err error) bool {
	var hookErr *HookError
	if !errors.As(err, &hookErr) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && transientSQLStates[pgErr.Code] {
		return false
	}
	return !pgconn.SafeToRetry(err) && !pgconn.Timeout(err)
}
