package engine

import (
	"time"

	"github.com/AdamDiouri/idem/internal/provider"
)

// decide turns a provider outcome and the step's history into the step's next
// state (design §9.3). It is pure so every rule is table-tested.
//
//   - Success / Permanent are final, except that a pivot step refused after an
//     ambiguous attempt is DEAD (parked for a human), never failed.
//   - Ambiguous retries after backoff (409 in-flight: soon); when attempts are
//     exhausted the step is DEAD and money may have moved.
//   - NoEffect retries after max(backoff, Retry-After capped at b.Cap); when
//     exhausted it is DEAD, and money may have moved only if an earlier
//     attempt left the step IN_DOUBT (sticky ambiguity).
func decide(s ClaimedStep, out provider.Outcome, b Backoff, inFlightDelay time.Duration, pivot bool) Result {
	r := Result{ProviderRef: out.ProviderRef, HTTPStatus: out.HTTPStatus}
	if out.Err != nil {
		r.Error = out.Err.Error()
	}
	attemptsLeft := s.Attempt < s.MaxAttempts

	switch out.Class {
	case provider.Success:
		r.Kind = ResultSucceeded
	case provider.Permanent:
		r.Kind = ResultFailedPermanent
		if pivot && s.PriorStatus == string(ResultInDoubt) {
			// An earlier attempt may have succeeded even though this one is
			// refused (a provider that does not replay). Compensating on that
			// guess could pay the merchant AND refund the customer.
			r.Kind, r.DeadReason = ResultDead, DeadPermanentAfterInDoubt
		}
	case provider.NoEffect:
		if !attemptsLeft {
			r.Kind, r.DeadReason = ResultDead, DeadAttemptsExhaustedNoEffect
			if s.PriorStatus == string(ResultInDoubt) {
				r.DeadReason = DeadAttemptsExhaustedInDoubt
			}
			break
		}
		r.Kind = ResultRetryNoEffect
		r.RetryDelay = max(b.Delay(s.Attempt), min(out.RetryAfter, b.Cap))
	default: // provider.Ambiguous, and anything unknown: assume the effect may have happened (D5)
		if !attemptsLeft {
			r.Kind, r.DeadReason = ResultDead, DeadAttemptsExhaustedInDoubt
			break
		}
		r.Kind = ResultInDoubt
		r.RetryDelay = b.Delay(s.Attempt)
		if out.InFlight {
			r.RetryDelay = inFlightDelay
		}
	}
	return r
}
