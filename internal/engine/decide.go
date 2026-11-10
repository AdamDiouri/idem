package engine

import (
	"time"

	"github.com/AdamDiouri/idem/internal/provider"
)

// decide turns a provider outcome and the step's history into the step's next
// state (design §9.3). It is pure so every rule is table-tested.
//
//   - Success / Permanent are final, except that a pivot step refused after an
//     ambiguous send attempt (the previous status, or the send history) is
//     DEAD (parked for a human), never failed.
//   - Accepted (async) waits in AWAITING_CALLBACK until callbackDeadline, when
//     it is polled (decidePoll).
//   - Ambiguous retries after backoff (409 in-flight: soon); when attempts are
//     exhausted the step is DEAD and money may have moved.
//   - NoEffect retries after max(backoff, Retry-After capped at b.Cap); when
//     exhausted it is DEAD, and money may have moved only if an earlier
//     attempt left the step IN_DOUBT (sticky ambiguity).
func decide(s ClaimedStep, out provider.Outcome, b Backoff, inFlightDelay, callbackDeadline time.Duration, pivot bool) Result {
	r := Result{ProviderRef: out.ProviderRef, HTTPStatus: out.HTTPStatus}
	if out.Err != nil {
		r.Error = out.Err.Error()
	}
	attemptsLeft := s.Attempt < s.MaxAttempts

	switch out.Class {
	case provider.Success:
		r.Kind = ResultSucceeded
	case provider.Accepted:
		r.Kind, r.RetryDelay = ResultAwaitingCallback, callbackDeadline
	case provider.Permanent:
		r.Kind = ResultFailedPermanent
		if pivot && (s.PriorStatus == string(ResultInDoubt) || s.SendDoubt) {
			// An earlier attempt may have succeeded even though this one is
			// refused (a provider that does not replay). Compensating on that
			// guess could pay the merchant AND refund the customer.
			r.Kind, r.DeadReason = ResultDead, DeadPermanentAfterInDoubt
		}
	case provider.NoEffect:
		if !attemptsLeft {
			r.Kind, r.DeadReason = ResultDead, DeadAttemptsExhaustedNoEffect
			r.AttemptOutcome = ResultRetryNoEffect // this attempt moved nothing
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
			// The provider is still processing this key: retry soon (from
			// inFlightDelay), but back off with equal jitter so a long-busy
			// provider can't burn the attempt budget in a second.
			r.RetryDelay = Backoff{Base: inFlightDelay, Cap: b.Cap, Rand: b.Rand}.EqualJitter(s.Attempt)
		}
	}
	return r
}

// decidePoll turns the answer to a status poll of an accepted request into the
// step's next state (design §4.5). A poll sends nothing, so it never creates
// doubt: a failed poll just waits again.
//
//   - paid → SUCCEEDED; failed → FAILED_PERMANENT, or DEAD under the pivot
//     rule when an earlier send was ambiguous.
//   - pending → wait again, backing off up to pb.Cap (real payouts settle in
//     hours or days); a failed poll → wait pb.Base.
//   - either, once the step has waited lim.MaxWait since acceptance (or used
//     lim.MaxPolls polls since the latest send, when set) → DEAD
//     callback_unresolved (money may have moved; a human checks with the
//     provider). An operator's retry re-sends, which starts a fresh budget.
//   - The pivot rule applies to a failed payout after an ambiguous send, or
//     when the poll resolves a step that was itself IN_DOUBT.
//
// PollLimits bound how long an accepted request is polled. Zero means no limit.
type PollLimits struct {
	MaxPolls int32         // polls since the latest send (optional)
	MaxWait  time.Duration // since the provider accepted the request
}

func (l PollLimits) exhausted(s ClaimedStep) bool {
	return (l.MaxPolls > 0 && s.PollsSinceSend >= l.MaxPolls) || (l.MaxWait > 0 && s.WaitedFor >= l.MaxWait)
}

func decidePoll(s ClaimedStep, out provider.Outcome, pb Backoff, lim PollLimits, pivot bool) Result {
	r := Result{ProviderRef: s.ProviderRef, HTTPStatus: out.HTTPStatus}
	if out.Err != nil {
		r.Error = out.Err.Error()
	}
	switch out.Class {
	case provider.Success:
		r.Kind = ResultSucceeded
		return r
	case provider.Permanent:
		r.Kind = ResultFailedPermanent
		if pivot && (s.SendDoubt || s.PriorStatus == string(ResultInDoubt)) {
			r.Kind, r.DeadReason = ResultDead, DeadPermanentAfterInDoubt
		}
		return r
	}
	if lim.exhausted(s) {
		r.Kind, r.DeadReason = ResultDead, DeadCallbackUnresolved
		return r
	}
	r.Kind = ResultAwaitingCallback
	if out.Class == provider.Pending {
		r.RetryDelay = min(pb.Cap, pb.Base+pb.Delay(s.PollsSinceSend))
	} else {
		r.RetryDelay = pb.Base
	}
	return r
}
