package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/provider"
)

const callbackDeadline = 10 * time.Minute

func TestDecide(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: 5 * time.Minute, Rand: func() float64 { return 0.5 }}
	const inFlight = 300 * time.Millisecond
	boom := errors.New("boom")

	step := func(attempt, maxAttempts int32, prior string) ClaimedStep {
		return ClaimedStep{Attempt: attempt, MaxAttempts: maxAttempts, PriorStatus: prior}
	}

	cases := []struct {
		name  string
		s     ClaimedStep
		out   provider.Outcome
		want  Result
		pivot bool
	}{
		{"success", step(8, 8, "IN_DOUBT"),
			provider.Outcome{Class: provider.Success, ProviderRef: "ch_1", HTTPStatus: 201},
			Result{Kind: ResultSucceeded, ProviderRef: "ch_1", HTTPStatus: 201}, false},
		{"permanent", step(1, 8, "PENDING"),
			provider.Outcome{Class: provider.Permanent, HTTPStatus: 402, Err: boom},
			Result{Kind: ResultFailedPermanent, HTTPStatus: 402, Error: "boom"}, false},
		{"ambiguous with attempts left", step(3, 8, "PENDING"),
			provider.Outcome{Class: provider.Ambiguous, HTTPStatus: 500, Err: boom},
			Result{Kind: ResultInDoubt, HTTPStatus: 500, Error: "boom", RetryDelay: 2 * time.Second}, false}, // 0.5 × 4s
		// Pre-existing issue fixed before merge: a provider still processing
		// the key must not burn the whole attempt budget in a second. In-flight
		// retries start soon (inFlightDelay) and back off with equal jitter.
		{"first in-flight retry is soon", step(1, 8, "PENDING"),
			provider.Outcome{Class: provider.Ambiguous, InFlight: true, HTTPStatus: 409, Err: boom},
			Result{Kind: ResultInDoubt, HTTPStatus: 409, Error: "boom", RetryDelay: 225 * time.Millisecond}, false}, // 300ms/2 + 0.5 × 300ms/2
		{"later in-flight retries back off", step(3, 8, "PENDING"),
			provider.Outcome{Class: provider.Ambiguous, InFlight: true, HTTPStatus: 409, Err: boom},
			Result{Kind: ResultInDoubt, HTTPStatus: 409, Error: "boom", RetryDelay: 900 * time.Millisecond}, false}, // 1.2s/2 + 0.5 × 1.2s/2
		{"no effect with attempts left", step(1, 8, "PENDING"),
			provider.Outcome{Class: provider.NoEffect, HTTPStatus: 429, Err: boom},
			Result{Kind: ResultRetryNoEffect, HTTPStatus: 429, Error: "boom", RetryDelay: 500 * time.Millisecond}, false},
		{"no effect honours Retry-After", step(1, 8, "PENDING"),
			provider.Outcome{Class: provider.NoEffect, HTTPStatus: 429, RetryAfter: 30 * time.Second, Err: boom},
			Result{Kind: ResultRetryNoEffect, HTTPStatus: 429, Error: "boom", RetryDelay: 30 * time.Second}, false},
		{"Retry-After capped at backoff cap", step(1, 8, "PENDING"),
			provider.Outcome{Class: provider.NoEffect, HTTPStatus: 429, RetryAfter: 10 * time.Minute, Err: boom},
			Result{Kind: ResultRetryNoEffect, HTTPStatus: 429, Error: "boom", RetryDelay: 5 * time.Minute}, false},
		{"ambiguous exhausted", step(8, 8, "PENDING"),
			provider.Outcome{Class: provider.Ambiguous, Err: boom},
			Result{Kind: ResultDead, Error: "boom", DeadReason: DeadAttemptsExhaustedInDoubt}, false},
		{"no effect exhausted, never in doubt", step(8, 8, "PENDING"),
			provider.Outcome{Class: provider.NoEffect, HTTPStatus: 429, Err: boom},
			Result{Kind: ResultDead, HTTPStatus: 429, Error: "boom", DeadReason: DeadAttemptsExhaustedNoEffect,
				AttemptOutcome: ResultRetryNoEffect}, false},
		{"no effect exhausted, was in doubt", step(8, 8, "IN_DOUBT"),
			provider.Outcome{Class: provider.NoEffect, HTTPStatus: 429, Err: boom},
			Result{Kind: ResultDead, HTTPStatus: 429, Error: "boom", DeadReason: DeadAttemptsExhaustedInDoubt,
				AttemptOutcome: ResultRetryNoEffect}, false},
		{"beyond max attempts still dead", step(9, 8, "IN_DOUBT"),
			provider.Outcome{Class: provider.Ambiguous, Err: boom},
			Result{Kind: ResultDead, Error: "boom", DeadReason: DeadAttemptsExhaustedInDoubt}, false},
		{name: "pivot permanent after doubt parks", s: step(3, 8, "IN_DOUBT"), pivot: true,
			out:  provider.Outcome{Class: provider.Permanent, HTTPStatus: 422, Err: boom},
			want: Result{Kind: ResultDead, HTTPStatus: 422, Error: "boom", DeadReason: DeadPermanentAfterInDoubt}},
		{name: "pivot permanent on first try fails normally", s: step(1, 8, "PENDING"), pivot: true,
			out:  provider.Outcome{Class: provider.Permanent, HTTPStatus: 422, Err: boom},
			want: Result{Kind: ResultFailedPermanent, HTTPStatus: 422, Error: "boom"}},
		{name: "non-pivot permanent after doubt", s: step(3, 8, "IN_DOUBT"), pivot: false,
			out:  provider.Outcome{Class: provider.Permanent, HTTPStatus: 402, Err: boom},
			want: Result{Kind: ResultFailedPermanent, HTTPStatus: 402, Error: "boom"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, decide(c.s, c.out, b, inFlight, callbackDeadline, c.pivot))
		})
	}
}

// Slice 2.1: an accepted async request waits for its callback deadline.
func TestDecide_AcceptedWaitsForCallback(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: 5 * time.Minute, Rand: func() float64 { return 0.5 }}
	r := decide(ClaimedStep{Attempt: 1, MaxAttempts: 8, PriorStatus: "PENDING"},
		provider.Outcome{Class: provider.Accepted, ProviderRef: "po_1", HTTPStatus: 202}, b, time.Second, callbackDeadline, true)
	require.Equal(t, Result{Kind: ResultAwaitingCallback, ProviderRef: "po_1", HTTPStatus: 202, RetryDelay: callbackDeadline}, r)
}

// The pivot rule reads the send history, not only the previous status: once a
// payout has waited in AWAITING_CALLBACK its prior status no longer says
// whether an earlier send was ambiguous.
func TestDecide_PivotRuleReadsSendHistory(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: 5 * time.Minute, Rand: func() float64 { return 0.5 }}
	refused := provider.Outcome{Class: provider.Permanent, HTTPStatus: 422, Err: errors.New("invalid_account")}
	doubt := decide(ClaimedStep{Attempt: 2, MaxAttempts: 8, SendDoubt: true}, refused, b, time.Second, callbackDeadline, true)
	require.Equal(t, ResultDead, doubt.Kind)
	require.Equal(t, DeadPermanentAfterInDoubt, doubt.DeadReason)
	clean := decide(ClaimedStep{Attempt: 2, MaxAttempts: 8}, refused, b, time.Second, callbackDeadline, true)
	require.Equal(t, ResultFailedPermanent, clean.Kind)
}

func TestDecidePoll(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: 5 * time.Minute, Rand: func() float64 { return 0.5 }}
	const maxPolls = 8
	failed := errors.New("payout failed: account_closed")
	poll := func(n int32, doubt bool) ClaimedStep {
		return ClaimedStep{Attempt: 1, MaxAttempts: 8, PollCount: n, PollsSinceSend: n, PriorStatus: "AWAITING_CALLBACK", ProviderRef: "po_1", SendDoubt: doubt}
	}
	cases := []struct {
		name string
		s    ClaimedStep
		out  provider.Outcome
		want Result
	}{
		{"paid", poll(1, false), provider.Outcome{Class: provider.Success, ProviderRef: "po_1", HTTPStatus: 200},
			Result{Kind: ResultSucceeded, ProviderRef: "po_1", HTTPStatus: 200}},
		{"failed cleanly", poll(2, false), provider.Outcome{Class: provider.Permanent, HTTPStatus: 200, Err: failed},
			Result{Kind: ResultFailedPermanent, ProviderRef: "po_1", HTTPStatus: 200, Error: failed.Error()}},
		{"failed after an ambiguous send", poll(2, true), provider.Outcome{Class: provider.Permanent, HTTPStatus: 200, Err: failed},
			Result{Kind: ResultDead, ProviderRef: "po_1", HTTPStatus: 200, Error: failed.Error(), DeadReason: DeadPermanentAfterInDoubt}},
		{"still pending", poll(3, false), provider.Outcome{Class: provider.Pending, HTTPStatus: 200, Err: errors.New("payout still pending")},
			Result{Kind: ResultAwaitingCallback, ProviderRef: "po_1", HTTPStatus: 200, Error: "payout still pending",
				RetryDelay: time.Second + 2*time.Second}}, // Base + 0.5 × 4s
		{"still pending at the poll budget", poll(maxPolls, false), provider.Outcome{Class: provider.Pending, HTTPStatus: 200},
			Result{Kind: ResultDead, ProviderRef: "po_1", HTTPStatus: 200, DeadReason: DeadCallbackUnresolved}},
		{"poll failed: never doubt", poll(3, false), provider.Outcome{Class: provider.Ambiguous, HTTPStatus: 500, Err: errors.New("boom")},
			Result{Kind: ResultAwaitingCallback, ProviderRef: "po_1", HTTPStatus: 500, Error: "boom", RetryDelay: time.Second}},
		{"poll unreachable at the budget", poll(maxPolls, false), provider.Outcome{Class: provider.NoEffect, Err: errors.New("refused")},
			Result{Kind: ResultDead, ProviderRef: "po_1", Error: "refused", DeadReason: DeadCallbackUnresolved}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, decidePoll(c.s, c.out, b, PollLimits{MaxPolls: maxPolls}, true))
		})
	}
}

// Fix before merge: real payouts settle in hours or days. The budget is a total
// wait (MaxWait), polls back off up to the poll cap, and a poll count limit is
// optional (0 = none).
func TestDecidePoll_TimeBudgetAndPollCap(t *testing.T) {
	pb := Backoff{Base: 2 * time.Second, Cap: time.Hour, Rand: func() float64 { return 1 }}
	pending := provider.Outcome{Class: provider.Pending, HTTPStatus: 200}
	waiting := func(polls int32, waited time.Duration) ClaimedStep {
		return ClaimedStep{Attempt: 1, MaxAttempts: 8, PollCount: polls, PollsSinceSend: polls,
			PriorStatus: "AWAITING_CALLBACK", ProviderRef: "po_1", WaitedFor: waited}
	}
	lim := PollLimits{MaxWait: 72 * time.Hour}

	r := decidePoll(waiting(500, 10*time.Hour), pending, pb, lim, true)
	require.Equal(t, ResultAwaitingCallback, r.Kind, "no poll count limit by default")
	require.Equal(t, time.Hour, r.RetryDelay, "polls back off up to the poll cap")

	r = decidePoll(waiting(3, 73*time.Hour), pending, pb, lim, true)
	require.Equal(t, ResultDead, r.Kind, "waited longer than MaxWait")
	require.Equal(t, DeadCallbackUnresolved, r.DeadReason)

	r = decidePoll(waiting(3, time.Minute), provider.Outcome{Class: provider.Ambiguous}, pb, PollLimits{MaxPolls: 3, MaxWait: 72 * time.Hour}, true)
	require.Equal(t, ResultDead, r.Kind, "an optional poll count limit still applies")
}

// Slice 2.3b (review R12): an ambiguous answer that carries Retry-After (a
// rate-limited lookup) waits at least that long, as a no-effect 429 does.
func TestDecide_InDoubtHonoursRetryAfter(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: 5 * time.Minute, Rand: func() float64 { return 0.5 }}
	s := ClaimedStep{Attempt: 1, MaxAttempts: 8, PriorStatus: "IN_DOUBT"}
	r := decide(s, provider.Outcome{Class: provider.Ambiguous, RetryAfter: 2 * time.Minute}, b, time.Second, callbackDeadline, false)
	require.Equal(t, ResultInDoubt, r.Kind)
	require.Equal(t, 2*time.Minute, r.RetryDelay)
}

// Slice 2.5b (implicit-state review, item 5): a step whose earlier send may
// have moved money, refused "no effect" until its budget ran out, parks as
// money-may-have-moved, whatever its status just before this claim.
func TestDecide_NoEffectExhaustedAfterEarlierDoubtIsInDoubt(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: 5 * time.Minute, Rand: func() float64 { return 0.5 }}
	refusedNoEffect := provider.Outcome{Class: provider.NoEffect, HTTPStatus: 429, Err: errors.New("rate limited")}
	r := decide(ClaimedStep{Attempt: 8, MaxAttempts: 8, PriorStatus: "PENDING", SendDoubt: true}, refusedNoEffect, b, time.Second, callbackDeadline, false)
	require.Equal(t, ResultDead, r.Kind)
	require.Equal(t, DeadAttemptsExhaustedInDoubt, r.DeadReason)
}

// The one Go definition of "an earlier send may have moved money".
func TestMoneyMayHaveMoved_Matrix(t *testing.T) {
	for _, prior := range []string{"PENDING", "IN_DOUBT", "AWAITING_CALLBACK"} {
		for _, doubt := range []bool{false, true} {
			s := ClaimedStep{PriorStatus: prior, SendDoubt: doubt}
			require.Equal(t, prior == "IN_DOUBT" || doubt, s.MoneyMayHaveMoved(), "%s doubt=%v", prior, doubt)
		}
	}
}

// Sticky doubt follows the same definition: a no-effect retry of a step whose
// earlier send may have moved money stays IN_DOUBT.
func TestStepStatus_NoEffectKeepsAnyDoubt(t *testing.T) {
	require.Equal(t, "IN_DOUBT", stepStatus(ClaimedStep{PriorStatus: "PENDING", SendDoubt: true}, ResultRetryNoEffect))
	require.Equal(t, "IN_DOUBT", stepStatus(ClaimedStep{PriorStatus: "IN_DOUBT"}, ResultRetryNoEffect))
	require.Equal(t, "PENDING", stepStatus(ClaimedStep{PriorStatus: "PENDING"}, ResultRetryNoEffect))
}

// Near key retention, a step whose earlier send may have moved money is
// looked up, never re-sent: the provider may have forgotten the key and
// perform it again.
func TestNearKeyRetention_AnyDoubt(t *testing.T) {
	w := &Worker{opts: WorkerOptions{KeyRetention: 24 * time.Hour, KeyRetentionMargin: time.Hour}}
	old := 23*time.Hour + 30*time.Minute
	require.True(t, w.nearKeyRetention(ClaimedStep{PriorStatus: "PENDING", SendDoubt: true, KeyAge: old}))
	require.True(t, w.nearKeyRetention(ClaimedStep{PriorStatus: "IN_DOUBT", KeyAge: old}))
	require.False(t, w.nearKeyRetention(ClaimedStep{PriorStatus: "PENDING", KeyAge: old}))
	require.False(t, w.nearKeyRetention(ClaimedStep{PriorStatus: "IN_DOUBT", KeyAge: time.Hour}))
}
