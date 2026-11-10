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
		{"ambiguous in flight retries soon", step(3, 8, "PENDING"),
			provider.Outcome{Class: provider.Ambiguous, InFlight: true, HTTPStatus: 409, Err: boom},
			Result{Kind: ResultInDoubt, HTTPStatus: 409, Error: "boom", RetryDelay: inFlight}, false},
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
			Result{Kind: ResultDead, HTTPStatus: 429, Error: "boom", DeadReason: DeadAttemptsExhaustedNoEffect}, false},
		{"no effect exhausted, was in doubt", step(8, 8, "IN_DOUBT"),
			provider.Outcome{Class: provider.NoEffect, HTTPStatus: 429, Err: boom},
			Result{Kind: ResultDead, HTTPStatus: 429, Error: "boom", DeadReason: DeadAttemptsExhaustedInDoubt}, false},
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
