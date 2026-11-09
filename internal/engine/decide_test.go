package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/provider"
)

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
			require.Equal(t, c.want, decide(c.s, c.out, b, inFlight, c.pivot))
		})
	}
}
