package engine_test

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/invariants"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/workflow"
)

const (
	leaseTTL    = 300 * time.Millisecond
	pastLease   = 400 * time.Millisecond
	callTimeout = 200 * time.Millisecond
)

type harness struct {
	pool   *pgxpool.Pool
	fake   *fakeprovider.Provider
	client *provider.Client
}

func newHarness(t *testing.T) harness {
	t.Helper()
	return newHarnessWith(t, fakeprovider.Options{Hang: 5 * time.Second})
}

func newHarnessWith(t *testing.T, opts fakeprovider.Options) harness {
	t.Helper()
	t.Cleanup(failpoint.Reset)
	fake := fakeprovider.New(opts)
	srv := httptest.NewServer(fake.Handler())
	t.Cleanup(srv.Close)
	return harness{pool: testutil_NewPool(t), fake: fake, client: provider.NewClient(srv.URL, callTimeout)}
}

func (h harness) worker(id string, steps ...engine.Step) *engine.Worker {
	return h.workerWith(id, nil, steps...)
}

// workerWith builds a worker whose retries are immediate unless tweak changes them.
func (h harness) workerWith(id string, tweak func(*engine.WorkerOptions), steps ...engine.Step) *engine.Worker {
	if len(steps) == 0 {
		steps = []engine.Step{engine.NewChargeStep(h.client)}
	}
	opts := engine.WorkerOptions{
		ID:            id,
		Steps:         steps,
		LeaseTTL:      leaseTTL,
		PollInterval:  10 * time.Millisecond,
		Backoff:       engine.Backoff{}, // zero base: retry immediately
		ShutdownGrace: 5 * time.Second,
		Logger:        slog.New(slog.DiscardHandler),
		MaxPolls:      8, // CallbackDeadline 0: an accepted payout is polled at once
	}
	if tweak != nil {
		tweak(&opts)
	}
	return engine.NewWorker(h.pool, opts)
}

func (h harness) setFaults(rules ...fakeprovider.Rule) {
	h.fake.SetFaults(fakeprovider.Faults{Rules: rules})
}

func (h harness) setMaxAttempts(t *testing.T, wf uuid.UUID, n int) {
	t.Helper()
	_, err := h.pool.Exec(context.Background(), `UPDATE steps SET max_attempts = $2 WHERE workflow_id = $1 AND name = 'charge'`, wf, n)
	require.NoError(t, err)
}

func charges(mode string, times int) fakeprovider.Rule {
	return fakeprovider.Rule{Endpoint: "charges", Mode: mode, Times: times}
}

func (h harness) runOnce(t *testing.T, w *engine.Worker) engine.RunResult {
	t.Helper()
	res, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	return res
}

func (h harness) reclaim(t *testing.T) int64 {
	t.Helper()
	n, err := engine.ReclaimExpiredLeases(context.Background(), h.pool)
	require.NoError(t, err)
	return n
}

func (h harness) assertNoViolations(t *testing.T) {
	t.Helper()
	v, err := invariants.Check(context.Background(), h.pool, h.fake.Effects())
	require.NoError(t, err)
	require.Empty(t, v)
}

func (h harness) chargeStatus(t *testing.T, wf uuid.UUID) string {
	t.Helper()
	return scalar[string](t, h.pool, `SELECT status FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf)
}

func (h harness) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	return scalar[int](t, h.pool, query, args...)
}

// assertChargedOnce checks the end state every recovery path must reach.
func (h harness) assertChargedOnce(t *testing.T, wf uuid.UUID) {
	t.Helper()
	require.Equal(t, "SUCCEEDED", h.chargeStatus(t, wf))
	require.Len(t, h.fake.Effects(), 1, "exactly one charge at the provider")
	require.Equal(t, 1, h.count(t, `SELECT count(*) FROM ledger_transactions WHERE workflow_id = $1`, wf))
	require.Equal(t, 1, h.count(t, `SELECT count(*) FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	h.assertNoViolations(t)
}

// crashes runs f and returns whether it panicked with a simulated crash.
func crashes(f func()) (crashed bool) {
	defer func() {
		if r := recover(); r != nil {
			if !failpoint.IsCrash(r) {
				panic(r)
			}
			crashed = true
		}
	}()
	f()
	return false
}

func TestWorker_NothingToDo(t *testing.T) {
	h := newHarness(t)
	require.False(t, h.runOnce(t, h.worker("w1")).Claimed)
}

func TestWorker_ChargeSucceeds(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")

	res := h.runOnce(t, h.worker("w1"))
	require.True(t, res.Claimed)
	require.Equal(t, "succeeded", res.Outcome)

	require.Equal(t, "SUCCEEDED", h.chargeStatus(t, wf))
	ref := scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf)
	require.Equal(t, h.fake.Effects()[0].ID, ref)

	type entry struct {
		account string
		amount  int64
	}
	rows, err := h.pool.Query(context.Background(), `
		SELECT e.account, e.amount_cents FROM ledger_entries e
		JOIN ledger_transactions t ON t.id = e.transaction_id
		WHERE t.workflow_id = $1 AND t.kind = 'charge' ORDER BY e.amount_cents DESC`, wf)
	require.NoError(t, err)
	var entries []entry
	for rows.Next() {
		var e entry
		require.NoError(t, rows.Scan(&e.account, &e.amount))
		entries = append(entries, e)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []entry{{"provider_receivable", 10000}, {"platform_revenue", -175}, {"merchant_payable", -9825}}, entries)

	require.Equal(t, "PENDING", scalar[string](t, h.pool, `SELECT status FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	require.Equal(t, 0, scalar[int](t, h.pool, `SELECT generation FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	require.Equal(t, workflow.StepIdempotencyKey(wf, "payout", 0),
		scalar[string](t, h.pool, `SELECT idempotency_key FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf))
	require.Equal(t, "RUNNING", scalar[string](t, h.pool, `SELECT status FROM workflows WHERE id = $1`, wf))

	effects := h.fake.Effects()
	require.Len(t, effects, 1)
	require.Equal(t, workflow.StepIdempotencyKey(wf, "charge", 0), effects[0].IdempotencyKey)
	h.assertNoViolations(t)
}

func TestWorker_DeclineFailsWorkflow_F10(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_decline_1")

	require.Equal(t, "failed_permanent", h.runOnce(t, h.worker("w1")).Outcome)
	require.Equal(t, "FAILED_PERMANENT", h.chargeStatus(t, wf))
	require.Equal(t, "FAILED", scalar[string](t, h.pool, `SELECT status FROM workflows WHERE id = $1`, wf))
	require.Equal(t, 0, h.count(t, `SELECT count(*) FROM ledger_transactions`))
	require.Equal(t, 0, h.count(t, `SELECT count(*) FROM steps WHERE name = 'payout'`))
	require.Empty(t, h.fake.Effects())
	h.assertNoViolations(t)
}

func TestWorker_AmbiguousRetriesWithSameKey_F7_F9(t *testing.T) {
	for _, mode := range []string{
		fakeprovider.ModeTimeoutAfterEffect,  // F7: money moved, response lost
		fakeprovider.ModeErrorAfterEffect,    // F9: 5xx after the effect
		fakeprovider.ModeTimeoutBeforeEffect, // F8: timeout, nothing happened
		fakeprovider.ModeErrorBeforeEffect,
	} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t)
			h.fake.SetFaults(fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "charges", Mode: mode, Times: 1}}})
			wf := submit(t, h.pool, "cus_1")
			w := h.worker("w1")

			require.Equal(t, "in_doubt", h.runOnce(t, w).Outcome)
			require.Equal(t, "IN_DOUBT", h.chargeStatus(t, wf))
			// A timed-out request may still hold the key at the provider briefly.
			require.Eventually(t, func() bool { return h.runOnce(t, w).Outcome == "succeeded" },
				3*time.Second, 50*time.Millisecond)
			h.assertChargedOnce(t, wf)
		})
	}
}

func TestWorker_CrashRecovery_F4_F5_F6(t *testing.T) {
	for _, fp := range []string{
		engine.FPAfterClaim,
		engine.FPBeforeProviderCall, // F4
		engine.FPAfterProviderCall,  // F5
		engine.FPBeforeCommit,       // F6
		engine.FPAfterCommit,
	} {
		t.Run(fp, func(t *testing.T) {
			h := newHarness(t)
			wf := submit(t, h.pool, "cus_1")
			failpoint.Enable(fp, failpoint.CrashOnce())

			require.True(t, crashes(func() { _, _ = h.worker("wA").RunOnce(context.Background()) }))

			switch fp {
			case engine.FPBeforeCommit:
				require.Equal(t, "RUNNING", h.chargeStatus(t, wf), "completion must have rolled back")
				require.Equal(t, 0, h.count(t, `SELECT count(*) FROM ledger_transactions`))
			case engine.FPAfterCommit:
				require.Equal(t, "SUCCEEDED", h.chargeStatus(t, wf))
			default:
				require.Equal(t, "RUNNING", h.chargeStatus(t, wf))
			}

			time.Sleep(pastLease)
			wB := h.worker("wB")
			if fp == engine.FPAfterCommit {
				require.Equal(t, int64(0), h.reclaim(t))
				require.False(t, h.runOnce(t, wB).Claimed)
			} else {
				require.Equal(t, int64(1), h.reclaim(t))
				require.Equal(t, "succeeded", h.runOnce(t, wB).Outcome)
			}
			h.assertChargedOnce(t, wf)
		})
	}
}

// pausedWorker runs worker A until it blocks at fp, then lets its lease expire,
// reclaims, and has worker B finish the step. It returns A's eventual result.
func pausedWorker(t *testing.T, h harness, fp string) (engine.RunResult, uuid.UUID) {
	t.Helper()
	return pausedWorkerOutcome(t, h, submit(t, h.pool, "cus_1"), fp, "succeeded")
}

func pausedWorkerOutcome(t *testing.T, h harness, wf uuid.UUID, fp, wantB string) (engine.RunResult, uuid.UUID) {
	t.Helper()
	action, entered, release := failpoint.BlockOnce()
	failpoint.Enable(fp, action)
	defer release()

	var aRes engine.RunResult
	var aErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		aRes, aErr = h.worker("wA").RunOnce(context.Background())
	}()

	<-entered
	time.Sleep(pastLease)
	require.Equal(t, int64(1), h.reclaim(t))
	require.Equal(t, wantB, h.runOnce(t, h.worker("wB")).Outcome)

	release()
	wg.Wait()
	require.NoError(t, aErr)
	return aRes, wf
}

func TestWorker_PausedWorkerIsFenced_F12(t *testing.T) {
	h := newHarness(t)
	res, wf := pausedWorker(t, h, engine.FPAfterProviderCall)
	require.Equal(t, "fenced", res.Outcome)
	require.Equal(t, int64(3), scalar[int64](t, h.pool, `SELECT lease_epoch FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf))
	h.assertChargedOnce(t, wf)
}

func TestWorker_LateProviderCallIsDeduplicated_F13(t *testing.T) {
	h := newHarness(t)
	res, wf := pausedWorker(t, h, engine.FPBeforeProviderCall)
	require.Equal(t, "fenced", res.Outcome)
	h.assertChargedOnce(t, wf)
}

func TestWorker_PanicInStepIsInDoubt(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")
	step := &stubStep{execute: func(context.Context, engine.ClaimedStep) provider.Outcome { panic("boom") }}

	require.Equal(t, "in_doubt", h.runOnce(t, h.worker("w1", step)).Outcome)
	require.Equal(t, "IN_DOUBT", h.chargeStatus(t, wf))
	require.Contains(t, scalar[string](t, h.pool, `SELECT last_error FROM steps WHERE workflow_id = $1`, wf), "panic")
}

func runInBackground(t *testing.T, f func(context.Context) error) (cancel func(), done <-chan error) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- f(ctx) }()
	t.Cleanup(cancelFn)
	return cancelFn, ch
}

func TestWorker_RunProcessesUntilCancelled(t *testing.T) {
	h := newHarness(t)
	for range 5 {
		submit(t, h.pool, "cus_1")
	}
	cancel, done := runInBackground(t, h.worker("w1").Run)

	require.Eventually(t, func() bool {
		return h.count(t, `SELECT count(*) FROM steps WHERE name = 'charge' AND status = 'SUCCEEDED'`) == 5
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
	require.Len(t, h.fake.Effects(), 5)
	h.assertNoViolations(t)
}

func TestWorker_RunSurvivesStepErrors(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")
	step := &stubStep{successErr: context.DeadlineExceeded, successErrFor: 1}

	_, _ = runInBackground(t, h.worker("w1", step).Run)
	_, _ = runInBackground(t, func(ctx context.Context) error {
		return engine.RunSweeper(ctx, h.pool, engine.SweeperOptions{Interval: 50 * time.Millisecond, StuckAfter: time.Hour, Logger: slog.New(slog.DiscardHandler)})
	})

	require.Eventually(t, func() bool { return h.chargeStatus(t, wf) == "SUCCEEDED" }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, 2, step.successes())
}

// Review finding: a panic in a completion hook must not kill the worker
// (spec §6, F25). It rolls back like a hook error and is counted as one
// (slice 2.3b): the provider acted, so the step is IN_DOUBT at once.
func TestWorker_HookPanicIsRecovered(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")
	step := &stubStep{panicFor: 1}

	var res engine.RunResult
	var err error
	require.NotPanics(t, func() { res, err = h.worker("w1", step).RunOnce(context.Background()) })
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic")
	require.True(t, res.Claimed)
	require.Equal(t, "IN_DOUBT", h.chargeStatus(t, wf), "rolled back; counted as a hook failure, no lease wait")
	require.Equal(t, "succeeded", h.runOnce(t, h.worker("w2", step)).Outcome)
}

func TestWorker_RateLimitRetriesWithoutDoubt_F11(t *testing.T) {
	h := newHarness(t)
	h.setFaults(charges(fakeprovider.ModeRateLimit, 1))
	wf := submit(t, h.pool, "cus_1")
	w := h.workerWith("w1", func(o *engine.WorkerOptions) { o.Backoff.Cap = 0 }) // ignore Retry-After: 1s

	require.Equal(t, "retry_no_effect", h.runOnce(t, w).Outcome)
	require.Equal(t, "PENDING", h.chargeStatus(t, wf), "a 429 proves no money moved")
	require.Equal(t, "succeeded", h.runOnce(t, w).Outcome)
	h.assertChargedOnce(t, wf)
}

func TestWorker_NoEffectAfterInDoubtStaysInDoubt(t *testing.T) {
	h := newHarness(t)
	// Attempt 1 times out (ambiguous); the next new request is rate limited (429).
	h.setFaults(charges(fakeprovider.ModeTimeoutBeforeEffect, 1), charges(fakeprovider.ModeRateLimit, 1))
	wf := submit(t, h.pool, "cus_1")
	w := h.workerWith("w1", func(o *engine.WorkerOptions) { o.Backoff.Cap = 0 })

	require.Equal(t, "in_doubt", h.runOnce(t, w).Outcome)
	// Until the provider notices the timed-out client, the key is in flight (409 → in_doubt).
	require.Eventually(t, func() bool { return h.runOnce(t, w).Outcome == "retry_no_effect" },
		3*time.Second, 20*time.Millisecond)
	require.Equal(t, "IN_DOUBT", h.chargeStatus(t, wf), "the first attempt may have moved money: doubt is sticky")

	require.Equal(t, "succeeded", h.runOnce(t, w).Outcome)
	h.assertChargedOnce(t, wf)
}

func TestWorker_DeadReasons_F22(t *testing.T) {
	cases := []struct {
		mode, first, reason string
	}{
		{fakeprovider.ModeErrorBeforeEffect, "in_doubt", engine.DeadAttemptsExhaustedInDoubt}, // 5xx is ambiguous
		{fakeprovider.ModeRateLimit, "retry_no_effect", engine.DeadAttemptsExhaustedNoEffect},
	}
	for _, c := range cases {
		t.Run(c.mode, func(t *testing.T) {
			h := newHarness(t)
			h.setFaults(charges(c.mode, 0))
			wf := submit(t, h.pool, "cus_1")
			h.setMaxAttempts(t, wf, 2)
			w := h.workerWith("w1", func(o *engine.WorkerOptions) { o.Backoff.Cap = 0 })

			require.Equal(t, c.first, h.runOnce(t, w).Outcome)
			require.Equal(t, "dead", h.runOnce(t, w).Outcome)

			require.Equal(t, "DEAD", h.chargeStatus(t, wf))
			require.Equal(t, c.reason, scalar[string](t, h.pool, `SELECT dead_reason FROM steps WHERE workflow_id = $1`, wf))
			require.Equal(t, "NEEDS_ATTENTION", scalar[string](t, h.pool, `SELECT status FROM workflows WHERE id = $1`, wf))
			require.Equal(t, 0, h.count(t, `SELECT count(*) FROM ledger_transactions`))
			require.False(t, h.runOnce(t, w).Claimed, "a dead step is never retried")
			require.Empty(t, h.fake.Effects())
			h.assertNoViolations(t)
		})
	}
}

func TestWorker_DeadIsFenced(t *testing.T) {
	h := newHarness(t)
	h.setFaults(charges(fakeprovider.ModeErrorBeforeEffect, 0))
	wf := submit(t, h.pool, "cus_1")
	h.setMaxAttempts(t, wf, 1)

	res, _ := pausedWorkerOutcome(t, h, wf, engine.FPAfterProviderCall, "dead")
	require.Equal(t, "fenced", res.Outcome)
	require.Equal(t, 1, h.count(t, `SELECT count(*) FROM step_attempts WHERE outcome = 'DEAD'`))
	require.Equal(t, "DEAD", h.chargeStatus(t, wf))
}

func TestWorker_InFlightConflictRetriesSoon(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")
	step := &stubStep{execute: func(context.Context, engine.ClaimedStep) provider.Outcome {
		return provider.Outcome{Class: provider.Ambiguous, InFlight: true, HTTPStatus: 409}
	}}
	w := h.workerWith("w1", func(o *engine.WorkerOptions) {
		o.Backoff = engine.Backoff{Base: time.Hour, Cap: time.Hour}
		o.InFlightDelay = time.Second
	}, step)

	require.Equal(t, "in_doubt", h.runOnce(t, w).Outcome)
	// Equal jitter draws [500ms, 1s] at completion; now() is read a little
	// later, so allow slack below 500ms. Still far from the 1h backoff.
	require.True(t, scalar[bool](t, h.pool,
		`SELECT run_after BETWEEN now() + interval '400 milliseconds' AND now() + interval '2 seconds' FROM steps WHERE workflow_id = $1`, wf))
}

func TestWorker_ShutdownGraceCancelsStuckStep(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")
	started := make(chan struct{})
	step := &stubStep{execute: func(ctx context.Context, _ engine.ClaimedStep) provider.Outcome {
		close(started)
		<-ctx.Done()
		return provider.Outcome{Class: provider.Ambiguous, Err: ctx.Err()}
	}}
	w := h.workerWith("w1", func(o *engine.WorkerOptions) { o.ShutdownGrace = 100 * time.Millisecond }, step)

	cancel, done := runInBackground(t, w.Run)
	<-started
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the shutdown grace")
	}
	require.Equal(t, "IN_DOUBT", h.chargeStatus(t, wf), "the outcome is recorded even though the call was cut short")
}

func retention(keep, margin time.Duration) func(*engine.WorkerOptions) {
	return func(o *engine.WorkerOptions) {
		o.KeyRetention, o.KeyRetentionMargin = keep, margin
	}
}

func TestWorker_KeyNearRetentionResolvedByLookup(t *testing.T) {
	h := newHarnessWith(t, fakeprovider.Options{Hang: 5 * time.Second, KeyRetention: 2 * time.Second})
	h.setFaults(charges(fakeprovider.ModeTimeoutAfterEffect, 1))
	wf := submit(t, h.pool, "cus_1")
	w := h.workerWith("w1", retention(2*time.Second, 1500*time.Millisecond)) // look up once the key is 500ms old

	require.Equal(t, "in_doubt", h.runOnce(t, w).Outcome)
	time.Sleep(400 * time.Millisecond)
	require.Equal(t, "succeeded", h.runOnce(t, w).Outcome)

	require.Equal(t, 1, h.fake.Calls()["POST /charges"], "the retry must not re-send the charge")
	require.Equal(t, 1, h.fake.Calls()["GET /charges"])
	require.Equal(t, h.fake.Effects()[0].ID,
		scalar[string](t, h.pool, `SELECT provider_ref FROM steps WHERE workflow_id = $1 AND name = 'charge'`, wf))
	h.assertChargedOnce(t, wf)
}

func TestWorker_KeyPastRetentionUnknownIsDead_F23(t *testing.T) {
	h := newHarnessWith(t, fakeprovider.Options{Hang: 5 * time.Second, KeyRetention: 300 * time.Millisecond})
	h.setFaults(charges(fakeprovider.ModeTimeoutBeforeEffect, 1))
	wf := submit(t, h.pool, "cus_1")
	w := h.workerWith("w1", retention(300*time.Millisecond, 100*time.Millisecond))

	require.Equal(t, "in_doubt", h.runOnce(t, w).Outcome)
	time.Sleep(400 * time.Millisecond)
	require.Equal(t, "dead", h.runOnce(t, w).Outcome)

	require.Equal(t, engine.DeadKeyExpiredUnknown, scalar[string](t, h.pool, `SELECT dead_reason FROM steps WHERE workflow_id = $1`, wf))
	require.Equal(t, "NEEDS_ATTENTION", scalar[string](t, h.pool, `SELECT status FROM workflows WHERE id = $1`, wf))
	require.Equal(t, 1, h.fake.Calls()["POST /charges"], "never charge again with a key the provider may have forgotten")
	require.Empty(t, h.fake.Effects())
	h.assertNoViolations(t)
}

func TestWorker_FreshInDoubtStillRetriesWithSameKey(t *testing.T) {
	h := newHarness(t)
	h.setFaults(charges(fakeprovider.ModeTimeoutAfterEffect, 1))
	wf := submit(t, h.pool, "cus_1")
	w := h.workerWith("w1", retention(time.Hour, time.Minute))

	require.Equal(t, "in_doubt", h.runOnce(t, w).Outcome)
	require.Equal(t, "succeeded", h.runOnce(t, w).Outcome)
	require.Equal(t, 0, h.fake.Calls()["GET /charges"])
	h.assertChargedOnce(t, wf)
}

func TestWorker_RunFinishesInFlightStepOnCancel(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")
	action, entered, release := failpoint.BlockOnce()
	failpoint.Enable(engine.FPAfterProviderCall, action)

	cancel, done := runInBackground(t, h.worker("w1").Run)
	<-entered
	cancel()
	release()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
	h.assertChargedOnce(t, wf)
}

// recordingHandler captures log records for assertions.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) find(msg, key, value string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Message != msg {
			continue
		}
		found := false
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key && a.Value.String() == value {
				found = true
				return false
			}
			return true
		})
		if found {
			return true
		}
	}
	return false
}

func TestRunSweeper_LogsStuckWorkflows(t *testing.T) {
	h := newHarness(t)
	wf := submit(t, h.pool, "cus_1")
	_, err := h.pool.Exec(context.Background(),
		`UPDATE workflows SET status = 'RUNNING', updated_at = now() - interval '2 hours' WHERE id = $1`, wf)
	require.NoError(t, err)

	rec := &recordingHandler{}
	_, _ = runInBackground(t, func(ctx context.Context) error {
		return engine.RunSweeper(ctx, h.pool, engine.SweeperOptions{Interval: 10 * time.Millisecond, StuckAfter: time.Hour, Logger: slog.New(rec)})
	})
	require.Eventually(t, func() bool { return rec.find("stuck workflows", "count", "1") },
		time.Second, 10*time.Millisecond)
}
