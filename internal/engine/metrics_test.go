package engine_test

import (
	"context"
	"log/slog"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/metrics"
)

func withMetrics(m *metrics.Metrics) func(*engine.WorkerOptions) {
	return func(o *engine.WorkerOptions) { o.Metrics = m }
}

func TestWorker_CountsOutcomes(t *testing.T) {
	h := newHarness(t)
	m := metrics.New()
	submitTo(t, h, "cus_decline_1", "mer_1")
	h.drain(t, h.workerWith("w1", withMetrics(m), h.chargeStep()))
	submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", withMetrics(m), h.chargeStep()))

	require.Equal(t, 1.0, testutil.ToFloat64(m.StepAttempts.WithLabelValues("charge", "failed_permanent")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.StepAttempts.WithLabelValues("charge", "succeeded")))
	require.Equal(t, 2, testutil.CollectAndCount(m.ProviderDuration), "one latency series per class seen")
}

func TestWorker_CountsFencedCommits(t *testing.T) {
	h := newHarness(t)
	m := metrics.New()
	submitTo(t, h, "cus_1", "mer_1")
	action, entered, release := failpoint.BlockOnce()
	failpoint.Enable(engine.FPAfterProviderCall, action)
	defer release()

	var wg sync.WaitGroup
	wg.Go(func() { _, _ = h.workerWith("wA", withMetrics(m), h.chargeStep()).RunOnce(context.Background()) })
	<-entered
	time.Sleep(pastLease)
	require.Equal(t, int64(1), h.reclaim(t))
	require.Equal(t, "succeeded", h.runOnce(t, h.workerWith("wB", withMetrics(m), h.chargeStep())).Outcome)
	release()
	wg.Wait()

	require.Equal(t, 1.0, testutil.ToFloat64(m.StepAttempts.WithLabelValues("charge", "fenced")))
}

func TestSweeper_ExportsGauges(t *testing.T) {
	h := newHarness(t)
	m := metrics.New()
	parkPayoutAfterDoubt(t, h)
	old := submitTo(t, h, "cus_2", "mer_2")
	_, err := h.pool.Exec(context.Background(),
		`UPDATE workflows SET status = 'RUNNING', updated_at = now() - interval '1 hour' WHERE id = $1`, old)
	require.NoError(t, err)

	_, _ = runInBackground(t, func(ctx context.Context) error {
		return engine.RunSweeper(ctx, h.pool, engine.SweeperOptions{
			Interval: 10 * time.Millisecond, StuckAfter: time.Hour, Logger: slog.New(slog.DiscardHandler), Metrics: m,
		})
	})
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(m.Parked.WithLabelValues(engine.DeadPermanentAfterInDoubt)) == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, testutil.ToFloat64(m.UnsettledIdle), 3500.0)
	require.Equal(t, 1.0, testutil.ToFloat64(m.Workflows.WithLabelValues("NEEDS_ATTENTION")))
	require.Equal(t, 0.0, testutil.ToFloat64(m.StepsOverLimit))
}

// Slice 2.1: polls are counted by result.
func TestWorker_CountsPolls(t *testing.T) {
	h := asyncHarness(t, 200*time.Millisecond)
	m := metrics.New()
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.runUntil(t, h.workerWith("w1", func(o *engine.WorkerOptions) {
		o.Metrics, o.CallbackDeadline, o.MaxPolls = m, 0, 1000
	}, h.allSteps()...), wf, "SUCCEEDED")
	require.GreaterOrEqual(t, testutil.ToFloat64(m.CallbackPolls.WithLabelValues("payout", "pending")), 1.0)
	require.Equal(t, 1.0, testutil.ToFloat64(m.CallbackPolls.WithLabelValues("payout", "paid")))
	// The send and every pending poll end awaiting the callback.
	require.GreaterOrEqual(t, testutil.ToFloat64(m.StepAttempts.WithLabelValues("payout", "awaiting_callback")), 2.0)
}

// sweepOnce runs a sweeper until it has exported one snapshot.
func sweepOnce(t *testing.T, h harness) *metrics.Metrics {
	t.Helper()
	m := metrics.New()
	cancel, done := runInBackground(t, func(ctx context.Context) error {
		return engine.RunSweeper(ctx, h.pool, engine.SweeperOptions{
			Interval: 10 * time.Millisecond, StuckAfter: time.Hour, Logger: slog.New(slog.DiscardHandler), Metrics: m,
		})
	})
	require.Eventually(t, func() bool { return testutil.ToFloat64(m.LastSnapshot) > 0 }, 2*time.Second, 10*time.Millisecond)
	cancel()
	<-done
	return m
}

// Deferred minor 2: an operator retried a workflow parked hours ago. It is
// progressing again, so it must not count as stuck (the gauge used to measure
// age since creation, so IdemWorkflowsStuck paged at once).
func TestSweeper_RetriedOldWorkflowIsNotIdle(t *testing.T) {
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_1")
	_, err := h.pool.Exec(context.Background(),
		`UPDATE workflows SET status = 'RUNNING', created_at = now() - interval '3 hours' WHERE id = $1`, wf)
	require.NoError(t, err)

	require.Less(t, testutil.ToFloat64(sweepOnce(t, h).UnsettledIdle), 60.0)
}

// A step attempt is progress even when the workflow's status doesn't change
// (a retrying charge): only a workflow nobody works on is idle.
func TestSweeper_StepAttemptCountsAsProgress(t *testing.T) {
	h := newHarness(t)
	h.setFaults(charges(fakeprovider.ModeRateLimit, 1))
	wf := submitTo(t, h, "cus_1", "mer_1")
	_, err := h.pool.Exec(context.Background(),
		`UPDATE workflows SET status = 'RUNNING', updated_at = now() - interval '1 hour' WHERE id = $1`, wf)
	require.NoError(t, err)
	require.Greater(t, testutil.ToFloat64(sweepOnce(t, h).UnsettledIdle), 3500.0, "nobody worked on it for an hour")

	require.True(t, h.runOnce(t, h.worker("w1")).Claimed)
	require.Equal(t, "RUNNING", h.workflowStatus(t, wf), "the attempt did not change the workflow")
	require.Less(t, testutil.ToFloat64(sweepOnce(t, h).UnsettledIdle), 60.0)
}

// Review finding I1: a crash-looping step (claimed, then abandoned when its
// lease is reclaimed, again and again) is not progress. Counting every claim
// as progress made IdemWorkflowsStuck blind to exactly the loop it exists for.
func TestSweeper_CrashLoopIsNotProgress(t *testing.T) {
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_1")
	_, err := h.pool.Exec(context.Background(),
		`UPDATE workflows SET status = 'RUNNING', updated_at = now() - interval '1 hour' WHERE id = $1`, wf)
	require.NoError(t, err)
	for range 3 {
		mustClaim(t, h.pool, "crashy", 10*time.Millisecond) // the worker dies before finishing
		time.Sleep(30 * time.Millisecond)
		require.EqualValues(t, 1, h.reclaim(t))
	}
	require.Greater(t, testutil.ToFloat64(sweepOnce(t, h).UnsettledIdle), 3500.0)
}

// Fix before merge: a payout waiting for a poll that is not due yet is healthy,
// however long the wait (bank payouts take hours). Overdue waits count as idle.
func TestSweeper_ScheduledCallbackWaitIsNotIdle(t *testing.T) {
	for name, tc := range map[string]struct {
		deadline string
		idle     bool
	}{
		"poll due in 30m": {"30 minutes", false},
		"poll overdue 1h": {"-1 hour", true},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t)
			wf := submitTo(t, h, "cus_1", "mer_1")
			step := scalar[uuid.UUID](t, h.pool, `SELECT id FROM steps WHERE workflow_id = $1`, wf)
			_, err := h.pool.Exec(ctx, `UPDATE workflows SET status = 'RUNNING', updated_at = now() - interval '2 hours' WHERE id = $1`, wf)
			require.NoError(t, err)
			_, err = h.pool.Exec(ctx, `UPDATE steps SET status = 'AWAITING_CALLBACK', provider_ref = 'po_x', attempt_count = 1,
				callback_deadline = now() + $2::interval, run_after = now() + $2::interval WHERE id = $1`, step, tc.deadline)
			require.NoError(t, err)
			_, err = h.pool.Exec(ctx, `INSERT INTO step_attempts (step_id, attempt_no, lease_epoch, worker_id, outcome, started_at, finished_at)
				VALUES ($1, 1, 1, 'w0', 'AWAITING_CALLBACK', now() - interval '2 hours', now() - interval '2 hours')`, step)
			require.NoError(t, err)

			idle := testutil.ToFloat64(sweepOnce(t, h).UnsettledIdle)
			if tc.idle {
				require.Greater(t, idle, 3500.0)
			} else {
				require.Less(t, idle, 60.0)
			}
		})
	}
}

// Slice 2.2: the sweeper exports the inbox backlog, open conflicts and
// overdue callbacks.
func TestSweeper_ExportsInboxAndCallbackGauges(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_1")
	step := scalar[uuid.UUID](t, h.pool, `SELECT id FROM steps WHERE workflow_id = $1`, wf)
	_, err := h.pool.Exec(ctx, `UPDATE workflows SET status = 'RUNNING' WHERE id = $1`, wf)
	require.NoError(t, err)
	_, err = h.pool.Exec(ctx, `UPDATE steps SET status = 'AWAITING_CALLBACK', provider_ref = 'po_x',
		callback_deadline = now() - interval '10 minutes', run_after = now() + interval '1 hour' WHERE id = $1`, step)
	require.NoError(t, err)
	_, err = h.pool.Exec(ctx, `INSERT INTO webhook_inbox (provider_event_id, type, payload, received_at) VALUES
		('evt_pending', 'payout.paid', '{}', now() - interval '1 hour')`)
	require.NoError(t, err)
	_, err = h.pool.Exec(ctx, `INSERT INTO webhook_inbox (provider_event_id, type, payload, status, step_id) VALUES
		('evt_conflict', 'payout.paid', '{}', 'CONFLICT', $1)`, step)
	require.NoError(t, err)

	m := sweepOnce(t, h)
	require.Equal(t, 1.0, testutil.ToFloat64(m.InboxPending))
	require.GreaterOrEqual(t, testutil.ToFloat64(m.InboxOldestPending), 3500.0)
	require.Equal(t, 1.0, testutil.ToFloat64(m.ConflictsOpen))
	require.Equal(t, 1.0, testutil.ToFloat64(m.CallbacksOverdue))
}

// Review focus: labels must stay bounded — no IDs, keys or error text.
func TestMetrics_NoUnboundedLabels(t *testing.T) {
	h := newHarness(t)
	m := metrics.New()
	parkPayoutAfterDoubt(t, h)
	submitTo(t, h, "cus_decline_1", "mer_1")
	submitTo(t, h, "cus_1", "mer_invalid_2")
	h.drain(t, h.workerWith("w1", withMetrics(m), h.allSteps()...))
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = engine.RunSweeper(ctx, h.pool, engine.SweeperOptions{Interval: 10 * time.Millisecond, StuckAfter: time.Hour,
			Logger: slog.New(slog.DiscardHandler), Metrics: m})
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()

	families, err := m.Gatherer().Gather()
	require.NoError(t, err)
	bounded := regexp.MustCompile(`^[A-Za-z_]{1,40}$`)
	seen := 0
	for _, f := range families {
		if len(f.GetName()) < 5 || f.GetName()[:5] != "idem_" {
			continue
		}
		for _, metric := range f.GetMetric() {
			for _, l := range metric.GetLabel() {
				seen++
				require.Regexp(t, bounded, l.GetValue(), "%s{%s=%q}", f.GetName(), l.GetName(), l.GetValue())
			}
		}
	}
	require.Positive(t, seen)
}

// Review finding 5: finished steps never count as over their attempt limit
// (a crash on the last attempt leaves attempt_count = max + 1 forever).
func TestSweeper_OverLimitIgnoresFinishedSteps(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	m := metrics.New()
	done := submitTo(t, h, "cus_1", "mer_1")
	stuck := submitTo(t, h, "cus_2", "mer_2")
	_, err := h.pool.Exec(ctx, `UPDATE steps SET status = 'SUCCEEDED', attempt_count = 2, max_attempts = 1 WHERE workflow_id = $1`, done)
	require.NoError(t, err)
	_, err = h.pool.Exec(ctx, `UPDATE steps SET status = 'IN_DOUBT', attempt_count = 5, max_attempts = 1 WHERE workflow_id = $1`, stuck)
	require.NoError(t, err)

	_, _ = runInBackground(t, func(ctx context.Context) error {
		return engine.RunSweeper(ctx, h.pool, engine.SweeperOptions{Interval: 10 * time.Millisecond, StuckAfter: time.Hour,
			Logger: slog.New(slog.DiscardHandler), Metrics: m})
	})
	require.Eventually(t, func() bool { return testutil.ToFloat64(m.LastSnapshot) > 0 }, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, 1.0, testutil.ToFloat64(m.StepsOverLimit), "only the unfinished step counts")
}
