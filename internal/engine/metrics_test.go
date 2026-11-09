package engine_test

import (
	"context"
	"log/slog"
	"regexp"
	"sync"
	"testing"
	"time"

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
