package metrics_test

import (
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/metrics"
)

func TestMetrics_HandlerExposesRegistered(t *testing.T) {
	m := metrics.New()
	m.ObserveAttempt("charge", "succeeded")
	m.ObserveProvider("charge", "success", 15*time.Millisecond)
	m.ObserveLookup("payout", true)
	m.AddReclaimed(2)
	m.SetSnapshot(metrics.Snapshot{
		Workflows:      map[string]int{"RUNNING": 3},
		Steps:          map[[2]string]int{{"charge", "PENDING"}: 1},
		UnsettledIdle:  time.Minute,
		StepsOverLimit: 0,
		Parked:         map[string]int{"permanent_after_in_doubt": 1},
	})
	m.ObserveSubmission("created")

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, name := range []string{
		"idem_step_attempts_total", "idem_provider_request_duration_seconds", "idem_key_retention_lookups_total",
		"idem_leases_reclaimed_total", "idem_workflows", "idem_steps", "idem_unsettled_workflow_max_idle_seconds",
		"idem_steps_over_attempt_limit", "idem_parked_workflows", "idem_workflow_submissions_total", "go_goroutines",
		"idem_inbox_pending", "idem_inbox_oldest_pending_age_seconds", "idem_webhook_conflicts_open", "idem_callbacks_overdue",
	} {
		require.Contains(t, string(body), name)
	}
}

func TestMetrics_SnapshotReplacesOldLabels(t *testing.T) {
	m := metrics.New()
	m.SetSnapshot(metrics.Snapshot{Parked: map[string]int{"attempts_exhausted_in_doubt": 1}})
	m.SetSnapshot(metrics.Snapshot{Parked: map[string]int{}})
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	require.NotContains(t, string(body), `idem_parked_workflows{reason="attempts_exhausted_in_doubt"}`,
		"a resolved reason must disappear, not stay at its last value")
}

func TestMetrics_NilIsANoOp(t *testing.T) {
	var m *metrics.Metrics
	require.NotPanics(t, func() {
		m.ObserveAttempt("charge", "succeeded")
		m.ObserveProvider("charge", "success", time.Millisecond)
		m.ObserveLookup("charge", false)
		m.AddReclaimed(1)
		m.SetSnapshot(metrics.Snapshot{})
		m.ObserveSubmission("created")
	})
}
