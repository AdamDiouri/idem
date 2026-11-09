package chaos

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func cfg(seed int64) PlanConfig {
	return PlanConfig{Seed: seed, Duration: 90 * time.Second, Workers: 4, LeaseTTL: 4 * time.Second}
}

func TestPlan_Deterministic(t *testing.T) {
	require.True(t, reflect.DeepEqual(NewPlan(cfg(1)), NewPlan(cfg(1))))
	require.False(t, reflect.DeepEqual(NewPlan(cfg(1)), NewPlan(cfg(2))))
}

func TestPlan_SortedAndWithinWindow(t *testing.T) {
	c := cfg(3)
	events := NewPlan(c)
	require.NotEmpty(t, events)
	for i, e := range events {
		require.Less(t, e.At, c.Duration*9/10, "event %d", i)
		if i > 0 {
			require.GreaterOrEqual(t, e.At, events[i-1].At)
		}
	}
}

func TestPlan_PausesOutlastTheLease(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		c := cfg(seed)
		for _, e := range NewPlan(c) {
			if e.Kind == PauseWorker {
				require.GreaterOrEqual(t, e.Duration, c.LeaseTTL*3/2)
			}
		}
	}
}

func TestPlan_TargetsValidWorkers(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		c := cfg(seed)
		for _, e := range NewPlan(c) {
			if e.Kind == KillWorker || e.Kind == PauseWorker {
				require.GreaterOrEqual(t, e.Target, 0)
				require.Less(t, e.Target, c.Workers)
			}
		}
	}
}

func TestPlan_CoversEveryKindOverAMinute(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		c := cfg(seed)
		c.Duration = 60 * time.Second
		seen := map[EventKind]bool{}
		for _, e := range NewPlan(c) {
			seen[e.Kind] = true
		}
		for _, k := range AllKinds {
			require.True(t, seen[k], "seed %d misses %s", seed, k)
		}
	}
}

func TestPlan_FaultsAreValid(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		for _, e := range NewPlan(cfg(seed)) {
			if e.Kind == SetProviderFaults {
				require.NotEmpty(t, e.Faults.Rules)
				require.NoError(t, e.Faults.Validate())
				for _, r := range e.Faults.Rules {
					require.GreaterOrEqual(t, r.Probability, 0.1)
					require.LessOrEqual(t, r.Probability, 0.4)
				}
			}
		}
	}
}

// Deferred minor 4: chaos runs several workers and sweepers on one host; they
// must not all try to bind the same metrics port (nothing scrapes them).
func TestWorkerEnv_DisablesMetricsListener(t *testing.T) {
	require.Contains(t, workerEnv("postgres://x", "http://p"), "IDEM_METRICS_ADDR=off")
}
