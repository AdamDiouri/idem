package chaos

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
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
					lo, hi := 0.1, 0.4
					if r.Endpoint == "webhooks" { // slice 2.3: likelier, so they meet settling payouts
						lo, hi = 0.2, 0.5
					}
					require.GreaterOrEqual(t, r.Probability, lo)
					require.LessOrEqual(t, r.Probability, hi)
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

// Slice 2.1: payouts settle (300ms) well inside the poll budget, so a healthy
// run never parks a payout as callback_unresolved.
func TestWorkerEnv_SetsCallbackTimings(t *testing.T) {
	env := workerEnv("postgres://x", "http://p")
	require.Contains(t, env, "IDEM_CALLBACK_DEADLINE=500ms")
	require.Contains(t, env, "IDEM_CALLBACK_MAX_POLLS=30")
	require.Contains(t, env, "IDEM_CALLBACK_MAX_WAIT=30s")
	require.Contains(t, env, "IDEM_CALLBACK_POLL_CAP=1s")
}

// Chaos compresses every timing about 10x; a provider that hangs for 30s while
// the client gives up after 1s is harsher than any real provider and parked
// healthy payouts. The hang is compressed too.
func TestProviderEnv_CompressesHang(t *testing.T) {
	env := providerEnv(9999, []int{8888})
	require.Contains(t, env, "FAKEPROVIDER_ADDR=127.0.0.1:9999")
	require.Contains(t, env, "FAKEPROVIDER_PAYOUT_SETTLE=300ms")
	require.Contains(t, env, "FAKEPROVIDER_HANG=3s")
}

// Slice 2.2: the chaos provider sends signed webhooks to the chaos API, which
// shares the secret.
func TestProviderEnv_SendsWebhooksToAPI(t *testing.T) {
	env := providerEnv(1, []int{2, 3})
	require.Contains(t, env, "FAKEPROVIDER_WEBHOOK_URL=http://127.0.0.1:2/webhooks/provider,http://127.0.0.1:3/webhooks/provider")
	require.Contains(t, env, "FAKEPROVIDER_WEBHOOK_SECRET="+webhookSecret)
	require.Contains(t, apiEnv("postgres://x", 2, "webhook_after_insert=exit@0.02"), "IDEM_PROVIDER_WEBHOOK_SECRETS="+webhookSecret)
	require.Contains(t, apiEnv("postgres://x", 2, "webhook_after_insert=exit@0.02"), "IDEM_FAILPOINTS=webhook_after_insert=exit@0.02")
	require.GreaterOrEqual(t, len(webhookSecret), 16)
}

var wantWebhookModes = []string{fakeprovider.ModeDupWebhook, fakeprovider.ModeFastWebhook, fakeprovider.ModeDelayWebhook,
	fakeprovider.ModeReorder, fakeprovider.ModeDropWebhook, fakeprovider.ModeBadSignature}

// Slice 2.3: runs of a minute or more inject every webhook fault.
func TestPlan_IncludesEveryWebhookFaultInLongRuns(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		seen := map[string]bool{}
		for _, e := range NewPlan(PlanConfig{Seed: seed, Duration: 90 * time.Second, Workers: 4, LeaseTTL: 4 * time.Second}) {
			for _, r := range e.Faults.Rules {
				if r.Endpoint == "webhooks" {
					seen[r.Mode] = true
				}
			}
		}
		for _, m := range wantWebhookModes {
			require.True(t, seen[m], "seed %d: no %s", seed, m)
		}
	}
}

func TestPlan_WebhookRulesTargetWebhooksOnly(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		for _, e := range NewPlan(PlanConfig{Seed: seed, Duration: 90 * time.Second, Workers: 4, LeaseTTL: 4 * time.Second}) {
			require.NoError(t, e.Faults.Validate(), "seed %d", seed)
		}
	}
}

// Slice 2.3: an API restart hits one replica, and over many seeds both.
func TestPlan_RestartAPITargetsOneReplica(t *testing.T) {
	targets := map[int]bool{}
	for seed := int64(1); seed <= 20; seed++ {
		for _, e := range NewPlan(PlanConfig{Seed: seed, Duration: 90 * time.Second, Workers: 4, APIs: 2, LeaseTTL: 4 * time.Second}) {
			if e.Kind == RestartAPI {
				require.GreaterOrEqual(t, e.Target, 0)
				require.Less(t, e.Target, 2)
				targets[e.Target] = true
			}
		}
	}
	require.Len(t, targets, 2)
}
