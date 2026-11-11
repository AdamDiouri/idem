package chaos

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/fakeshop"
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

// faultWindows maps each SetProviderFaults event to how long it stays in
// force: until the next fault change, or the end of the campaign.
func faultWindows(events []Event, duration time.Duration) map[int]time.Duration {
	out := map[int]time.Duration{}
	for i, e := range events {
		if e.Kind != SetProviderFaults {
			continue
		}
		end := duration
		for _, n := range events[i+1:] {
			if n.Kind == SetProviderFaults || n.Kind == ClearProviderFaults {
				end = n.At
				break
			}
		}
		out[i] = end - e.At
	}
	return out
}

// Slice 2.3 (found by campaigns): a webhook fault armed for under a second
// can meet no payout at all. Each one gets a window of at least 5s.
func TestPlan_EveryWebhookFaultHasALongWindow(t *testing.T) {
	for seed := int64(1); seed <= 30; seed++ {
		events := NewPlan(PlanConfig{Seed: seed, Duration: 90 * time.Second, Workers: 4, APIs: 2, LeaseTTL: 4 * time.Second})
		long := map[string]bool{}
		for i, w := range faultWindows(events, 90*time.Second) {
			if w < minFaultWindow {
				continue
			}
			for _, r := range events[i].Faults.Rules {
				if r.Endpoint == "webhooks" {
					long[r.Mode] = true
				}
			}
		}
		for _, m := range wantWebhookModes {
			require.True(t, long[m], "seed %d: %s never in force for %s", seed, m, minFaultWindow)
		}
	}
}

// Final review C1/C2: runs of a minute or more reserve a webhook-fault block:
// one set per webhook fault, that fault alone, each in force for at least
// minFaultWindow, starting once traffic flows (>= 10% of the run), with no
// other fault change inside it.
func TestPlan_WebhookFaultBlock(t *testing.T) {
	for _, d := range []time.Duration{60 * time.Second, 90 * time.Second, 120 * time.Second, 300 * time.Second} {
		for seed := int64(1); seed <= 2000; seed++ {
			events := NewPlan(PlanConfig{Seed: seed, Duration: d, Workers: 4, APIs: 2, LeaseTTL: 4 * time.Second})
			found := map[string]bool{}
			for i, e := range events {
				if e.Kind != SetProviderFaults || len(e.Faults.Rules) != 1 || e.Faults.Rules[0].Endpoint != "webhooks" {
					continue
				}
				// Planned with slack: executed times drift (a slow event before a
				// window's start), and the verdict measures the executed window.
				if faultWindow(events, i, d) < minFaultWindow+faultWindowSlack || e.At < d/10 || e.At > d*9/10 {
					continue
				}
				found[e.Faults.Rules[0].Mode] = true
			}
			for _, m := range wantWebhookModes {
				require.True(t, found[m], "%s seed %d: no dedicated %s window", d, seed, m)
			}
			kinds := map[EventKind]bool{}
			for _, e := range events {
				kinds[e.Kind] = true
				require.Less(t, e.At, d, "%s seed %d: event past the run", d, seed)
			}
			require.Len(t, kinds, len(AllKinds), "%s seed %d: every disruption kind still occurs", d, seed)
		}
	}
}

// Slice 2.5: in runs of a minute or more, every shop fault gets a window of
// its own, long enough to meet deliveries, inside the campaign.
func TestPlan_ShopFaultBlockCoversEveryMode(t *testing.T) {
	for _, d := range []time.Duration{60 * time.Second, 90 * time.Second, 120 * time.Second, 300 * time.Second} {
		for seed := int64(1); seed <= 500; seed++ {
			events := NewPlan(PlanConfig{Seed: seed, Duration: d, Workers: 4, APIs: 2, Relays: 2, LeaseTTL: 4 * time.Second})
			found := map[string]bool{}
			for i, e := range events {
				if e.Kind != SetShopFaults || len(e.ShopFaults.Rules) != 1 {
					continue
				}
				if shopFaultWindow(events, i, d) < minFaultWindow+faultWindowSlack || e.At < d/10 || e.At > d*9/10 {
					continue
				}
				found[e.ShopFaults.Rules[0].Mode] = true
			}
			for _, m := range fakeshop.Modes {
				require.True(t, found[m], "%s seed %d: no dedicated %s window", d, seed, m)
			}
		}
	}
}

func TestPlan_ShopFaultsAreValidAndRelayTargetsExist(t *testing.T) {
	for seed := int64(1); seed <= 50; seed++ {
		for _, e := range NewPlan(PlanConfig{Seed: seed, Duration: 90 * time.Second, Workers: 4, APIs: 2, Relays: 2, LeaseTTL: 4 * time.Second}) {
			switch e.Kind {
			case SetShopFaults:
				require.NoError(t, e.ShopFaults.Validate())
				require.NotEmpty(t, e.ShopFaults.Rules)
			case KillRelay:
				require.Less(t, e.Target, 2)
			}
		}
	}
}

// Minor fix: a process that never becomes ready shows why, from its own log
// (e.g. a release binary refusing failpoints).
func TestWithLogTail_ShowsTheProcessLog(t *testing.T) {
	log := filepath.Join(t.TempDir(), "api-0.log")
	require.NoError(t, os.WriteFile(log, []byte("starting\nfailpoints requested but this binary was built with -tags nofailpoint\n"), 0o600))
	err := withLogTail(errors.New("api-0: not ready"), log)
	require.ErrorContains(t, err, "api-0: not ready")
	require.ErrorContains(t, err, "built with -tags nofailpoint")
	require.Equal(t, "boom", withLogTail(errors.New("boom"), filepath.Join(t.TempDir(), "missing.log")).Error())
}

// Slice 2.3b (F26): runs of a minute or more restart everything at least once.
func TestNewPlan_LongRunHasRestartAll(t *testing.T) {
	events := NewPlan(PlanConfig{Seed: 7, Duration: 90 * time.Second, Workers: 4, APIs: 2, LeaseTTL: time.Second})
	n := 0
	for _, e := range events {
		if e.Kind == RestartAll {
			n++
		}
	}
	require.Positive(t, n)
}
