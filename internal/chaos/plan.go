// Package chaos runs idem as real processes under a seeded plan of
// disruptions (kills, pauses, restarts, provider faults) while misbehaving
// clients drive traffic, then checks every invariant (design §14.3).
package chaos

import (
	"math"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
)

// EventKind is one kind of disruption.
type EventKind string

// Disruptions the harness can inject.
const (
	KillWorker          EventKind = "kill_worker"      // SIGKILL; the supervisor restarts it
	PauseWorker         EventKind = "pause_worker"     // SIGSTOP for Duration, then SIGCONT
	RestartAPI          EventKind = "restart_api"      // SIGTERM, then start again
	RestartPostgres     EventKind = "restart_postgres" // docker restart
	SetProviderFaults   EventKind = "set_provider_faults"
	ClearProviderFaults EventKind = "clear_provider_faults"
	// RestartAll is a rolling deploy done badly (F26): every worker, sweeper
	// and API replica gets SIGTERM at once, drains, and starts again.
	RestartAll EventKind = "restart_all"
)

// AllKinds lists every disruption kind.
var AllKinds = []EventKind{KillWorker, PauseWorker, RestartAPI, RestartPostgres, SetProviderFaults, ClearProviderFaults, RestartAll}

// Event is one scheduled disruption.
type Event struct {
	At       time.Duration       `json:"at"`
	Kind     EventKind           `json:"kind"`
	Target   int                 `json:"target"`             // worker index, or API replica for restart_api
	Duration time.Duration       `json:"duration,omitempty"` // pauses
	Faults   fakeprovider.Faults `json:"faults,omitzero"`
}

// PlanConfig parameterises NewPlan.
type PlanConfig struct {
	Seed     int64
	Duration time.Duration
	Workers  int
	APIs     int // API replicas; a RestartAPI event targets one of them
	LeaseTTL time.Duration
}

const meanGap = 3 * time.Second

var kindWeights = []struct {
	kind   EventKind
	weight int
}{
	{KillWorker, 28}, {PauseWorker, 20}, {SetProviderFaults, 25},
	{RestartAPI, 10}, {RestartPostgres, 10}, {ClearProviderFaults, 4}, {RestartAll, 3},
}

var endpointModes = map[string][]string{
	"charges": {fakeprovider.ModeTimeoutBeforeEffect, fakeprovider.ModeTimeoutAfterEffect, fakeprovider.ModeErrorBeforeEffect,
		fakeprovider.ModeErrorAfterEffect, fakeprovider.ModeRateLimit, fakeprovider.ModeDecline},
	"payouts": {fakeprovider.ModeTimeoutBeforeEffect, fakeprovider.ModeTimeoutAfterEffect, fakeprovider.ModeErrorBeforeEffect,
		fakeprovider.ModeErrorAfterEffect, fakeprovider.ModeRateLimit, fakeprovider.ModePayoutFail},
	"refunds": {fakeprovider.ModeTimeoutBeforeEffect, fakeprovider.ModeTimeoutAfterEffect, fakeprovider.ModeErrorBeforeEffect,
		fakeprovider.ModeErrorAfterEffect, fakeprovider.ModeRateLimit},
	// Slice 2.3: provider webhooks (drawn once per payout, at acceptance).
	"webhooks": webhookModes,
}

var webhookModes = []string{fakeprovider.ModeDupWebhook, fakeprovider.ModeFastWebhook, fakeprovider.ModeDelayWebhook,
	fakeprovider.ModeReorder, fakeprovider.ModeDropWebhook, fakeprovider.ModeBadSignature}

var endpoints = []string{"charges", "payouts", "refunds", "webhooks"}

// NewPlan returns the disruption schedule for cfg. It is a pure function of
// cfg: the same seed always yields the same plan (OS scheduling during the run
// is not reproducible, the plan is). Events fall in the first 90% of the run so
// the system has time to settle; over a minute or more, every kind occurs.
func NewPlan(cfg PlanConfig) []Event {
	rng := rand.New(rand.NewPCG(uint64(cfg.Seed), uint64(cfg.Seed))) //nolint:gosec // reproducible chaos, not security
	window := cfg.Duration * 9 / 10

	var events []Event
	for at := expGap(rng); at < window; at += expGap(rng) {
		events = append(events, newEvent(rng, cfg, at, pickKind(rng)))
	}

	if cfg.Duration >= time.Minute {
		seen := map[EventKind]bool{}
		for _, e := range events {
			seen[e.Kind] = true
		}
		for _, k := range AllKinds {
			if !seen[k] {
				at := time.Duration(rng.Int64N(int64(window)))
				events = append(events, newEvent(rng, cfg, at, k))
			}
		}
		events = webhookFaultBlock(rng, events, cfg.Duration)
	}

	sort.SliceStable(events, func(i, j int) bool { return events[i].At < events[j].At })
	return events
}

func expGap(rng *rand.Rand) time.Duration {
	return time.Duration(rng.ExpFloat64() * float64(meanGap))
}

func pickKind(rng *rand.Rand) EventKind {
	n := rng.IntN(100)
	for _, kw := range kindWeights {
		if n < kw.weight {
			return kw.kind
		}
		n -= kw.weight
	}
	return KillWorker
}

func newEvent(rng *rand.Rand, cfg PlanConfig, at time.Duration, kind EventKind) Event {
	e := Event{At: at, Kind: kind}
	switch kind {
	case KillWorker:
		e.Target = rng.IntN(max(cfg.Workers, 1))
	case RestartAPI:
		e.Target = rng.IntN(max(cfg.APIs, 1)) // one replica at a time
	case PauseWorker:
		e.Target = rng.IntN(max(cfg.Workers, 1))
		// 1.5–2.5 lease TTLs: the lease always expires while the worker is frozen.
		e.Duration = time.Duration(float64(cfg.LeaseTTL) * (1.5 + rng.Float64()))
	case SetProviderFaults:
		e.Faults = randomFaults(rng)
	}
	return e
}

func randomFaults(rng *rand.Rand) fakeprovider.Faults {
	f := fakeprovider.Faults{Seed: rng.Int64N(math.MaxInt32) + 1}
	for range 1 + rng.IntN(3) {
		endpoint := endpoints[rng.IntN(len(endpoints))]
		modes := endpointModes[endpoint]
		mode := modes[rng.IntN(len(modes))]
		if endpoint == webhooksEndpoint {
			f.Rules = append(f.Rules, webhookRule(rng, mode))
			continue
		}
		f.Rules = append(f.Rules, fakeprovider.Rule{
			Endpoint:    endpoint,
			Mode:        mode,
			Probability: 0.1 + 0.3*rng.Float64(),
		})
	}
	return f
}

const webhooksEndpoint = "webhooks"

// webhookRule is a webhook fault, likely enough (0.2–0.5 per payout) to meet
// payouts that settle while it is armed.
func webhookRule(rng *rand.Rand, mode string) fakeprovider.Rule {
	return fakeprovider.Rule{Endpoint: webhooksEndpoint, Mode: mode, Probability: 0.2 + 0.3*rng.Float64()}
}

// minFaultWindow is how long a fault set must stay in force before a fault in
// it can be expected to have fired: a webhook fault is drawn once per payout,
// at acceptance, so a sub-second window may meet no payout at all.
const minFaultWindow = 5 * time.Second

// faultWindowSlack pads each planned webhook-fault window beyond
// minFaultWindow: events run one after another, so a slow one (a database
// restart) delays the next window's start, and the verdict measures the
// executed window. Found by campaigns: windows planned at exactly 5s ran 4.99s.
const faultWindowSlack = 3 * time.Second

// faultWindow is how long the fault set of events[i] stays in force: until the
// next fault change, or the end of the campaign. events must be sorted.
func faultWindow(events []Event, i int, end time.Duration) time.Duration {
	for _, n := range events[i+1:] {
		if n.Kind == SetProviderFaults || n.Kind == ClearProviderFaults {
			return n.At - events[i].At
		}
	}
	return end - events[i].At
}

// webhookFaultBlock reserves, in runs of a minute or more, one window of
// minFaultWindow + faultWindowSlack per webhook fault, back to back: each holds that fault alone
// (so no other rule wins its draws), starts once traffic flows (at least 10%
// into the run) and ends inside the campaign window. Any other fault change
// that would fall inside the block is dropped, so no window is cut short.
// Because the block itself sets provider faults, every disruption kind still
// occurs; a ClearProviderFaults is re-added after the block if the plan lost
// its only one.
func webhookFaultBlock(rng *rand.Rand, events []Event, duration time.Duration) []Event {
	slot := minFaultWindow + faultWindowSlack
	block := time.Duration(len(webhookModes)) * slot
	earliest, latest := duration/10, duration*9/10-block
	latest = max(latest, earliest)
	start := earliest + time.Duration(rng.Int64N(int64(latest-earliest)+1))
	end := start + block

	kept := events[:0:0]
	clears := 0
	for _, e := range events {
		faultChange := e.Kind == SetProviderFaults || e.Kind == ClearProviderFaults
		if faultChange && e.At >= start && e.At < end {
			continue
		}
		if e.Kind == ClearProviderFaults {
			clears++
		}
		kept = append(kept, e)
	}
	for i, mode := range webhookModes {
		kept = append(kept, Event{At: start + time.Duration(i)*slot, Kind: SetProviderFaults, Faults: fakeprovider.Faults{
			Seed: rng.Int64N(math.MaxInt32) + 1, Rules: []fakeprovider.Rule{webhookRule(rng, mode)},
		}})
	}
	if clears == 0 {
		kept = append(kept, Event{At: end, Kind: ClearProviderFaults})
	}
	sort.SliceStable(kept, func(i, j int) bool { return kept[i].At < kept[j].At })
	return kept
}
