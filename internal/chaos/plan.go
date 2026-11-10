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
)

// AllKinds lists every disruption kind.
var AllKinds = []EventKind{KillWorker, PauseWorker, RestartAPI, RestartPostgres, SetProviderFaults, ClearProviderFaults}

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
	{KillWorker, 30}, {PauseWorker, 20}, {SetProviderFaults, 25},
	{RestartAPI, 10}, {RestartPostgres, 10}, {ClearProviderFaults, 5},
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
		sort.SliceStable(events, func(i, j int) bool { return events[i].At < events[j].At })
		events = armEveryWebhookFault(rng, events, cfg.Duration)
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

// armEveryWebhookFault makes sure each webhook fault is in force for at least
// minFaultWindow at some point: a missing mode joins the longest-lasting fault
// set that lacks a webhook rule (else the longest one), or, if none lasts long
// enough, a new fault set at the start of the longest quiet stretch. events
// must be sorted; the result is.
func armEveryWebhookFault(rng *rand.Rand, events []Event, end time.Duration) []Event {
	for _, mode := range webhookModes {
		best, bestWindow, covered := -1, time.Duration(0), false
		for i, e := range events {
			if e.Kind != SetProviderFaults {
				continue
			}
			w := faultWindow(events, i, end)
			hasWebhook := false
			for _, r := range e.Faults.Rules {
				if r.Endpoint == webhooksEndpoint {
					hasWebhook = true
					covered = covered || (r.Mode == mode && w >= minFaultWindow)
				}
			}
			// Prefer a long set with no webhook rule yet, so modes don't
			// crowd one set (the first matching rule wins each draw).
			score := w
			if hasWebhook {
				score /= 2
			}
			if score > bestWindow {
				best, bestWindow = i, score
			}
		}
		if covered {
			continue
		}
		if best >= 0 && faultWindow(events, best, end) >= minFaultWindow {
			events[best].Faults.Rules = append(events[best].Faults.Rules, webhookRule(rng, mode))
			continue
		}
		at := longestQuietStart(events, end)
		events = append(events, Event{At: at, Kind: SetProviderFaults, Faults: fakeprovider.Faults{
			Seed: rng.Int64N(math.MaxInt32) + 1, Rules: []fakeprovider.Rule{webhookRule(rng, mode)},
		}})
		sort.SliceStable(events, func(i, j int) bool { return events[i].At < events[j].At })
	}
	return events
}

// longestQuietStart returns the start of the longest stretch with no fault change.
func longestQuietStart(events []Event, end time.Duration) time.Duration {
	prev, bestStart, bestLen := time.Millisecond, time.Millisecond, time.Duration(0)
	for _, e := range events {
		if e.Kind != SetProviderFaults && e.Kind != ClearProviderFaults {
			continue
		}
		if e.At-prev > bestLen {
			bestStart, bestLen = prev, e.At-prev
		}
		prev = e.At + time.Millisecond
	}
	if end-prev > bestLen {
		bestStart = prev
	}
	return bestStart
}
