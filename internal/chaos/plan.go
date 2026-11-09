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
	Target   int                 `json:"target,omitempty"`   // worker index
	Duration time.Duration       `json:"duration,omitempty"` // pauses
	Faults   fakeprovider.Faults `json:"faults,omitzero"`
}

// PlanConfig parameterises NewPlan.
type PlanConfig struct {
	Seed     int64
	Duration time.Duration
	Workers  int
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
}

var endpoints = []string{"charges", "payouts", "refunds"}

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
		f.Rules = append(f.Rules, fakeprovider.Rule{
			Endpoint:    endpoint,
			Mode:        modes[rng.IntN(len(modes))],
			Probability: 0.1 + 0.3*rng.Float64(),
		})
	}
	return f
}
