package chaos

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeprovider"

	"github.com/AdamDiouri/idem/internal/invariants"
)

// healthy is a report that should pass: traffic, faults and disruptions all
// happened while work was in flight.
func healthy() Report {
	return Report{
		Seed:   42,
		Config: Config{Seed: 42, Duration: 30 * time.Second, Workflows: 10, Workers: 4, Sweepers: 2},
		Events: []ExecutedEvent{
			{Event: Event{Kind: KillWorker}, InFlight: 3},
			{Event: Event{Kind: PauseWorker}, InFlight: 2},
			{Event: Event{Kind: SetProviderFaults}, InFlight: 5},
		},
		Clients:   ClientResult{Submitted: 10, Accepted: map[string]string{"k": "wf"}},
		Restarts:  map[string]int{"w0": 1},
		FaultHits: map[string]int{"charges/error_before_effect": 3},
		Outcomes:  map[string]int{"SUCCEEDED": 1},
	}
}

func TestReport_PassesOnlyWhenEverythingHolds(t *testing.T) {
	r := healthy()
	r.Evaluate()
	require.True(t, r.Passed, "reasons: %v", r.Reasons)

	cases := map[string]func(*Report){
		"violation":  func(r *Report) { r.Violations = []invariants.Violation{{Invariant: "I1", WorkflowID: "wf"}} },
		"stuck":      func(r *Report) { r.Liveness = []invariants.Violation{{Invariant: "I4", WorkflowID: "wf"}} },
		"conflict":   func(r *Report) { r.Clients.Conflicts = []string{"k"} },
		"unfinished": func(r *Report) { r.Clients.Unfinished = []string{"k"} },
		"unexpected": func(r *Report) { r.Clients.Unexpected = []string{"k: status 422"} },
		"failed event": func(r *Report) {
			r.Events = append(r.Events, ExecutedEvent{Event: Event{Kind: RestartPostgres}, Err: "docker: boom"})
		},
		"ack mismatch": func(r *Report) { r.AckMismatches = []string{"k: client saw wf, database has wf2"} },
		"unexpected parking": func(r *Report) {
			r.Parked = []Parked{{WorkflowID: "wf", Step: "charge", Reason: "attempts_exhausted_in_doubt"}}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := healthy()
			mutate(&r)
			r.Evaluate()
			require.False(t, r.Passed)
			require.NotEmpty(t, r.Reasons)
		})
	}
}

func TestReport_VacuousRunFails(t *testing.T) {
	cases := map[string]func(*Report){
		"no accepted workflow": func(r *Report) { r.Clients.Accepted = map[string]string{} },
		"no fault event":       func(r *Report) { r.Events = nil },
		"events hit an idle system": func(r *Report) {
			for i := range r.Events {
				r.Events[i].InFlight = 0
			}
		},
		"no disruption": func(r *Report) {
			r.Restarts = map[string]int{}
			r.Events = []ExecutedEvent{{Event: Event{Kind: SetProviderFaults}, InFlight: 4}}
		},
		"provider faults never fired": func(r *Report) { r.FaultHits = map[string]int{} },
		"failpoints armed but never fired": func(r *Report) {
			r.Config.Failpoints = "after_claim=exit@0.01"
			r.FailpointExits = 0
		},
		"a kind never overlapped work over a minute": func(r *Report) { r.Config.Duration = 90 * time.Second },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := healthy()
			mutate(&r)
			r.Evaluate()
			require.False(t, r.Passed)
			require.Contains(t, strings.Join(r.Reasons, "; "), "vacuous")
		})
	}
}

// A kill or pause aimed at a worker that is already down (e.g. restarting after
// a failpoint exit) is skipped: harmless, not effective, and not a failure.
func TestReport_SkippedEventDoesNotFail(t *testing.T) {
	r := healthy()
	r.Events = append(r.Events, ExecutedEvent{Event: Event{Kind: KillWorker}, Skipped: "worker-1: process is not running", InFlight: 3})
	r.Evaluate()
	require.True(t, r.Passed, "reasons: %v", r.Reasons)
	require.Contains(t, r.Text(), "skipped: 1")
}

func TestReport_ExpectedParkingPasses(t *testing.T) {
	r := healthy()
	r.Parked = []Parked{{WorkflowID: "wf", Step: "payout", Reason: "permanent_after_in_doubt"}}
	r.Evaluate()
	require.True(t, r.Passed, "reasons: %v", r.Reasons)
	require.Contains(t, r.Text(), "payout permanent_after_in_doubt")
}

func TestReport_TextIncludesSeedAndReproduceCommand(t *testing.T) {
	r := healthy()
	r.Evaluate()
	text := r.Text()
	require.Contains(t, text, "seed 42")
	require.Contains(t, text, "PASSED")
	require.Contains(t, text, "bin/chaos --seed 42 --duration 30s --workflows 10 --workers 4 --apis 1 --sweepers 2")
}

func TestReport_TextShowsRecoveryCounts(t *testing.T) {
	r := healthy()
	r.FencedCommits = 3
	r.InDoubtResolved = 7
	r.Evaluate()
	require.True(t, r.Passed, "recovery counts are informational: %v", r.Reasons)
	text := r.Text()
	require.Contains(t, text, "fenced commits: 3")
	require.Contains(t, text, "callback polls: 0 (payouts that waited: 0)")
	r.Webhooks = WebhookCounts{Applied: 3, Duplicates: 1, Deferred: 2}
	require.Contains(t, r.Text(), "webhooks: 3 applied, 1 duplicates, 2 deferred")
	require.Contains(t, text, "in-doubt resolved: 7")
}

// Slice 2.3: every armed failpoint must have fired in a run of a minute or more.
func TestReport_EveryArmedFailpointMustFire(t *testing.T) {
	r := healthy()
	r.Config.Duration = 90 * time.Second
	r.Config.Failpoints = "after_claim=exit@0.1,inbox_before_commit=exit@0.1"
	r.FailpointExits = 2
	r.FailpointFires = map[string]int{"after_claim": 2}
	r.Evaluate()
	require.False(t, r.Passed)
	require.Contains(t, strings.Join(r.Reasons, "\n"), "failpoint inbox_before_commit was armed but never fired")

	r = healthy()
	r.Config.Duration = 90 * time.Second
	r.Config.Failpoints = "after_claim=exit@0.1,inbox_before_commit=exit@0.1"
	r.Config.APIFailpoints = "webhook_after_insert=exit@0.1"
	r.FailpointExits = 3
	r.FailpointFires = map[string]int{"after_claim": 2, "inbox_before_commit": 1}
	r.Evaluate()
	require.Contains(t, strings.Join(r.Reasons, "\n"), "failpoint webhook_after_insert was armed but never fired")

	r.FailpointFires["webhook_after_insert"] = 1
	r.Reasons = nil
	r.Evaluate()
	require.NotContains(t, strings.Join(r.Reasons, "\n"), "failpoint")
}

func TestReport_TextShowsFailpointFires(t *testing.T) {
	r := healthy()
	r.FailpointFires = map[string]int{"inbox_before_commit": 1, "after_claim": 2}
	require.Contains(t, r.Text(), "failpoints fired: after_claim=2 inbox_before_commit=1")
}

// Review focus 5: a webhook fault armed in an effective event but never hit
// proves nothing, so the run is vacuous.
func TestReport_WebhookFaultArmedButNeverFiredIsVacuous(t *testing.T) {
	r := healthy()
	r.Events = append(r.Events, ExecutedEvent{InFlight: 4, Event: Event{Kind: SetProviderFaults,
		Faults: fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "webhooks", Mode: fakeprovider.ModeReorder, Probability: 0.3}}}}})
	r.Evaluate()
	require.Contains(t, strings.Join(r.Reasons, "\n"), "webhook fault reorder was armed but never fired")

	r.FaultHits["webhooks/reorder"] = 2
	r.Reasons = nil
	r.Evaluate()
	require.NotContains(t, strings.Join(r.Reasons, "\n"), "webhook fault")
}

// Slice 2.3: with two replicas, each must have served clients and stored
// webhooks, or the run proves nothing about replicas.
func TestReport_ReplicaWithoutTrafficIsVacuous(t *testing.T) {
	r := healthy()
	r.Config.APIs = 2
	r.Config.Duration = 90 * time.Second // the duplicate rule needs the dedicated dup window (runs >= 1m)
	r.Clients.PerReplica = map[string]int{"api-0": 5}
	r.WebhooksAccepted = map[string]int{"api-0": 3}
	r.WebhooksDuplicate = map[string]int{"api-0": 1}
	r.Evaluate()
	reasons := strings.Join(r.Reasons, "\n")
	require.Contains(t, reasons, "API replica api-1 answered no client request")
	require.Contains(t, reasons, "API replica api-1 accepted no new webhook")
	require.Contains(t, reasons, "API replica api-1 deduplicated no webhook")

	// Final review I3: duplicates alone are not stored webhooks.
	r.Clients.PerReplica["api-1"], r.WebhooksDuplicate["api-1"] = 4, 7
	r.Reasons = nil
	r.Evaluate()
	require.Contains(t, strings.Join(r.Reasons, "\n"), "API replica api-1 accepted no new webhook")

	r.WebhooksAccepted["api-1"] = 2
	r.Reasons = nil
	r.Evaluate()
	require.NotContains(t, strings.Join(r.Reasons, "\n"), "replica")
	require.Contains(t, r.Text(), "api replicas: 2 (requests api-0=5 api-1=4; webhooks accepted api-0=3 api-1=2, duplicates api-0=1 api-1=7)")
}

// Slice 2.3 (found by campaigns): a webhook fault in force for under the
// minimum window had no real chance to fire, so its silence is not vacuity.
func TestReport_ShortWebhookWindowIsNotVacuous(t *testing.T) {
	t0 := time.Now()
	r := healthy()
	r.Events = append(r.Events,
		ExecutedEvent{InFlight: 4, WallTime: t0.Add(5800 * time.Millisecond), Event: Event{At: 5800 * time.Millisecond, Kind: SetProviderFaults,
			Faults: fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "webhooks", Mode: fakeprovider.ModeReorder, Probability: 0.3}}}}},
		ExecutedEvent{InFlight: 4, WallTime: t0.Add(6700 * time.Millisecond), Event: Event{At: 6700 * time.Millisecond, Kind: ClearProviderFaults}})
	r.Evaluate()
	require.NotContains(t, strings.Join(r.Reasons, "\n"), "webhook fault reorder")
}

// Final review C1: in a run of a minute or more, every webhook fault must have
// had a real window (effective, in force >= minFaultWindow on the executed
// timeline) and fired; a fault that never had one is not silently skipped.
func TestReport_EveryWebhookModeNeedsARealWindow(t *testing.T) {
	t0 := time.Now()
	r := healthy()
	r.Config.Duration = 90 * time.Second
	at := 20 * time.Second
	for _, m := range wantWebhookModes {
		if m == fakeprovider.ModeDupWebhook {
			continue // never armed
		}
		r.Events = append(r.Events, ExecutedEvent{InFlight: 5, WallTime: t0.Add(at), Event: Event{At: at, Kind: SetProviderFaults,
			Faults: fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "webhooks", Mode: m, Probability: 0.3}}}}})
		r.FaultHits["webhooks/"+m] = 3
		at += 6 * time.Second
	}
	r.Evaluate()
	reasons := strings.Join(r.Reasons, "\n")
	require.Contains(t, reasons, "webhook fault dup_webhook never had a real window")
	require.NotContains(t, reasons, "webhook fault reorder")

	// A window that is planned long but executed short (a slow event in
	// between) does not count (final review I1).
	r = healthy()
	r.Config.Duration = 90 * time.Second
	r.Events = append(r.Events,
		ExecutedEvent{InFlight: 5, WallTime: t0.Add(20 * time.Second), Event: Event{At: 20 * time.Second, Kind: SetProviderFaults,
			Faults: fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "webhooks", Mode: fakeprovider.ModeReorder, Probability: 0.3}}}}},
		ExecutedEvent{InFlight: 5, WallTime: t0.Add(30*time.Second + 500*time.Millisecond), Event: Event{At: 26 * time.Second, Kind: RestartPostgres}},
		ExecutedEvent{InFlight: 5, WallTime: t0.Add(31 * time.Second), Event: Event{At: 26 * time.Second, Kind: ClearProviderFaults}})
	r.Evaluate()
	require.NotContains(t, strings.Join(r.Reasons, "\n"), "webhook fault reorder never had a real window", "11s executed window counts")
	r.Events[len(r.Events)-1].WallTime = t0.Add(22 * time.Second) // executed: only 2s in force
	r.Reasons = nil
	r.Evaluate()
	require.Contains(t, strings.Join(r.Reasons, "\n"), "webhook fault reorder never had a real window")
}

// Found by campaigns (seed 2): in-flight is sampled at the instant an event
// fires; a window opened right after a database restart sampled 0 while
// traffic flowed through the whole window. Effectiveness of a webhook window
// is judged over the window: any event during it that saw work in flight.
func TestReport_WebhookWindowEffectiveOverTheWindow(t *testing.T) {
	t0 := time.Now()
	r := healthy()
	r.Config.Duration = 90 * time.Second
	r.Events = append(r.Events,
		ExecutedEvent{InFlight: 0, WallTime: t0.Add(22800 * time.Millisecond), Event: Event{At: 22580 * time.Millisecond, Kind: SetProviderFaults,
			Faults: fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "webhooks", Mode: fakeprovider.ModeFastWebhook, Probability: 0.3}}}}},
		ExecutedEvent{InFlight: 16, WallTime: t0.Add(23470 * time.Millisecond), Event: Event{At: 22650 * time.Millisecond, Kind: RestartPostgres}},
		ExecutedEvent{InFlight: 9, WallTime: t0.Add(30590 * time.Millisecond), Event: Event{At: 30580 * time.Millisecond, Kind: ClearProviderFaults}})
	r.FaultHits["webhooks/fast_webhook"] = 19
	r.Evaluate()
	require.NotContains(t, strings.Join(r.Reasons, "\n"), "webhook fault fast_webhook never had a real window")

	// A window during which nothing at all saw work in flight still does not count.
	r.Events[len(r.Events)-2].InFlight = 0
	r.Reasons = nil
	r.Evaluate()
	require.Contains(t, strings.Join(r.Reasons, "\n"), "webhook fault fast_webhook never had a real window")
}

// Minor fixes (slice 2.3 review).
func TestReport_ReproduceIncludesAPIs(t *testing.T) {
	r := healthy()
	r.Config.APIs = 2
	require.Contains(t, r.Reproduce(), "--apis 2")
}

func TestReport_ShortRunWithOnlyAPIFailpointsIsJudged(t *testing.T) {
	r := healthy()
	r.Config.APIFailpoints = "webhook_after_insert=exit@0.1"
	r.FailpointExits = 0
	r.Evaluate()
	require.Contains(t, strings.Join(r.Reasons, "\n"), "failpoints were armed but none fired")
}

func TestReport_TextShowsConflicts(t *testing.T) {
	r := healthy()
	require.Contains(t, r.Text(), "conflicts: 0")
	r.Violations = []invariants.Violation{{Invariant: "I10", WorkflowID: "wf"}, {Invariant: "I1", WorkflowID: "wf"}}
	require.Contains(t, r.Text(), "conflicts: 1")
}

// Found by the chaos-tagged short campaign (flaky): in a run under a minute
// there is no dedicated dup_webhook window, so a replica may legitimately see
// no duplicate; only runs of a minute or more require cross-replica dedup.
func TestReport_ShortRunNeedsNoDuplicatesPerReplica(t *testing.T) {
	r := healthy() // 30s
	r.Config.APIs = 2
	r.Clients.PerReplica = map[string]int{"api-0": 5, "api-1": 4}
	r.WebhooksAccepted = map[string]int{"api-0": 3, "api-1": 2}
	r.WebhooksDuplicate = map[string]int{"api-0": 1}
	r.Evaluate()
	require.NotContains(t, strings.Join(r.Reasons, "\n"), "deduplicated no webhook")
}
