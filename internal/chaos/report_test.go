package chaos

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	require.Contains(t, text, "bin/chaos --seed 42 --duration 30s --workflows 10 --workers 4 --sweepers 2")
}

func TestReport_TextShowsRecoveryCounts(t *testing.T) {
	r := healthy()
	r.FencedCommits = 3
	r.InDoubtResolved = 7
	r.Evaluate()
	require.True(t, r.Passed, "recovery counts are informational: %v", r.Reasons)
	text := r.Text()
	require.Contains(t, text, "fenced commits: 3")
	require.Contains(t, text, "in-doubt resolved: 7")
}
