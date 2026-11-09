package chaos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/invariants"
)

// healthy is a report that should pass: traffic, faults and disruptions all happened.
func healthy() Report {
	return Report{
		Seed:     42,
		Config:   Config{Seed: 42, Duration: 90 * time.Second, Workflows: 10, Workers: 4, Sweepers: 2},
		Events:   []ExecutedEvent{{Event: Event{Kind: KillWorker}}, {Event: Event{Kind: PauseWorker}}, {Event: Event{Kind: SetProviderFaults}}},
		Clients:  ClientResult{Submitted: 10, Accepted: map[string]string{"k": "wf"}},
		Restarts: map[string]int{"w0": 1},
		Outcomes: map[string]int{"SUCCEEDED": 1},
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
		"only failed events":   func(r *Report) { r.Events = []ExecutedEvent{{Event: Event{Kind: KillWorker}, Err: "boom"}} },
		"no disruption": func(r *Report) {
			r.Restarts = map[string]int{}
			r.Events = []ExecutedEvent{{Event: Event{Kind: SetProviderFaults}}}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := healthy()
			mutate(&r)
			r.Evaluate()
			require.False(t, r.Passed)
			require.Contains(t, r.Reasons[len(r.Reasons)-1], "vacuous")
		})
	}
}

func TestReport_TextIncludesSeedAndReproduceCommand(t *testing.T) {
	r := healthy()
	r.Evaluate()
	text := r.Text()
	require.Contains(t, text, "seed 42")
	require.Contains(t, text, "PASSED")
	require.Contains(t, text, "bin/chaos --seed 42 --duration 1m30s --workflows 10 --workers 4 --sweepers 2")
}
