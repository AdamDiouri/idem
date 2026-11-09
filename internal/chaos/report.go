package chaos

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/AdamDiouri/idem/internal/invariants"
)

// ExecutedEvent is a planned event as it actually happened.
type ExecutedEvent struct {
	Event
	WallTime time.Time `json:"wall_time"`
	Err      string    `json:"error,omitempty"`
}

// Report is a campaign's verdict and evidence.
type Report struct {
	Seed           int64                  `json:"seed"`
	Config         Config                 `json:"config"`
	Events         []ExecutedEvent        `json:"events"`
	Clients        ClientResult           `json:"clients"`
	Outcomes       map[string]int         `json:"outcomes"` // workflow status → count
	NeedsAttention []string               `json:"needs_attention"`
	Violations     []invariants.Violation `json:"violations"`
	Liveness       []invariants.Violation `json:"liveness"`
	Restarts       map[string]int         `json:"restarts"` // process → crash restarts
	SettleTime     time.Duration          `json:"settle_time"`
	Passed         bool                   `json:"passed"`
	Reasons        []string               `json:"reasons"`
}

// Evaluate sets Passed and Reasons. A run passes only if every check holds AND
// it was not vacuous: workflows were accepted, at least one fault event ran,
// and at least one process crashed or was paused.
func (r *Report) Evaluate() {
	r.Reasons = nil
	add := func(format string, args ...any) { r.Reasons = append(r.Reasons, fmt.Sprintf(format, args...)) }

	if n := len(r.Violations); n > 0 {
		add("%d invariant violations", n)
	}
	if n := len(r.Liveness); n > 0 {
		add("%d workflows did not settle (I4)", n)
	}
	if n := len(r.Clients.Conflicts); n > 0 {
		add("%d idempotency keys returned two different workflows", n)
	}
	if n := len(r.Clients.Unfinished); n > 0 {
		add("%d submissions were never accepted", n)
	}
	if n := len(r.Clients.Unexpected); n > 0 {
		add("%d unexpected API responses", n)
	}

	executed, pauses := 0, 0
	for _, e := range r.Events {
		if e.Err == "" {
			executed++
			if e.Kind == PauseWorker {
				pauses++
			}
		}
	}
	restarts := 0
	for _, n := range r.Restarts {
		restarts += n
	}
	switch {
	case len(r.Clients.Accepted) == 0:
		add("vacuous: no workflow was accepted")
	case executed == 0:
		add("vacuous: no fault event was executed")
	case restarts+pauses == 0:
		add("vacuous: no process crashed or was paused")
	}
	r.Passed = len(r.Reasons) == 0
}

// Reproduce returns the command that replays this run's fault plan.
func (r *Report) Reproduce() string {
	c := r.Config
	return fmt.Sprintf("bin/chaos --seed %d --duration %s --workflows %d --workers %d --sweepers %d",
		r.Seed, c.Duration, c.Workflows, c.Workers, c.Sweepers)
}

// Text renders a human-readable summary.
func (r *Report) Text() string {
	var b strings.Builder
	verdict := "FAILED"
	if r.Passed {
		verdict = "PASSED"
	}
	fmt.Fprintf(&b, "chaos run seed %d: %s\n", r.Seed, verdict)
	fmt.Fprintf(&b, "reproduce the fault plan: %s\n", r.Reproduce())
	b.WriteString("(the plan is reproducible; OS scheduling is not)\n\n")

	kinds := map[EventKind]int{}
	failed := 0
	for _, e := range r.Events {
		if e.Err != "" {
			failed++
			continue
		}
		kinds[e.Kind]++
	}
	fmt.Fprintf(&b, "events executed: %d (failed to apply: %d)\n", len(r.Events)-failed, failed)
	for _, k := range AllKinds {
		fmt.Fprintf(&b, "  %-22s %d\n", k, kinds[k])
	}

	fmt.Fprintf(&b, "crash restarts:\n")
	for _, name := range slices.Sorted(maps.Keys(r.Restarts)) {
		fmt.Fprintf(&b, "  %-22s %d\n", name, r.Restarts[name])
	}

	fmt.Fprintf(&b, "clients: submitted %d, accepted %d, malformed rejected %d, behaviors %v\n",
		r.Clients.Submitted, len(r.Clients.Accepted), r.Clients.Rejected, r.Clients.Behaviors)
	fmt.Fprintf(&b, "outcomes: ")
	for _, s := range slices.Sorted(maps.Keys(r.Outcomes)) {
		fmt.Fprintf(&b, "%s=%d ", s, r.Outcomes[s])
	}
	fmt.Fprintf(&b, "\nsettled in: %s\n", r.SettleTime.Round(time.Millisecond))
	fmt.Fprintf(&b, "needs attention: %d\n", len(r.NeedsAttention))
	for _, id := range r.NeedsAttention {
		fmt.Fprintf(&b, "  %s\n", id)
	}

	fmt.Fprintf(&b, "violations: %d\n", len(r.Violations)+len(r.Liveness))
	for _, v := range append(append([]invariants.Violation{}, r.Violations...), r.Liveness...) {
		fmt.Fprintf(&b, "  %s\n", v.String())
	}
	for _, s := range r.Clients.Conflicts {
		fmt.Fprintf(&b, "  conflict: %s\n", s)
	}
	for _, s := range r.Clients.Unexpected {
		fmt.Fprintf(&b, "  unexpected: %s\n", s)
	}
	if len(r.Reasons) > 0 {
		fmt.Fprintf(&b, "\nreasons:\n")
		for _, s := range r.Reasons {
			fmt.Fprintf(&b, "  - %s\n", s)
		}
	}
	return b.String()
}

// Write saves report.json and report.txt into dir.
func (r *Report) Write(dir string) error {
	j, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), j, 0o644); err != nil { //nolint:gosec // report file
		return err
	}
	return os.WriteFile(filepath.Join(dir, "report.txt"), []byte(r.Text()), 0o644) //nolint:gosec // report file
}
