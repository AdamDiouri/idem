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
	// Skipped explains an event that could not apply harmlessly: a kill or
	// pause aimed at a worker that was already down (restarting). Not a failure.
	Skipped string `json:"skipped,omitempty"`
	// InFlight is how many workflows were not yet settled when the event
	// fired. An event on an idle system proves nothing.
	InFlight int `json:"in_flight"`
}

// effective reports whether the event happened and met work in flight.
func (e ExecutedEvent) effective() bool { return e.Err == "" && e.Skipped == "" && e.InFlight > 0 }

// Parked is a workflow left for a human, with the reason.
type Parked struct {
	WorkflowID string `json:"workflow_id"`
	Step       string `json:"step"`
	Reason     string `json:"reason"`
}

// expectedParking lists the parking reasons a healthy chaos run can produce.
// Anything else is a design smell (spec §13) and fails the run.
var expectedParking = map[string]bool{
	"permanent_after_in_doubt": true, // pivot safety rule
}

// Report is a campaign's verdict and evidence.
type Report struct {
	Seed           int64           `json:"seed"`
	Config         Config          `json:"config"`
	Events         []ExecutedEvent `json:"events"`
	Clients        ClientResult    `json:"clients"`
	Outcomes       map[string]int  `json:"outcomes"` // workflow status → count
	NeedsAttention []string        `json:"needs_attention"`
	Parked         []Parked        `json:"parked"`
	AckMismatches  []string        `json:"ack_mismatches"` // accepted IDs the database disagrees with
	FaultHits      map[string]int  `json:"fault_hits"`     // provider faults that actually fired
	FailpointExits int             `json:"failpoint_exits"`
	// Recovery evidence (informational): completions discarded because the
	// worker had lost its lease, and steps that succeeded after being IN_DOUBT.
	FencedCommits   int                    `json:"fenced_commits"`
	InDoubtResolved int                    `json:"in_doubt_resolved"`
	Violations      []invariants.Violation `json:"violations"`
	Liveness        []invariants.Violation `json:"liveness"`
	Restarts        map[string]int         `json:"restarts"` // process → crash restarts
	SettleTime      time.Duration          `json:"settle_time"`
	Passed          bool                   `json:"passed"`
	Reasons         []string               `json:"reasons"`
}

// Evaluate sets Passed and Reasons. A run passes only if every check holds AND
// it was not vacuous: workflows were accepted, faults fired while work was in
// flight (each kind, for runs of a minute or more), provider faults actually
// hit requests, armed failpoints actually fired, and processes crashed or froze.
func (r *Report) Evaluate() {
	r.Reasons = nil
	add := func(format string, args ...any) { r.Reasons = append(r.Reasons, fmt.Sprintf(format, args...)) }

	if n := len(r.Violations); n > 0 {
		add("%d invariant violations", n)
	}
	if n := len(r.Liveness); n > 0 {
		add("%d workflows or steps did not settle (I4)", n)
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
	if n := len(r.AckMismatches); n > 0 {
		add("%d accepted workflows disagree with the database", n)
	}
	unexpected := 0
	for _, p := range r.Parked {
		if !expectedParking[p.Reason] {
			unexpected++
		}
	}
	if unexpected > 0 {
		add("%d workflows parked for unexpected reasons", unexpected)
	}

	failed, pauses := 0, 0
	effective := map[EventKind]int{}
	for _, e := range r.Events {
		if e.Err != "" {
			failed++
			continue
		}
		if e.Skipped != "" {
			continue
		}
		if e.Kind == PauseWorker {
			pauses++
		}
		if e.effective() {
			effective[e.Kind]++
		}
	}
	if failed > 0 {
		add("%d events failed to apply", failed)
	}

	restarts := 0
	for _, n := range r.Restarts {
		restarts += n
	}
	hits := 0
	for _, n := range r.FaultHits {
		hits += n
	}
	totalEffective := 0
	for _, n := range effective {
		totalEffective += n
	}

	switch {
	case len(r.Clients.Accepted) == 0:
		add("vacuous: no workflow was accepted")
	case totalEffective == 0:
		add("vacuous: no fault event overlapped work in flight")
	case restarts+pauses == 0:
		add("vacuous: no process crashed or was paused")
	}
	if effective[SetProviderFaults] > 0 && hits == 0 {
		add("vacuous: provider faults were set but never fired")
	}
	if r.Config.Failpoints != "" && r.FailpointExits == 0 {
		add("vacuous: failpoints were armed but none fired (binary built with -tags nofailpoint?)")
	}
	if r.Config.Duration >= time.Minute {
		for _, k := range AllKinds {
			if effective[k] == 0 {
				add("vacuous: no %s event overlapped work in flight", k)
			}
		}
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

	kinds, effective := map[EventKind]int{}, map[EventKind]int{}
	failed, skipped := 0, 0
	for _, e := range r.Events {
		if e.Err != "" {
			failed++
			continue
		}
		if e.Skipped != "" {
			skipped++
			continue
		}
		kinds[e.Kind]++
		if e.effective() {
			effective[e.Kind]++
		}
	}
	fmt.Fprintf(&b, "events executed: %d (failed to apply: %d, skipped: %d); effective = met work in flight\n",
		len(r.Events)-failed-skipped, failed, skipped)
	for _, k := range AllKinds {
		fmt.Fprintf(&b, "  %-22s %d (effective %d)\n", k, kinds[k], effective[k])
	}
	fmt.Fprintf(&b, "provider fault hits: %v\n", r.FaultHits)
	fmt.Fprintf(&b, "failpoint exits: %d\n", r.FailpointExits)
	fmt.Fprintf(&b, "fenced commits: %d\nin-doubt resolved: %d\n", r.FencedCommits, r.InDoubtResolved)

	fmt.Fprintf(&b, "crash restarts:\n")
	for _, name := range slices.Sorted(maps.Keys(r.Restarts)) {
		fmt.Fprintf(&b, "  %-22s %d\n", name, r.Restarts[name])
	}

	fmt.Fprintf(&b, "clients: submitted %d, accepted %d, malformed rejected %d, retried attempts %d, behaviors %v\n",
		r.Clients.Submitted, len(r.Clients.Accepted), r.Clients.Rejected, r.Clients.Retries, r.Clients.Behaviors)
	fmt.Fprintf(&b, "outcomes: ")
	for _, s := range slices.Sorted(maps.Keys(r.Outcomes)) {
		fmt.Fprintf(&b, "%s=%d ", s, r.Outcomes[s])
	}
	fmt.Fprintf(&b, "\nsettled in: %s\n", r.SettleTime.Round(time.Millisecond))
	fmt.Fprintf(&b, "needs attention: %d\n", len(r.NeedsAttention))
	for _, p := range r.Parked {
		fmt.Fprintf(&b, "  %s %s %s\n", p.WorkflowID, p.Step, p.Reason)
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
	for _, s := range r.AckMismatches {
		fmt.Fprintf(&b, "  ack mismatch: %s\n", s)
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
