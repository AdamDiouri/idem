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
	FailpointFires map[string]int  `json:"failpoint_fires"` // name → times it fired (from process logs)
	// Per API replica: provider webhooks it stored as new, and those it
	// recognised as duplicates (often of an event another replica stored).
	WebhooksAccepted  map[string]int `json:"webhooks_accepted"`
	WebhooksDuplicate map[string]int `json:"webhooks_duplicate"`
	// Recovery evidence (informational): completions discarded because the
	// worker had lost its lease, and steps that succeeded after being IN_DOUBT.
	FencedCommits   int                    `json:"fenced_commits"`
	InDoubtResolved int                    `json:"in_doubt_resolved"`
	CallbackPolls   int                    `json:"callback_polls"` // polls of accepted payouts (slice 2.1)
	PayoutsWaited   int                    `json:"payouts_waited"` // payout steps polled at least once
	Webhooks        WebhookCounts          `json:"webhooks"`       // provider webhooks (slice 2.2)
	Violations      []invariants.Violation `json:"violations"`
	Liveness        []invariants.Violation `json:"liveness"`
	Restarts        map[string]int         `json:"restarts"` // process → crash restarts
	SettleTime      time.Duration          `json:"settle_time"`
	// WebhooksInFlight counts provider webhooks neither delivered nor given
	// up when the settle phase ended: the inbox was judged incomplete.
	WebhooksInFlight int      `json:"webhooks_in_flight"`
	Passed           bool     `json:"passed"`
	Reasons          []string `json:"reasons"`
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
	if n := r.WebhooksInFlight; n > 0 {
		add("%d webhook deliveries still in flight at the verdict", n)
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
	// Webhook faults: a fault had a real window when it was in an effective
	// fault set that stayed in force for minFaultWindow on the EXECUTED
	// timeline. Every fault that had one must have fired; in runs of a minute
	// or more, every fault must have had one (the plan reserves them).
	armedWebhook := r.realWebhookWindows()
	for _, mode := range webhookModes {
		switch {
		case armedWebhook[mode] && r.FaultHits[webhooksEndpoint+"/"+mode] == 0:
			add("vacuous: webhook fault %s was armed but never fired", mode)
		case !armedWebhook[mode] && r.Config.Duration >= time.Minute:
			add("vacuous: webhook fault %s never had a real window (in force %s while work was in flight)", mode, minFaultWindow)
		}
	}
	// Polling completes payouts without webhooks, so the webhook path must
	// show it did real work, not merely that faults were drawn.
	if r.Webhooks.Applied == 0 {
		add("vacuous: no webhook completed a payout")
	}
	if r.Config.Duration >= time.Minute && r.Webhooks.StaleIgnored == 0 {
		add("vacuous: no reordered webhook was recognised as stale")
	}
	if effective[SetProviderFaults] > 0 && hits == 0 {
		add("vacuous: provider faults were set but never fired")
	}
	if (r.Config.Failpoints != "" || r.Config.APIFailpoints != "") && r.FailpointExits == 0 {
		add("vacuous: failpoints were armed but none fired (binary built with -tags nofailpoint?)")
	}
	if r.Config.APIs > 1 {
		for i := range r.Config.APIs {
			name := fmt.Sprintf("api-%d", i)
			if r.Clients.PerReplica[name] == 0 {
				add("vacuous: API replica %s answered no client request", name)
			}
			if r.WebhooksAccepted[name] == 0 {
				add("vacuous: API replica %s accepted no new webhook", name)
			}
			// Runs of a minute or more have the dedicated dup_webhook window,
			// whose copies go to the next replica; shorter runs may see none.
			if r.Config.Duration >= time.Minute && r.WebhooksDuplicate[name] == 0 {
				add("vacuous: API replica %s deduplicated no webhook (no evidence of deduplication across replicas)", name)
			}
		}
	}
	if r.Config.Duration >= time.Minute {
		for _, name := range armedFailpoints(r.Config.Failpoints + "," + r.Config.APIFailpoints) {
			if r.FailpointFires[name] == 0 {
				add("vacuous: failpoint %s was armed but never fired", name)
			}
		}
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
	return fmt.Sprintf("bin/chaos --seed %d --duration %s --workflows %d --workers %d --apis %d --sweepers %d",
		r.Seed, c.Duration, c.Workflows, c.Workers, max(c.APIs, 1), c.Sweepers)
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
	if r.Config.APIs > 1 {
		var reqs, accepted, dups []string
		for i := range r.Config.APIs {
			name := fmt.Sprintf("api-%d", i)
			reqs = append(reqs, fmt.Sprintf("%s=%d", name, r.Clients.PerReplica[name]))
			accepted = append(accepted, fmt.Sprintf("%s=%d", name, r.WebhooksAccepted[name]))
			dups = append(dups, fmt.Sprintf("%s=%d", name, r.WebhooksDuplicate[name]))
		}
		fmt.Fprintf(&b, "api replicas: %d (requests %s; webhooks accepted %s, duplicates %s)\n", r.Config.APIs,
			strings.Join(reqs, " "), strings.Join(accepted, " "), strings.Join(dups, " "))
	}
	conflicts := 0
	for _, v := range r.Violations {
		if v.Invariant == "I10" {
			conflicts++
		}
	}
	fmt.Fprintf(&b, "conflicts: %d (provider webhooks contradicting idem; any is an I10 violation)\n", conflicts)
	fmt.Fprintf(&b, "failpoint exits: %d\n", r.FailpointExits)
	if len(r.FailpointFires) > 0 {
		var fires []string
		for _, name := range slices.Sorted(maps.Keys(r.FailpointFires)) {
			fires = append(fires, fmt.Sprintf("%s=%d", name, r.FailpointFires[name]))
		}
		fmt.Fprintf(&b, "failpoints fired: %s\n", strings.Join(fires, " "))
	}
	fmt.Fprintf(&b, "fenced commits: %d\nin-doubt resolved: %d\n", r.FencedCommits, r.InDoubtResolved)
	fmt.Fprintf(&b, "callback polls: %d (payouts that waited: %d)\n", r.CallbackPolls, r.PayoutsWaited)
	fmt.Fprintf(&b, "webhooks: %d applied, %d duplicates, %d deferred, %d stale ignored, %d in flight at the verdict\n",
		r.Webhooks.Applied, r.Webhooks.Duplicates, r.Webhooks.Deferred, r.Webhooks.StaleIgnored, r.WebhooksInFlight)

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

// WebhookCounts summarise the inbox after a run: payouts completed by a
// webhook (counted from the steps' webhook attempts, not the inbox's own
// label), duplicates absorbed, and events that needed more than one attempt.
type WebhookCounts struct {
	Applied    int `json:"applied"`
	Duplicates int `json:"duplicates"`
	Deferred   int `json:"deferred"`
	// StaleIgnored counts reordered (stale) events recognised as stale.
	StaleIgnored int `json:"stale_ignored"`
}

// armedFailpoints lists the failpoint names in comma-separated specs
// ("name=action[@p],..."), sorted and without duplicates.
func armedFailpoints(specs string) []string {
	seen := map[string]bool{}
	for entry := range strings.SplitSeq(specs, ",") {
		if name, _, ok := strings.Cut(strings.TrimSpace(entry), "="); ok && name != "" {
			seen[name] = true
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// realWebhookWindows reports which webhook faults were in a fault set that
// stayed in force for minFaultWindow while work was in flight, measured on the
// executed timeline (wall times), not the plan: a slow event in between shortens a
// window, a late fault change lengthens it.
func (r *Report) realWebhookWindows() map[string]bool {
	out := map[string]bool{}
	if len(r.Events) == 0 {
		return out
	}
	start := r.Events[0].WallTime.Add(-r.Events[0].At) // when the campaign began
	for _, e := range r.Events[1:] {
		if s := e.WallTime.Add(-e.At); s.Before(start) {
			start = s
		}
	}
	end := start.Add(r.Config.Duration)
	for i, e := range r.Events {
		if e.Kind != SetProviderFaults || e.Err != "" || e.Skipped != "" {
			continue
		}
		until := end
		for _, n := range r.Events[i+1:] {
			if (n.Kind == SetProviderFaults || n.Kind == ClearProviderFaults) && n.Err == "" && n.Skipped == "" {
				until = n.WallTime
				break
			}
		}
		if until.Sub(e.WallTime) < minFaultWindow {
			continue
		}
		// Work in flight during the window, not only at its first instant:
		// in-flight is sampled when an event fires, and a window opened right
		// after a database restart samples 0 while traffic flows through it.
		busy := e.InFlight > 0
		for _, n := range r.Events[i+1:] {
			if !n.WallTime.Before(until) {
				break
			}
			busy = busy || n.InFlight > 0
		}
		if !busy {
			continue
		}
		for _, rule := range e.Faults.Rules {
			if rule.Endpoint == webhooksEndpoint {
				out[rule.Mode] = true
			}
		}
	}
	return out
}
