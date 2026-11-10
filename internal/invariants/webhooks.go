package invariants

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/store/db"
)

// CheckWebhooks checks the webhook inbox against the provider's delivery log.
// Polling completes every payout whether or not webhooks work, so without
// this a broken webhook path would pass every other invariant. Against an
// honest provider:
//
//   - I11: every event the provider saw acknowledged is in the inbox; no event
//     was accepted on a wrongly signed delivery alone (a bad_signature fault
//     whose first attempt is the only one); the inbox holds no event the
//     provider never sent; an event marked applied left its trace on the step
//     (an attempt of kind webhook).
//   - I10 (with the CONFLICT check in Check): a true event ends PROCESSED; a
//     stale (reordered) one ends IGNORED as stale, or PROCESSED when it reached
//     a step still waiting (applied with the provider's re-checked state, never
//     its own claim); either may be EVIDENCE only on a workflow that needs
//     attention. Anything else (orphan,
//     malformed, unsupported, mismatch, a true event dismissed as stale) means
//     idem misread its news.
//
// prunedBefore is the inbox retention cutoff: an acknowledged event created
// before it may have been pruned, so its absence is not a violation. Chaos
// passes the zero time (nothing is pruned during a run); `idem check` passes
// now − IDEM_INBOX_RETENTION. EVIDENCE rows also stay after an operator
// resolves the parked workflow, so an operator's action allows them.
func CheckWebhooks(ctx context.Context, pool *pgxpool.Pool, deliveries []fakeprovider.WebhookDelivery, prunedBefore time.Time) ([]Violation, error) {
	rows, err := db.New(pool).InboxRowsForCheck(ctx)
	if err != nil {
		return nil, fmt.Errorf("query inbox: %w", err)
	}
	return checkWebhookRows(rows, deliveries, prunedBefore), nil
}

func checkWebhookRows(rows []db.InboxRowsForCheckRow, deliveries []fakeprovider.WebhookDelivery, prunedBefore time.Time) []Violation {
	var violations []Violation
	add := func(invariant, format string, args ...any) {
		violations = append(violations, Violation{Invariant: invariant, Detail: fmt.Sprintf(format, args...)})
	}

	sent := make(map[string]fakeprovider.WebhookDelivery, len(deliveries))
	for _, d := range deliveries {
		sent[d.EventID] = d
	}
	stored := make(map[string]bool, len(rows))
	for _, r := range rows {
		stored[r.ProviderEventID] = true
	}

	for _, d := range deliveries {
		if !d.Delivered {
			continue
		}
		mayBePruned := !prunedBefore.IsZero() && d.Created < prunedBefore.Unix()
		if !stored[d.EventID] && !mayBePruned {
			add("I11", "provider event %s was acknowledged but is not in the inbox", d.EventID)
		}
		if d.Fault == fakeprovider.ModeBadSignature && d.Attempts == 1 {
			add("I11", "provider event %s was accepted on its only, wrongly signed delivery", d.EventID)
		}
	}

	for _, r := range rows {
		d, ok := sent[r.ProviderEventID]
		if !ok {
			add("I11", "inbox holds event %s, which the provider never sent", r.ProviderEventID)
			continue
		}
		if r.Status == "PROCESSED" && r.Note == "applied" && !r.StepApplied {
			add("I11", "event %s was marked applied, but its step records no webhook attempt", r.ProviderEventID)
		}
		parked := (r.WorkflowStatus != nil && *r.WorkflowStatus == "NEEDS_ATTENTION") || r.OperatorActed
		switch {
		case r.Status == "EVIDENCE" && parked:
		case r.Status == "EVIDENCE":
			add("I10", "event %s is EVIDENCE on a workflow that never needed attention", r.ProviderEventID)
		case r.Status == "CONFLICT":
			// Reported by Check (I10), with its workflow.
		case d.Stale && r.Status == "IGNORED" && r.Note == "stale":
		case r.Status == "PROCESSED":
			// A stale event that reached a step still waiting is applied too,
			// with the provider's re-checked state, never the event's claim;
			// I3 and I7 judge the money that resulted.
		case d.Stale:
			add("I10", "stale event %s ended %s (%s), want IGNORED as stale or PROCESSED", r.ProviderEventID, r.Status, r.Note)
		default:
			add("I10", "event %s ended %s (%s), want PROCESSED", r.ProviderEventID, r.Status, r.Note)
		}
	}
	return violations
}
