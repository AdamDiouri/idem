package invariants

import (
	"context"
	"fmt"

	"github.com/AdamDiouri/idem/internal/store/db"
)

// checkOutbox is I8 (slice 2.4, D13): every terminal workflow has exactly one
// outbox event, of its status's type, carrying its view; no other workflow
// has one. The event is written in the transition's own transaction, so a
// violation means a path that ends workflows without transitionWorkflow, or a
// broken transaction.
func checkOutbox(ctx context.Context, q *db.Queries) ([]Violation, error) {
	rows, err := q.OutboxMismatches(ctx)
	if err != nil {
		return nil, fmt.Errorf("query outbox mismatches: %w", err)
	}
	var out []Violation
	add := func(r db.OutboxMismatchesRow, format string, args ...any) {
		out = append(out, Violation{Invariant: "I8", WorkflowID: r.WorkflowID.String(), Detail: fmt.Sprintf(format, args...)})
	}
	for _, r := range rows {
		terminal := r.Status == "SUCCEEDED" || r.Status == "FAILED" || r.Status == "COMPENSATED"
		switch {
		case terminal && r.Events != 1:
			add(r, "%s workflow has %d outbox events, want exactly 1", r.Status, r.Events)
		case !terminal:
			add(r, "%s workflow is not terminal but has %d outbox events", r.Status, r.Events)
		case r.WrongTypes != "":
			add(r, "%s workflow has an outbox event of type %s", r.Status, r.WrongTypes)
		case r.WrongPayloads > 0:
			add(r, "%s workflow's outbox event payload is not its view", r.Status)
		}
	}
	return out, nil
}
