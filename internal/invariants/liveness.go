package invariants

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/store/db"
)

// CheckLiveness evaluates I4: every workflow has reached a terminal or
// attention state. It is only meaningful after quiescence (no new traffic, no
// faults); during normal operation in-flight workflows are expected.
func CheckLiveness(ctx context.Context, pool *pgxpool.Pool) ([]Violation, error) {
	rows, err := db.New(pool).NonTerminalWorkflows(ctx)
	if err != nil {
		return nil, fmt.Errorf("query non-terminal workflows: %w", err)
	}
	violations := make([]Violation, 0, len(rows))
	for _, r := range rows {
		violations = append(violations, Violation{
			Invariant:  "I4",
			WorkflowID: r.ID.String(),
			Detail:     fmt.Sprintf("%s since %s", r.Status, r.UpdatedAt.Time.UTC().Format(time.RFC3339)),
		})
	}
	return violations, nil
}

// checkKeys evaluates I6: every inbound idempotency key resolved to exactly
// one workflow with a stored response, and every workflow came from a key.
func checkKeys(ctx context.Context, q *db.Queries) ([]Violation, error) {
	orphans, err := q.KeysWithoutWorkflow(ctx)
	if err != nil {
		return nil, fmt.Errorf("query keys without workflow: %w", err)
	}
	var violations []Violation
	for _, k := range orphans {
		violations = append(violations, Violation{
			Invariant: "I6",
			Detail:    fmt.Sprintf("idempotency key %s/%s has no workflow or stored response", k.ClientID, k.Key),
		})
	}

	keyless, err := q.WorkflowsWithoutKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("query workflows without key: %w", err)
	}
	for _, id := range keyless {
		violations = append(violations, Violation{Invariant: "I6", WorkflowID: id.String(), Detail: "workflow has no idempotency key"})
	}
	return violations, nil
}
