package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/workflow"
)

// Workflow statuses (design §8.2).
const (
	wfRunning        = "RUNNING"
	wfSucceeded      = "SUCCEEDED"
	wfFailed         = "FAILED"
	wfCompensating   = "COMPENSATING"
	wfCompensated    = "COMPENSATED"
	wfNeedsAttention = "NEEDS_ATTENTION"
)

// ErrUnexpectedWorkflowStatus means a workflow was not in the state a
// transition expected — someone (a human, a bug) changed it concurrently. The
// completion rolls back rather than overwrite it.
var ErrUnexpectedWorkflowStatus = errors.New("workflow is not in the expected status")

// transitionWorkflow moves a workflow to `to` only if it is currently in one of `from`.
func transitionWorkflow(ctx context.Context, q *db.Queries, id uuid.UUID, from []string, to string) error {
	n, err := q.TransitionWorkflow(ctx, db.TransitionWorkflowParams{ToStatus: to, ID: id, FromStatuses: from})
	if err != nil {
		return fmt.Errorf("transition workflow %s to %s: %w", id, to, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: workflow %s, want one of %v before %s", ErrUnexpectedWorkflowStatus, id, from, to)
	}
	return nil
}

// newStep creates the workflow's next step (generation 0, PENDING) with its
// idempotency key derived once, here, and stored forever (D6). refundChargeRef
// is set only for refunds.
func newStep(ctx context.Context, q *db.Queries, workflowID uuid.UUID, name string, maxAttempts int32, refundChargeRef string) error {
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("new %s step id: %w", name, err)
	}
	if err := q.InsertStep(ctx, db.InsertStepParams{
		ID:              id,
		WorkflowID:      workflowID,
		Name:            name,
		Generation:      0,
		Status:          "PENDING",
		IdempotencyKey:  workflow.StepIdempotencyKey(workflowID, name, 0),
		MaxAttempts:     maxAttempts,
		RefundChargeRef: optional(refundChargeRef),
	}); err != nil {
		return fmt.Errorf("create %s step: %w", name, err)
	}
	return nil
}
