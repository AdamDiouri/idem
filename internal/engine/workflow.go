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

// terminal reports whether a workflow status is an outcome the shop is told about.
func terminal(status string) bool {
	return status == wfSucceeded || status == wfFailed || status == wfCompensated
}

// transitionWorkflow moves a workflow to `to` only if it is currently in one
// of `from`. It is the only writer of workflow terminal states, and so the
// only writer of outbox events (D13): a terminal transition inserts its event
// in the same transaction, so the event exists if and only if the outcome
// committed (the database also refuses a terminal workflow without its event,
// migration 00014). A second event of the same type for a workflow fails the
// transaction; a different type cannot happen, because terminal states have no
// way out, and I8 would report it.
func transitionWorkflow(ctx context.Context, q *db.Queries, id uuid.UUID, from []string, to string) error {
	n, err := q.TransitionWorkflow(ctx, db.TransitionWorkflowParams{ToStatus: to, ID: id, FromStatuses: from})
	if err != nil {
		return fmt.Errorf("transition workflow %s to %s: %w", id, to, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: workflow %s, want one of %v before %s", ErrUnexpectedWorkflowStatus, id, from, to)
	}
	if !terminal(to) {
		return nil
	}
	eventID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("new outbox event id: %w", err)
	}
	inserted, err := q.InsertOutboxEvent(ctx, db.InsertOutboxEventParams{ID: eventID, WorkflowID: id})
	if err != nil {
		return fmt.Errorf("write outbox event for workflow %s: %w", id, err)
	}
	if inserted != 1 {
		return fmt.Errorf("write outbox event for workflow %s: %d rows inserted, want 1", id, inserted)
	}
	return nil
}

// newStep creates the workflow's next step (generation 0, PENDING) with its
// idempotency key derived once, here, and stored forever (D6). refundChargeRef
// is set only for refunds.
func newStep(ctx context.Context, q *db.Queries, workflowID uuid.UUID, name string, maxAttempts int32, refundChargeRef string) error {
	_, err := newStepGeneration(ctx, q, workflowID, name, 0, maxAttempts, refundChargeRef)
	return err
}

// newStepGeneration creates a step of the given generation and returns its ID.
// Only admin new-generation uses a generation above 0 (a new idempotency key).
func newStepGeneration(ctx context.Context, q *db.Queries, workflowID uuid.UUID, name string, generation, maxAttempts int32, refundChargeRef string) (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, fmt.Errorf("new %s step id: %w", name, err)
	}
	if err := q.InsertStep(ctx, db.InsertStepParams{
		ID:              id,
		WorkflowID:      workflowID,
		Name:            name,
		Generation:      generation,
		Status:          "PENDING",
		IdempotencyKey:  workflow.StepIdempotencyKey(workflowID, name, int(generation)),
		MaxAttempts:     maxAttempts,
		RefundChargeRef: optional(refundChargeRef),
	}); err != nil {
		return uuid.Nil, fmt.Errorf("create %s step: %w", name, err)
	}
	return id, nil
}
