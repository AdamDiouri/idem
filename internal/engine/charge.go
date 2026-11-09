package engine

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/AdamDiouri/idem/internal/ledger"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/workflow"
)

const (
	stepCharge        = "charge"
	stepPayout        = "payout"
	payoutMaxAttempts = 8
	statusPending     = "PENDING"
)

type chargeStep struct {
	client *provider.Client
}

// NewChargeStep returns the step that charges the customer.
func NewChargeStep(client *provider.Client) Step {
	return &chargeStep{client: client}
}

func (c *chargeStep) Name() string { return stepCharge }

func (c *chargeStep) Execute(ctx context.Context, s ClaimedStep) provider.Outcome {
	return c.client.CreateCharge(ctx, s.IdempotencyKey, provider.ChargeRequest{
		AmountCents: s.Input.AmountCents,
		Currency:    s.Input.Currency,
		CustomerID:  s.Input.CustomerID,
		WorkflowID:  s.WorkflowID,
	})
}

// OnSuccess posts the charge to the ledger and creates the payout step, in the
// same transaction as the charge's SUCCEEDED transition (D2, GR5).
func (c *chargeStep) OnSuccess(ctx context.Context, q *db.Queries, s ClaimedStep, _ string) error {
	if err := ledger.Post(ctx, q, ledger.Txn{
		WorkflowID: s.WorkflowID,
		StepID:     s.ID,
		Kind:       stepCharge,
		Entries:    ledger.ChargeEntries(s.Input.AmountCents, s.Input.FeeCents),
	}); err != nil {
		return err
	}

	payoutID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("new payout step id: %w", err)
	}
	if err := q.InsertStep(ctx, db.InsertStepParams{
		ID:             payoutID,
		WorkflowID:     s.WorkflowID,
		Name:           stepPayout,
		Generation:     0,
		Status:         statusPending,
		IdempotencyKey: workflow.StepIdempotencyKey(s.WorkflowID, stepPayout, 0),
		MaxAttempts:    payoutMaxAttempts,
	}); err != nil {
		return fmt.Errorf("create payout step: %w", err)
	}
	return q.BumpWorkflowVersion(ctx, s.WorkflowID)
}

// OnPermanentFailure fails the workflow: the charge never happened, so there is
// nothing to compensate.
func (c *chargeStep) OnPermanentFailure(ctx context.Context, q *db.Queries, s ClaimedStep) error {
	return q.SetWorkflowStatus(ctx, db.SetWorkflowStatusParams{Status: "FAILED", ID: s.WorkflowID})
}
