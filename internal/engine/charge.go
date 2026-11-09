package engine

import (
	"context"

	"github.com/AdamDiouri/idem/internal/ledger"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/store/db"
)

const (
	stepCharge        = "charge"
	stepPayout        = "payout"
	payoutMaxAttempts = 8
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
	return c.client.CreateCharge(ctx, s.IdempotencyKey, chargeRequest(s))
}

func (c *chargeStep) Lookup(ctx context.Context, s ClaimedStep) (provider.Outcome, bool) {
	return c.client.LookupCharge(ctx, s.IdempotencyKey, chargeRequest(s))
}

func chargeRequest(s ClaimedStep) provider.ChargeRequest {
	return provider.ChargeRequest{
		AmountCents: s.Input.AmountCents,
		Currency:    s.Input.Currency,
		CustomerID:  s.Input.CustomerID,
		WorkflowID:  s.WorkflowID,
	}
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

	if err := newStep(ctx, q, s.WorkflowID, stepPayout, payoutMaxAttempts, ""); err != nil {
		return err
	}
	// Advance only a RUNNING workflow: if it was parked meanwhile, roll back.
	return transitionWorkflow(ctx, q, s.WorkflowID, []string{wfRunning}, wfRunning)
}

// OnPermanentFailure fails the workflow: the charge never happened, so there is
// nothing to compensate.
func (c *chargeStep) OnPermanentFailure(ctx context.Context, q *db.Queries, s ClaimedStep) error {
	return transitionWorkflow(ctx, q, s.WorkflowID, []string{wfRunning}, wfFailed)
}
