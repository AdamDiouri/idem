package engine

import (
	"context"
	"fmt"

	"github.com/AdamDiouri/idem/internal/ledger"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/store/db"
)

const (
	stepRefund        = "refund"
	refundMaxAttempts = 20 // compensation must try harder (design §9.3)
)

type payoutStep struct {
	client *provider.Client
}

// NewPayoutStep returns the step that pays the merchant. It is the saga's pivot
// (design §8.1): once it succeeds the money has left and cannot be undone.
func NewPayoutStep(client *provider.Client) Step {
	return &payoutStep{client: client}
}

func (p *payoutStep) Name() string { return stepPayout }

func (p *payoutStep) Pivot() bool { return true }

func (p *payoutStep) Execute(ctx context.Context, s ClaimedStep) provider.Outcome {
	return p.client.CreatePayout(ctx, s.IdempotencyKey, payoutRequest(s))
}

func (p *payoutStep) Lookup(ctx context.Context, s ClaimedStep) (provider.Outcome, bool) {
	return p.client.LookupPayout(ctx, s.IdempotencyKey, payoutRequest(s))
}

// Poll asks the provider for the accepted payout's current state.
func (p *payoutStep) Poll(ctx context.Context, s ClaimedStep) provider.Outcome {
	return p.client.GetPayout(ctx, s.ProviderRef, payoutRequest(s))
}

func payoutRequest(s ClaimedStep) provider.PayoutRequest {
	return provider.PayoutRequest{
		AmountCents: s.Input.AmountCents - s.Input.FeeCents,
		Currency:    s.Input.Currency,
		MerchantID:  s.Input.MerchantID,
		WorkflowID:  s.WorkflowID,
	}
}

// OnSuccess posts the payout and completes the workflow.
func (p *payoutStep) OnSuccess(ctx context.Context, q *db.Queries, s ClaimedStep, _ string) error {
	if err := ledger.Post(ctx, q, ledger.Txn{
		WorkflowID: s.WorkflowID,
		StepID:     s.ID,
		Kind:       stepPayout,
		Entries:    ledger.PayoutEntries(s.Input.AmountCents, s.Input.FeeCents),
	}); err != nil {
		return err
	}
	return transitionWorkflow(ctx, q, s.WorkflowID, []string{wfRunning}, wfSucceeded)
}

// OnPermanentFailure compensates: the provider definitively refused the payout,
// so no money reached the merchant and the customer must be refunded. Only a
// definite failure gets here; an ambiguous payout that runs out of attempts
// goes DEAD instead and is never refunded automatically.
func (p *payoutStep) OnPermanentFailure(ctx context.Context, q *db.Queries, s ClaimedStep) error {
	chargeRef, err := q.ChargeProviderRef(ctx, s.WorkflowID)
	if err != nil {
		return fmt.Errorf("payout failed but no succeeded charge found for workflow %s: %w", s.WorkflowID, err)
	}
	if err := newStep(ctx, q, s.WorkflowID, stepRefund, refundMaxAttempts, chargeRef); err != nil {
		return err
	}
	return transitionWorkflow(ctx, q, s.WorkflowID, []string{wfRunning}, wfCompensating)
}
