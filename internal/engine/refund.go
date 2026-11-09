package engine

import (
	"context"
	"errors"

	"github.com/AdamDiouri/idem/internal/ledger"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/store/db"
)

type refundStep struct {
	client *provider.Client
}

// NewRefundStep returns the compensation step: it returns the full charge,
// fee included, to the customer after a payout definitively failed.
func NewRefundStep(client *provider.Client) Step {
	return &refundStep{client: client}
}

func (r *refundStep) Name() string { return stepRefund }

func (r *refundStep) Pivot() bool { return false }

var errNoChargeRef = errors.New("refund step has no charge reference")

func (r *refundStep) Execute(ctx context.Context, s ClaimedStep) provider.Outcome {
	if s.RefundChargeRef == "" { // impossible given steps_refund_has_charge; never guess
		return provider.Outcome{Class: provider.Permanent, Err: errNoChargeRef}
	}
	return r.client.CreateRefund(ctx, s.IdempotencyKey, refundRequest(s))
}

func (r *refundStep) Lookup(ctx context.Context, s ClaimedStep) (provider.Outcome, bool) {
	if s.RefundChargeRef == "" {
		return provider.Outcome{Class: provider.Permanent, Err: errNoChargeRef}, false
	}
	return r.client.LookupRefund(ctx, s.IdempotencyKey, refundRequest(s))
}

func refundRequest(s ClaimedStep) provider.RefundRequest {
	return provider.RefundRequest{
		AmountCents: s.Input.AmountCents,
		Currency:    s.Input.Currency,
		ChargeID:    s.RefundChargeRef,
		WorkflowID:  s.WorkflowID,
	}
}

// OnSuccess posts the reversal and marks the workflow compensated.
func (r *refundStep) OnSuccess(ctx context.Context, q *db.Queries, s ClaimedStep, _ string) error {
	if err := ledger.Post(ctx, q, ledger.Txn{
		WorkflowID: s.WorkflowID,
		StepID:     s.ID,
		Kind:       stepRefund,
		Entries:    ledger.RefundEntries(s.Input.AmountCents, s.Input.FeeCents),
	}); err != nil {
		return err
	}
	return transitionWorkflow(ctx, q, s.WorkflowID, []string{wfCompensating}, wfCompensated)
}

// OnPermanentFailure: the customer was charged and the provider refuses the
// refund. Nothing automatic can fix that; a human must act.
func (r *refundStep) OnPermanentFailure(ctx context.Context, q *db.Queries, s ClaimedStep) error {
	return transitionWorkflow(ctx, q, s.WorkflowID, []string{wfCompensating}, wfNeedsAttention)
}
