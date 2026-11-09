package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/workflow"
)

// ErrNotActionable means the target is not a parked workflow / parked step
// (a worker may own it, it already moved on, or the action doesn't fit it).
// Nothing was changed.
var ErrNotActionable = errors.New("not actionable: workflow is not parked or the step does not fit this action")

// ErrConfirmationRequired means the action is irreversible and the parked step
// may have moved money: the caller must confirm, after checking with the
// provider, that it did not.
var ErrConfirmationRequired = errors.New("money may have moved for this step: verify with the provider, then confirm (--confirm-no-effect)")

const stepChargeName = "charge"

// defaultMaxAttempts is used for steps created by admin actions.
var defaultMaxAttempts = map[string]int32{stepChargeName: 8, stepPayout: payoutMaxAttempts, stepRefund: refundMaxAttempts}

// ParkedWorkflow is a workflow waiting for a human, with the step that parked it.
type ParkedWorkflow struct {
	WorkflowID  uuid.UUID     `json:"workflow_id"`
	StepID      uuid.UUID     `json:"step_id"`
	Step        string        `json:"step"`
	StepStatus  string        `json:"step_status"`
	DeadReason  string        `json:"dead_reason"`
	LastError   string        `json:"last_error"`
	Attempts    int32         `json:"attempts"`
	AmountCents int64         `json:"amount_cents"`
	CustomerID  string        `json:"customer_id"`
	MerchantID  string        `json:"merchant_id"`
	ParkedFor   time.Duration `json:"parked_for"`
}

// Admin performs audited human interventions on parked workflows (design
// §11.4). Every mutation is one transaction that checks the expected state,
// changes it through the steps' own hooks where money is involved, and writes
// one admin_actions row.
type Admin struct {
	pool  *pgxpool.Pool
	steps map[string]Step
	actor string
}

// NewAdmin returns an Admin. steps provide the hooks (ledger postings, saga
// moves) used by ConfirmStep; their provider clients are never called.
func NewAdmin(pool *pgxpool.Pool, steps []Step, actor string) *Admin {
	a := &Admin{pool: pool, steps: map[string]Step{}, actor: actor}
	for _, s := range steps {
		a.steps[s.Name()] = s
	}
	return a
}

// ListParked lists parked workflows; reason, if set, filters by dead_reason.
func (a *Admin) ListParked(ctx context.Context, reason string) ([]ParkedWorkflow, error) {
	rows, err := db.New(a.pool).ListParkedSteps(ctx)
	if err != nil {
		return nil, fmt.Errorf("list parked workflows: %w", err)
	}
	var out []ParkedWorkflow
	for _, r := range rows {
		p := ParkedWorkflow{
			WorkflowID: r.WorkflowID, StepID: r.StepID, Step: r.Name, StepStatus: r.Status,
			DeadReason: deref(r.DeadReason), LastError: deref(r.LastError), Attempts: r.AttemptCount,
			ParkedFor: time.Duration(r.ParkedMs) * time.Millisecond,
		}
		var in workflow.PaymentInput
		if err := json.Unmarshal(r.Input, &in); err == nil {
			p.AmountCents, p.CustomerID, p.MerchantID = in.AmountCents, in.CustomerID, in.MerchantID
		}
		if reason == "" || p.DeadReason == reason {
			out = append(out, p)
		}
	}
	return out, nil
}

// RetryStep hands a parked step back to the workers with the SAME idempotency
// key (always safe: the provider deduplicates). A step whose dead reason says
// money may have moved goes back IN_DOUBT (sticky), otherwise PENDING.
func (a *Admin) RetryStep(ctx context.Context, stepID uuid.UUID, reason string) error {
	return a.withParkedStep(ctx, stepID, reason, func(ctx context.Context, q *db.Queries, st db.LockParkedStepRow) (string, map[string]any, error) {
		to := "PENDING"
		moved, err := mayHaveMoved(ctx, q, st)
		if err != nil {
			return "", nil, err
		}
		if moved {
			to = string(ResultInDoubt)
		}
		if err := resetStep(ctx, q, st.ID, to, defaultMaxAttempts[st.Name]); err != nil {
			return "", nil, err
		}
		if err := reactivate(ctx, q, st); err != nil {
			return "", nil, err
		}
		return "retry_step", map[string]any{"step": st.Name, "from": st.Status, "dead_reason": deref(st.DeadReason), "to": to,
			"extra_attempts": defaultMaxAttempts[st.Name]}, nil
	})
}

// NewGeneration sends a parked step again under a NEW idempotency key. That is
// a new payment as far as the provider is concerned, so the caller must have
// verified that the old key moved no money (confirmNoEffect). The old step
// stays as history.
func (a *Admin) NewGeneration(ctx context.Context, stepID uuid.UUID, reason string, confirmNoEffect bool) (uuid.UUID, error) {
	if !confirmNoEffect {
		return uuid.Nil, errors.New("new-generation mints a new idempotency key: confirm that the old key moved no money (--confirm-no-effect)")
	}
	var newID uuid.UUID
	err := a.withParkedStep(ctx, stepID, reason, func(ctx context.Context, q *db.Queries, st db.LockParkedStepRow) (string, map[string]any, error) {
		gen, err := q.NextStepGeneration(ctx, db.NextStepGenerationParams{WorkflowID: st.WorkflowID, Name: st.Name})
		if err != nil {
			return "", nil, fmt.Errorf("next generation: %w", err)
		}
		// A refund reverses the workflow's CURRENT successful charge (which a
		// charge new-generation or confirm-step may have changed).
		ref := deref(st.RefundChargeRef)
		if st.Name == stepRefund {
			if ref, err = q.ChargeProviderRef(ctx, st.WorkflowID); err != nil {
				return "", nil, fmt.Errorf("no succeeded charge to refund: %w", err)
			}
		}
		if newID, err = newStepGeneration(ctx, q, st.WorkflowID, st.Name, gen, defaultMaxAttempts[st.Name], ref); err != nil {
			return "", nil, err
		}
		if err := reactivate(ctx, q, st); err != nil {
			return "", nil, err
		}
		return "new_generation", map[string]any{"step": st.Name, "old_step": st.ID, "new_step": newID, "generation": gen}, nil
	})
	return newID, err
}

// ConfirmStep records that a dead step's effect DID happen (verified with the
// provider, providerRef identifies it) and runs the step's own success hook:
// the ledger posting and the saga's next move, exactly as a worker would.
func (a *Admin) ConfirmStep(ctx context.Context, stepID uuid.UUID, providerRef, reason string) error {
	if strings.TrimSpace(providerRef) == "" {
		return errors.New("confirm-step needs the provider reference of the effect")
	}
	return a.withParkedStep(ctx, stepID, reason, func(ctx context.Context, q *db.Queries, st db.LockParkedStepRow) (string, map[string]any, error) {
		step, ok := a.steps[st.Name]
		if !ok {
			return "", nil, fmt.Errorf("%w: unknown step %s", ErrNotActionable, st.Name)
		}
		if st.Status == "FAILED_PERMANENT" {
			// A refusal can still hide a real effect when an earlier attempt was
			// ambiguous; a refused refund may also have been made outside idem.
			moved, err := mayHaveMoved(ctx, q, st)
			if err != nil {
				return "", nil, err
			}
			if !moved && st.Name != stepRefund {
				return "", nil, fmt.Errorf("%w: step %s was definitively refused and never in doubt", ErrNotActionable, st.Name)
			}
		}
		used, err := q.ProviderRefInUse(ctx, &providerRef)
		if err != nil {
			return "", nil, err
		}
		if used {
			return "", nil, fmt.Errorf("%w: provider reference %s is already recorded on another step", ErrNotActionable, providerRef)
		}
		if err := reactivate(ctx, q, st); err != nil {
			return "", nil, err
		}
		n, err := q.ConfirmStepSucceeded(ctx, db.ConfirmStepSucceededParams{ProviderRef: providerRef, ID: st.ID})
		if err != nil || n == 0 {
			return "", nil, errors.Join(ErrNotActionable, err)
		}
		claimed, err := asClaimed(st)
		if err != nil {
			return "", nil, err
		}
		if err := runHook(func() error { return step.OnSuccess(ctx, q, claimed, providerRef) }); err != nil {
			return "", nil, fmt.Errorf("%s on success: %w", st.Name, err)
		}
		return "confirm_step", map[string]any{"step": st.Name, "dead_reason": deref(st.DeadReason), "provider_ref": providerRef}, nil
	})
}

// Compensate refunds the customer of a workflow whose payout is parked, after
// a human verified the merchant was NOT paid. If the payout may have paid the
// merchant, confirmNoEffect must be true (refunding too would pay out twice).
func (a *Admin) Compensate(ctx context.Context, workflowID uuid.UUID, reason string, confirmNoEffect bool) error {
	return a.withParkedWorkflow(ctx, workflowID, reason, func(ctx context.Context, q *db.Queries, st db.LockParkedStepRow) (string, map[string]any, error) {
		if st.Name != stepPayout {
			return "", nil, fmt.Errorf("%w: compensate needs a parked payout (parked step is %s)", ErrNotActionable, st.Name)
		}
		if err := requireConfirmation(ctx, q, st, confirmNoEffect); err != nil {
			return "", nil, err
		}
		chargeRef, err := q.ChargeProviderRef(ctx, st.WorkflowID)
		if err != nil {
			return "", nil, fmt.Errorf("no succeeded charge to refund: %w", err)
		}
		gen, err := q.NextStepGeneration(ctx, db.NextStepGenerationParams{WorkflowID: st.WorkflowID, Name: stepRefund})
		if err != nil {
			return "", nil, err
		}
		if _, err := newStepGeneration(ctx, q, st.WorkflowID, stepRefund, gen, refundMaxAttempts, chargeRef); err != nil {
			return "", nil, err
		}
		if err := transitionWorkflow(ctx, q, st.WorkflowID, []string{wfNeedsAttention}, wfCompensating); err != nil {
			return "", nil, err
		}
		return "compensate", map[string]any{"payout_step": st.ID, "dead_reason": deref(st.DeadReason), "charge_ref": chargeRef}, nil
	})
}

// Fail closes a workflow whose charge is parked, after a human verified the
// customer was NOT charged. If the charge may have happened, confirmNoEffect
// must be true.
func (a *Admin) Fail(ctx context.Context, workflowID uuid.UUID, reason string, confirmNoEffect bool) error {
	return a.withParkedWorkflow(ctx, workflowID, reason, func(ctx context.Context, q *db.Queries, st db.LockParkedStepRow) (string, map[string]any, error) {
		if st.Name != stepChargeName {
			return "", nil, fmt.Errorf("%w: fail needs a parked charge (parked step is %s)", ErrNotActionable, st.Name)
		}
		if err := requireConfirmation(ctx, q, st, confirmNoEffect); err != nil {
			return "", nil, err
		}
		if err := transitionWorkflow(ctx, q, st.WorkflowID, []string{wfNeedsAttention}, wfFailed); err != nil {
			return "", nil, err
		}
		return "fail", map[string]any{"charge_step": st.ID, "dead_reason": deref(st.DeadReason)}, nil
	})
}

type parkedAction func(ctx context.Context, q *db.Queries, st db.LockParkedStepRow) (action string, details map[string]any, err error)

func (a *Admin) withParkedWorkflow(ctx context.Context, workflowID uuid.UUID, reason string, fn parkedAction) error {
	stepID, err := db.New(a.pool).ParkedStepOfWorkflow(ctx, workflowID)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: workflow %s", ErrNotActionable, workflowID)
	}
	if err != nil {
		return err
	}
	return a.withParkedStep(ctx, stepID, reason, fn)
}

// withParkedStep runs fn in one transaction holding locks on the parked step
// and its workflow, then writes the audit row. Any failure rolls back.
func (a *Admin) withParkedStep(ctx context.Context, stepID uuid.UUID, reason string, fn parkedAction) error {
	if strings.TrimSpace(reason) == "" {
		return errors.New("a reason is required for every admin action")
	}
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)

	st, err := q.LockParkedStep(ctx, stepID)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: step %s", ErrNotActionable, stepID)
	}
	if err != nil {
		return fmt.Errorf("lock parked step: %w", err)
	}

	action, details, err := fn(ctx, q, st)
	if err != nil {
		return err
	}
	detailJSON, err := json.Marshal(details)
	if err != nil {
		return err
	}
	auditID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	if err := q.InsertAdminAction(ctx, db.InsertAdminActionParams{
		ID: auditID, Actor: a.actor, Action: action, WorkflowID: st.WorkflowID, StepID: &st.ID,
		Reason: strings.TrimSpace(reason), Details: detailJSON,
	}); err != nil {
		return fmt.Errorf("audit: %w", err)
	}
	return tx.Commit(ctx)
}

// mayHaveMoved reports whether the parked step may have moved money: its dead
// reason says so, or any attempt was ambiguous or abandoned (sticky doubt also
// covers refused non-pivot steps, which have no dead reason).
func mayHaveMoved(ctx context.Context, q *db.Queries, st db.LockParkedStepRow) (bool, error) {
	if st.DeadReason != nil && MoneyMayHaveMoved(*st.DeadReason) {
		return true, nil
	}
	had, err := q.StepHadInDoubtAttempt(ctx, st.ID)
	if err != nil {
		return false, fmt.Errorf("attempt history: %w", err)
	}
	return had, nil
}

func requireConfirmation(ctx context.Context, q *db.Queries, st db.LockParkedStepRow, confirmed bool) error {
	if confirmed {
		return nil
	}
	moved, err := mayHaveMoved(ctx, q, st)
	if err != nil {
		return err
	}
	if moved {
		return ErrConfirmationRequired
	}
	return nil
}

// reactivate moves the workflow back to where its parked step belongs.
func reactivate(ctx context.Context, q *db.Queries, st db.LockParkedStepRow) error {
	to := wfRunning
	if st.Name == stepRefund {
		to = wfCompensating
	}
	return transitionWorkflow(ctx, q, st.WorkflowID, []string{wfNeedsAttention}, to)
}

func resetStep(ctx context.Context, q *db.Queries, id uuid.UUID, to string, extraAttempts int32) error {
	n, err := q.ResetStepForRetry(ctx, db.ResetStepForRetryParams{Status: to, ExtraAttempts: extraAttempts, ID: id})
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotActionable
	}
	return nil
}

func asClaimed(st db.LockParkedStepRow) (ClaimedStep, error) {
	c := ClaimedStep{ID: st.ID, WorkflowID: st.WorkflowID, Name: st.Name, IdempotencyKey: st.IdempotencyKey,
		RefundChargeRef: deref(st.RefundChargeRef)}
	if err := json.Unmarshal(st.Input, &c.Input); err != nil {
		return ClaimedStep{}, fmt.Errorf("decode workflow input: %w", err)
	}
	return c, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
