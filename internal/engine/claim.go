package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/store/db"
)

// Claim takes ownership of the next runnable step whose name is in names.
// It returns false when nothing is runnable. The claim, its attempt record and
// the workflow's PENDING → RUNNING transition commit together.
func Claim(ctx context.Context, pool *pgxpool.Pool, workerID string, names []string, leaseTTL time.Duration) (ClaimedStep, bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return ClaimedStep{}, false, fmt.Errorf("begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)

	row, err := q.ClaimStep(ctx, db.ClaimStepParams{
		WorkerID: workerID,
		LeaseMs:  leaseTTL.Milliseconds(),
		Names:    names,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return ClaimedStep{}, false, nil
	}
	if err != nil {
		return ClaimedStep{}, false, fmt.Errorf("claim step: %w", err)
	}

	s := ClaimedStep{
		ID:             row.ID,
		WorkflowID:     row.WorkflowID,
		Name:           row.Name,
		IdempotencyKey: row.IdempotencyKey,
		LeaseEpoch:     row.LeaseEpoch,
		Attempt:        row.AttemptCount,
		AttemptNo:      row.AttemptCount + row.PollCount,
		PollCount:      row.PollCount,
		PollsSinceSend: row.PollsSinceSend,
		WaitedFor:      time.Duration(row.WaitedMs) * time.Millisecond,
		MaxAttempts:    row.MaxAttempts,
		SendDoubt:      row.SendDoubt,
		KeyAge:         time.Duration(row.KeyAgeMs) * time.Millisecond,
	}
	if row.RefundChargeRef != nil {
		s.RefundChargeRef = *row.RefundChargeRef
	}
	if row.PriorStatus != nil {
		s.PriorStatus = *row.PriorStatus
	}
	if row.ProviderRef != nil {
		s.ProviderRef = *row.ProviderRef
	}
	kind := "send"
	if s.PriorStatus == string(ResultAwaitingCallback) {
		kind = "poll" // a poll sends nothing (design D17)
	}
	if err := json.Unmarshal(row.Input, &s.Input); err != nil {
		return ClaimedStep{}, false, fmt.Errorf("decode workflow input: %w", err)
	}

	if err := q.InsertStepAttempt(ctx, db.InsertStepAttemptParams{
		StepID:     s.ID,
		AttemptNo:  s.AttemptNo,
		LeaseEpoch: s.LeaseEpoch,
		WorkerID:   workerID,
		Kind:       kind,
	}); err != nil {
		return ClaimedStep{}, false, fmt.Errorf("record attempt: %w", err)
	}
	if err := q.MarkWorkflowRunning(ctx, s.WorkflowID); err != nil {
		return ClaimedStep{}, false, fmt.Errorf("mark workflow running: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return ClaimedStep{}, false, fmt.Errorf("commit claim: %w", err)
	}
	return s, true, nil
}
