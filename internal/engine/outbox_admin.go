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

	"github.com/AdamDiouri/idem/internal/store/db"
)

// OutboxEvent is a workflow outcome that has not reached its shop (slice 2.5).
type OutboxEvent struct {
	ID         uuid.UUID `json:"event_id"`
	WorkflowID uuid.UUID `json:"workflow_id"`
	ClientID   string    `json:"client_id"`
	Type       string    `json:"type"`
	Status     string    `json:"status"`
	Attempts   int32     `json:"attempts"`
	Version    int64     `json:"version"` // the lease epoch: grows with every change of owner; redeliver's guard
	LastError  string    `json:"last_error,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// OutboxFilter selects events for ListOutbox: by default every event not
// delivered (PENDING, DEAD, NO_ENDPOINT, SKIPPED).
type OutboxFilter struct {
	OnlyDead bool // only DEAD
	All      bool // delivered ones too
}

// ListOutbox lists events, oldest first.
func (a *Admin) ListOutbox(ctx context.Context, f OutboxFilter) ([]OutboxEvent, error) {
	rows, err := db.New(a.pool).ListOutboxEvents(ctx, db.ListOutboxEventsParams{IncludeDelivered: f.All, OnlyDead: f.OnlyDead})
	if err != nil {
		return nil, fmt.Errorf("list outbox: %w", err)
	}
	out := make([]OutboxEvent, 0, len(rows))
	for _, r := range rows {
		out = append(out, OutboxEvent{ID: r.ID, WorkflowID: r.WorkflowID, ClientID: r.ClientID, Type: r.Type,
			Status: r.Status, Attempts: r.Attempts, Version: r.LeaseEpoch, LastError: r.LastError, CreatedAt: r.CreatedAt.Time})
	}
	return out, nil
}

// Redeliver sends an event again (M2 design §5.5): one the relay gave up on
// (DEAD), one for a client that has since been given a URL (NO_ENDPOINT), one
// held back at upgrade (SKIPPED), or one the shop lost (DELIVERED). version
// is the VERSION the operator saw in ListOutbox (the lease epoch, which only
// grows): if it moved on, the event changed meanwhile and the action is
// refused (ErrStale). An event still PENDING is the relay's (ErrNotActionable).
// Audited.
func (a *Admin) Redeliver(ctx context.Context, eventID uuid.UUID, version int64, reason string) error {
	if strings.TrimSpace(reason) == "" {
		return errors.New("a reason is required for every admin action")
	}
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	ev, err := q.LockOutboxEvent(ctx, eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: no outbox event %s", ErrNotActionable, eventID)
	}
	if err != nil {
		return fmt.Errorf("lock outbox event: %w", err)
	}
	switch {
	case ev.Status == "PENDING":
		return fmt.Errorf("%w: event %s is still PENDING; the relay is delivering it", ErrNotActionable, eventID)
	case ev.LeaseEpoch != version:
		return fmt.Errorf("%w: event %s is now at version %d, not %d; list it again", ErrStale, eventID, ev.LeaseEpoch, version)
	}
	if err := a.requeue(ctx, q, eventID, ev.WorkflowID, ev.Status, ev.Attempts, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RedeliverAll sends again every event of one client in one status: DEAD
// (after a long outage) or NO_ENDPOINT and SKIPPED (once the client has a
// URL). One audit row per event. It returns how many were queued.
func (a *Admin) RedeliverAll(ctx context.Context, clientID, status, reason string) (int, error) {
	if strings.TrimSpace(reason) == "" {
		return 0, errors.New("a reason is required for every admin action")
	}
	switch status {
	case "DEAD", "NO_ENDPOINT", "SKIPPED":
	default:
		return 0, fmt.Errorf("%w: bulk redelivery takes DEAD, NO_ENDPOINT or SKIPPED, not %q", ErrNotActionable, status)
	}
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	events, err := q.LockClientOutboxEvents(ctx, db.LockClientOutboxEventsParams{ClientID: clientID, Status: status})
	if err != nil {
		return 0, fmt.Errorf("lock outbox events: %w", err)
	}
	for _, ev := range events {
		if err := a.requeue(ctx, q, ev.ID, ev.WorkflowID, ev.Status, ev.Attempts, reason); err != nil {
			return 0, err
		}
	}
	return len(events), tx.Commit(ctx)
}

// requeue makes a locked event PENDING again with a fresh budget, audited.
func (a *Admin) requeue(ctx context.Context, q *db.Queries, eventID, workflowID uuid.UUID, from string, attempts int32, reason string) error {
	if err := q.RequeueOutboxEvent(ctx, eventID); err != nil {
		return fmt.Errorf("requeue outbox event: %w", err)
	}
	details, err := json.Marshal(map[string]any{"event_id": eventID, "from_status": from, "attempts": attempts})
	if err != nil {
		return err
	}
	auditID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	if err := q.InsertAdminAction(ctx, db.InsertAdminActionParams{
		ID: auditID, Actor: a.actor, Action: "redeliver_event", WorkflowID: workflowID,
		Reason: strings.TrimSpace(reason), Details: details,
	}); err != nil {
		return fmt.Errorf("audit: %w", err)
	}
	return nil
}
