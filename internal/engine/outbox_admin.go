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
	LastError  string    `json:"last_error,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// ListOutbox lists every event not delivered (PENDING, DEAD, NO_ENDPOINT,
// SKIPPED), oldest first; onlyDead keeps the DEAD ones.
func (a *Admin) ListOutbox(ctx context.Context, onlyDead bool) ([]OutboxEvent, error) {
	rows, err := db.New(a.pool).ListOutboxEvents(ctx, onlyDead)
	if err != nil {
		return nil, fmt.Errorf("list outbox: %w", err)
	}
	out := make([]OutboxEvent, 0, len(rows))
	for _, r := range rows {
		out = append(out, OutboxEvent{ID: r.ID, WorkflowID: r.WorkflowID, ClientID: r.ClientID, Type: r.Type,
			Status: r.Status, Attempts: r.Attempts, LastError: r.LastError, CreatedAt: r.CreatedAt.Time})
	}
	return out, nil
}

// Redeliver sends an event again (M2 design §5.5): one the relay gave up on
// (DEAD), one for a client that has since been given a URL (NO_ENDPOINT), one
// held back at upgrade (SKIPPED), or one the shop lost (DELIVERED). attempts
// is the ATTEMPTS the operator saw in ListOutbox: if it moved on, the event
// changed meanwhile and the action is refused (ErrStale). An event still
// PENDING is the relay's (ErrNotActionable). Audited.
func (a *Admin) Redeliver(ctx context.Context, eventID uuid.UUID, attempts int32, reason string) error {
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
	case ev.Attempts != attempts:
		return fmt.Errorf("%w: event %s now has %d attempts, not %d; list it again", ErrStale, eventID, ev.Attempts, attempts)
	}
	if err := q.RequeueOutboxEvent(ctx, eventID); err != nil {
		return fmt.Errorf("requeue outbox event: %w", err)
	}
	details, err := json.Marshal(map[string]any{"event_id": eventID, "from_status": ev.Status, "attempts": ev.Attempts})
	if err != nil {
		return err
	}
	auditID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	if err := q.InsertAdminAction(ctx, db.InsertAdminActionParams{
		ID: auditID, Actor: a.actor, Action: "redeliver_event", WorkflowID: ev.WorkflowID,
		Reason: strings.TrimSpace(reason), Details: details,
	}); err != nil {
		return fmt.Errorf("audit: %w", err)
	}
	return tx.Commit(ctx)
}
