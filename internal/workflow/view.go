package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/store/db"
)

// Reader returns the shop's view of a workflow: the SQL function
// workflow_view, which the outbox also stores at each terminal transition, so
// what GET returns and what the shop is notified of cannot drift.
type Reader struct {
	q *db.Queries
}

// NewReader returns a Reader on pool.
func NewReader(pool *pgxpool.Pool) *Reader { return &Reader{q: db.New(pool)} }

// View returns the workflow's view as JSON. found is false when the workflow
// does not exist or belongs to another client: the two are indistinguishable
// by design.
func (r *Reader) View(ctx context.Context, clientID string, id uuid.UUID) (view []byte, found bool, err error) {
	v, err := r.q.WorkflowViewForClient(ctx, db.WorkflowViewForClientParams{ID: id, ClientID: clientID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read workflow %s: %w", id, err)
	}
	return []byte(v), true, nil
}
