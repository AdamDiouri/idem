package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/store/db"
)

// ReclaimExpiredLeases moves every RUNNING step whose lease has expired to
// IN_DOUBT and bumps its fencing token, so the old holder can no longer
// complete it. It returns how many steps were reclaimed. Safe to run from any
// number of sweepers at once.
func ReclaimExpiredLeases(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	n, err := db.New(pool).ReclaimExpiredLeases(ctx)
	if err != nil {
		return 0, fmt.Errorf("reclaim expired leases: %w", err)
	}
	return n, nil
}

// StuckWorkflow is a non-terminal workflow that has not changed for too long.
type StuckWorkflow struct {
	ID     uuid.UUID
	Status string
	Idle   time.Duration // since the last status change or step attempt
}

// FindStuckWorkflows returns PENDING, RUNNING or COMPENSATING workflows that
// made no progress (no status change, no step attempt) for olderThan, longest
// idle first (design §11.1). It changes nothing: stuck work must be
// made loud, not silently "fixed".
func FindStuckWorkflows(ctx context.Context, pool *pgxpool.Pool, olderThan time.Duration) ([]StuckWorkflow, error) {
	rows, err := db.New(pool).FindStuckWorkflows(ctx, olderThan.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("find stuck workflows: %w", err)
	}
	out := make([]StuckWorkflow, len(rows))
	for i, r := range rows {
		out[i] = StuckWorkflow{ID: r.ID, Status: r.Status, Idle: time.Duration(r.IdleMs) * time.Millisecond}
	}
	return out, nil
}
