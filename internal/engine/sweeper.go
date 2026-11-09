package engine

import (
	"context"
	"fmt"

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
