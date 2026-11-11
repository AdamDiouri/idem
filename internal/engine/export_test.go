package engine

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/AdamDiouri/idem/internal/store/db"
)

// SetInboxAfterRead installs a hook that runs after an inbox event's read
// phase (snapshot, and provider re-check if any) and before its apply
// transaction (tests only).
func SetInboxAfterRead(p *InboxProcessor, f func()) { p.opts.afterRead = f }

// SetBeforeRecordHookFailure installs a hook that runs after a completion's
// hook failed and before the failure is recorded (tests only).
func SetBeforeRecordHookFailure(o *WorkerOptions, f func()) { o.beforeRecordHookFailure = f }

// TransitionWorkflow exposes transitionWorkflow inside a caller's transaction
// (tests only).
func TransitionWorkflow(ctx context.Context, tx pgx.Tx, id uuid.UUID, from []string, to string) error {
	return transitionWorkflow(ctx, db.New(tx), id, from, to)
}

// IsTerminal exposes terminal (tests only).
func IsTerminal(status string) bool { return terminal(status) }
