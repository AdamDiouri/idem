package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/store"
	"github.com/AdamDiouri/idem/internal/testutil"
)

var workflowStatuses = []string{"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "COMPENSATING", "COMPENSATED", "NEEDS_ATTENTION"}

// allowedWorkflow is slice 2.5b's transition table (plan; design §8.2).
var allowedWorkflow = map[string][]string{
	"PENDING":         {"RUNNING"},
	"RUNNING":         {"SUCCEEDED", "FAILED", "COMPENSATING", "NEEDS_ATTENTION"},
	"COMPENSATING":    {"COMPENSATED", "NEEDS_ATTENTION"},
	"NEEDS_ATTENTION": {"RUNNING", "COMPENSATING", "FAILED"},
}

// plantWorkflow creates a workflow already in status, bypassing every trigger.
func plantWorkflow(t *testing.T, status string) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	testutil.Plant(t, dsn, `INSERT INTO workflows (id, client_id, type, status, input) VALUES ($1, 'c', 'payment_payout', $2, $3)`,
		id, status, viewInput)
	return id
}

// moveWorkflow changes the status as the services would (the restricted
// role), writing the outbox event a terminal status needs (D13).
func moveWorkflow(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, to string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `UPDATE workflows SET status = $2 WHERE id = $1`, id, to); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox (id, client_id, workflow_id, type, payload)
		SELECT gen_random_uuid(), client_id, id, workflow_event_type(status), convert_to(workflow_view(id), 'UTF8')
		FROM workflows WHERE id = $1 AND workflow_event_type(status) IS NOT NULL`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Slice 2.5b: every edge of the table is allowed, every other move refused,
// a status may always be written again.
func TestWorkflowTransitions_Table(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	for _, from := range workflowStatuses {
		for _, to := range workflowStatuses {
			t.Run(from+"→"+to, func(t *testing.T) {
				id := plantWorkflow(t, from)
				err := moveWorkflow(ctx, pool, id, to)
				allowed := from == to || contains(allowedWorkflow[from], to)
				if allowed {
					require.NoError(t, err)
					return
				}
				require.Error(t, err)
				require.Equal(t, "23514", pgCode(t, err))
				require.Contains(t, err.Error(), fmt.Sprintf("%s → %s", from, to))
				require.Contains(t, err.Error(), id.String())
			})
		}
	}
}

// "Terminal" and "active" are defined once, in SQL; Go agrees.
func TestWorkflowStatusSets_SQL(t *testing.T) {
	pool := testutil.NewOwnerPool(t, dsn)
	for _, s := range workflowStatuses {
		var terminal, active bool
		require.NoError(t, pool.QueryRow(context.Background(), `SELECT workflow_is_terminal($1), workflow_is_active($1)`, s).Scan(&terminal, &active))
		require.Equal(t, s == "SUCCEEDED" || s == "FAILED" || s == "COMPENSATED", terminal, s)
		require.Equal(t, s == "PENDING" || s == "RUNNING" || s == "COMPENSATING", active, s)
	}
}

func TestMigrate_00017DownAndUp(t *testing.T) {
	ctx := context.Background()
	testutil.NewPool(t, dsn)
	owner := testutil.NewOwnerPool(t, dsn)
	t.Cleanup(func() { require.NoError(t, store.Migrate(ctx, owner)) })
	require.NoError(t, store.MigrateDownTo(ctx, owner, 16))
	var n int
	require.NoError(t, owner.QueryRow(ctx, `SELECT count(*) FROM pg_trigger WHERE tgname = 'workflows_transition_check'`).Scan(&n))
	require.Zero(t, n)
	require.NoError(t, store.Migrate(ctx, owner))
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

var stepStatuses = []string{"PENDING", "RUNNING", "IN_DOUBT", "AWAITING_CALLBACK", "SUCCEEDED", "FAILED_PERMANENT", "DEAD"}

// allowedStep is slice 2.5b's step transition table (plan; design §8.3).
var allowedStep = map[string][]string{
	"PENDING":           {"RUNNING"},
	"IN_DOUBT":          {"RUNNING"},
	"AWAITING_CALLBACK": {"RUNNING", "SUCCEEDED", "FAILED_PERMANENT", "DEAD"},
	"RUNNING":           {"PENDING", "IN_DOUBT", "AWAITING_CALLBACK", "SUCCEEDED", "FAILED_PERMANENT", "DEAD"},
	"DEAD":              {"PENDING", "IN_DOUBT", "SUCCEEDED"},
	"FAILED_PERMANENT":  {"PENDING", "IN_DOUBT", "SUCCEEDED"},
}

func plantStep(t *testing.T, status string) uuid.UUID {
	t.Helper()
	wf := plantWorkflow(t, "RUNNING")
	id := uuid.Must(uuid.NewV7())
	// Valid under the other step constraints: a DEAD step has a reason, a
	// waiting one a provider reference and a deadline.
	testutil.Plant(t, dsn, `INSERT INTO steps (id, workflow_id, name, status, idempotency_key, max_attempts,
		provider_ref, callback_deadline, dead_reason)
		VALUES ($1, $2, 'charge', $3, $4, 8, $5, now(), CASE WHEN $3 = 'DEAD' THEN 'attempts_exhausted_no_effect' END)`,
		id, wf, status, "k-"+id.String(), "po_"+id.String())
	return id
}

func TestStepTransitions_Table(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	for _, from := range stepStatuses {
		for _, to := range stepStatuses {
			t.Run(from+"→"+to, func(t *testing.T) {
				id := plantStep(t, from)
				_, err := pool.Exec(ctx, `UPDATE steps SET status = $2,
					dead_reason = CASE WHEN $2 = 'DEAD' THEN 'attempts_exhausted_no_effect' END WHERE id = $1`, id, to)
				if from == to || contains(allowedStep[from], to) {
					require.NoError(t, err)
					return
				}
				require.Error(t, err)
				require.Equal(t, "23514", pgCode(t, err))
				require.Contains(t, err.Error(), fmt.Sprintf("step %s: %s → %s", id, from, to))
			})
		}
	}
}

func TestMigrate_00018DownAndUp(t *testing.T) {
	ctx := context.Background()
	testutil.NewPool(t, dsn)
	owner := testutil.NewOwnerPool(t, dsn)
	t.Cleanup(func() { require.NoError(t, store.Migrate(ctx, owner)) })
	require.NoError(t, store.MigrateDownTo(ctx, owner, 17))
	var n int
	require.NoError(t, owner.QueryRow(ctx, `SELECT count(*) FROM pg_trigger WHERE tgname = 'steps_transition_check'`).Scan(&n))
	require.Zero(t, n)
	require.NoError(t, store.Migrate(ctx, owner))
}
