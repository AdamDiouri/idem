package invariants_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/invariants"
	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/testutil"
)

func i8(vs []invariants.Violation) []invariants.Violation {
	var out []invariants.Violation
	for _, v := range vs {
		if v.Invariant == "I8" {
			out = append(out, v)
		}
	}
	return out
}

// finished moves wf to status with its event, as a real transition does; the
// negative tests then break the event as the owner (the database refuses a
// terminal workflow without one, so the bad state must be planted after).
func finished(t *testing.T, e env, status string) uuid.UUID {
	t.Helper()
	wf, _ := newWorkflow(t, e.pool)
	testutil.SetWorkflowStatus(t, e.pool, wf, status)
	return wf
}

func ownerExec(t *testing.T, sql string, args ...any) {
	t.Helper()
	_, err := testutil.NewOwnerPool(t, dsn).Exec(context.Background(), sql, args...)
	require.NoError(t, err)
}

func plantEvent(t *testing.T, pool *pgxpool.Pool, wf uuid.UUID, typ, payload string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `INSERT INTO outbox (id, client_id, workflow_id, type, payload)
		VALUES ($1, 'c', $2, $3, convert_to($4, 'UTF8'))`, uuid.Must(uuid.NewV7()), wf, typ, payload)
	require.NoError(t, err)
}

// Slice 2.4, I8: every terminal workflow has exactly one outbox event, of the
// matching type, carrying its view; no other workflow has one.
func TestI8_CleanTerminalWorkflowHasNone(t *testing.T) {
	e := setup(t)
	finished(t, e, "FAILED")
	newWorkflow(t, e.pool) // a PENDING workflow, with no event
	require.Empty(t, i8(e.check(t)))
}

func TestI8_TerminalWithoutEvent(t *testing.T) {
	e := setup(t)
	wf := finished(t, e, "SUCCEEDED")
	ownerExec(t, `DELETE FROM outbox WHERE workflow_id = $1`, wf) // e.g. lost by hand
	v := i8(e.check(t))
	require.Len(t, v, 1)
	require.Equal(t, wf.String(), v[0].WorkflowID)
	require.Contains(t, v[0].Detail, "0 outbox events")
}

func TestI8_TwoEventsForOneWorkflow(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	wf := finished(t, e, "FAILED")
	owner := testutil.NewOwnerPool(t, dsn)
	_, err := owner.Exec(ctx, `ALTER TABLE outbox DROP CONSTRAINT outbox_workflow_id_type_key`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = owner.Exec(ctx, `DELETE FROM outbox`)
		_, err := owner.Exec(ctx, `ALTER TABLE outbox ADD CONSTRAINT outbox_workflow_id_type_key UNIQUE (workflow_id, type)`)
		require.NoError(t, err)
	})
	writeEvent(t, e.pool, wf)
	v := i8(e.check(t))
	require.Len(t, v, 1)
	require.Contains(t, v[0].Detail, "2 outbox events")
}

func TestI8_EventOnNonTerminalWorkflow(t *testing.T) {
	e := setup(t)
	wf, _ := newWorkflow(t, e.pool) // PENDING
	plantEvent(t, e.pool, wf, "workflow.succeeded", "{}")
	v := i8(e.check(t))
	require.Len(t, v, 1)
	require.Contains(t, v[0].Detail, "not terminal")
}

func TestI8_WrongType(t *testing.T) {
	e := setup(t)
	wf := finished(t, e, "FAILED")
	ownerExec(t, `UPDATE outbox SET type = 'workflow.succeeded' WHERE workflow_id = $1`, wf)
	v := i8(e.check(t))
	require.Len(t, v, 1)
	require.Contains(t, v[0].Detail, "workflow.succeeded")
}

// The frozen payload is what the shop is told: it must be the workflow's view.
func TestI8_PayloadDiffersFromView(t *testing.T) {
	e := setup(t)
	wf := finished(t, e, "FAILED")
	ownerExec(t, `UPDATE outbox SET payload = convert_to('{"status":"succeeded"}', 'UTF8') WHERE workflow_id = $1`, wf)
	v := i8(e.check(t))
	require.Len(t, v, 1)
	require.Contains(t, v[0].Detail, "payload")
}

// writeEvent writes wf's event the way transitionWorkflow does.
func writeEvent(t *testing.T, pool *pgxpool.Pool, wf uuid.UUID) {
	t.Helper()
	n, err := db.New(pool).InsertOutboxEvent(context.Background(), db.InsertOutboxEventParams{ID: uuid.Must(uuid.NewV7()), WorkflowID: wf})
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}
