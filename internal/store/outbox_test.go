package store_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/store"
	"github.com/AdamDiouri/idem/internal/testutil"
)

const viewInput = `{"amount_cents":10000,"fee_cents":175,"currency":"EUR","customer_id":"cus_1","merchant_id":"mer_1"}`

// insertWorkflow creates a workflow in status, as the owner (any status is allowed).
func insertWorkflow(t *testing.T, pool *pgxpool.Pool, client, status string) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	_, err := pool.Exec(context.Background(), `INSERT INTO workflows (id, client_id, type, status, input)
		VALUES ($1, $2, 'payment_payout', $3, $4)`, id, client, status, viewInput)
	require.NoError(t, err)
	return id
}

// insertFailedCharge records a permanently refused charge step with code (nil = NULL).
func insertFailedCharge(t *testing.T, pool *pgxpool.Pool, wf uuid.UUID, code *string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `INSERT INTO steps (id, workflow_id, name, status, idempotency_key, max_attempts, failure_code)
		VALUES ($1, $2, 'charge', 'FAILED_PERMANENT', $3, 8, $4)`, uuid.Must(uuid.NewV7()), wf, "k-"+wf.String(), code)
	require.NoError(t, err)
}

func view(t *testing.T, pool *pgxpool.Pool, wf uuid.UUID) *string {
	t.Helper()
	var v *string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT workflow_view($1)`, wf).Scan(&v))
	return v
}

func viewFields(t *testing.T, pool *pgxpool.Pool, wf uuid.UUID) map[string]any {
	t.Helper()
	v := view(t, pool, wf)
	require.NotNil(t, v)
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(*v), &m))
	return m
}

func ptr(s string) *string { return &s }

// Slice 2.4 (spec §5.1): the shop's view of a workflow, defined once in SQL so
// the outbox payload and GET /workflows/{id} cannot drift.
func TestWorkflowView_ShapesPerStatus(t *testing.T) {
	testutil.NewPool(t, dsn)
	owner := testutil.NewOwnerPool(t, dsn)

	for _, status := range []string{"PENDING", "RUNNING", "COMPENSATING", "NEEDS_ATTENTION"} {
		m := viewFields(t, owner, insertWorkflow(t, owner, "c", status))
		require.Equal(t, "processing", m["status"], status)
		require.NotContains(t, m, "reason", status)
	}

	succeeded := insertWorkflow(t, owner, "c", "SUCCEEDED")
	m := viewFields(t, owner, succeeded)
	require.Equal(t, "succeeded", m["status"])
	require.NotContains(t, m, "reason")
	require.Equal(t, succeeded.String(), m["workflow_id"])
	require.EqualValues(t, 10000, m["amount_cents"])
	require.EqualValues(t, 175, m["fee_cents"])
	require.Equal(t, "EUR", m["currency"])

	declined := insertWorkflow(t, owner, "c", "FAILED")
	insertFailedCharge(t, owner, declined, ptr("card_declined"))
	m = viewFields(t, owner, declined)
	require.Equal(t, "failed", m["status"])
	require.Equal(t, "card_declined", m["reason"])

	noCode := insertWorkflow(t, owner, "c", "FAILED")
	insertFailedCharge(t, owner, noCode, nil)
	require.Equal(t, "charge_failed", viewFields(t, owner, noCode)["reason"])

	failedByOperator := insertWorkflow(t, owner, "c", "FAILED") // admin fail: no FAILED_PERMANENT charge
	require.Equal(t, "charge_failed", viewFields(t, owner, failedByOperator)["reason"])

	compensated := insertWorkflow(t, owner, "c", "COMPENSATED")
	m = viewFields(t, owner, compensated)
	require.Equal(t, "compensated", m["status"])
	require.Equal(t, "payout_failed", m["reason"])

	// Keys in the documented order; the client sees no internals.
	v := *view(t, owner, declined)
	order := []string{`"workflow_id"`, `"status"`, `"reason"`, `"amount_cents"`, `"fee_cents"`, `"currency"`, `"created_at"`, `"updated_at"`}
	require.True(t, strings.HasPrefix(v, `{"workflow_id":`), v)
	last := -1
	for _, k := range order {
		i := strings.Index(v, k)
		require.Greater(t, i, last, "key %s out of order in %s", k, v)
		last = i
	}
	require.Len(t, viewFields(t, owner, declined), len(order))
	require.Regexp(t, `"created_at":"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z"`, v)
}

// Review focus 4: raw provider text never reaches the shop.
func TestWorkflowView_UnsafeFailureCodeFallsBack(t *testing.T) {
	testutil.NewPool(t, dsn)
	owner := testutil.NewOwnerPool(t, dsn)
	for _, code := range []string{"Card Declined", strings.Repeat("a", 200), "provider refused request: 402", "", "_x", "9lives"} {
		wf := insertWorkflow(t, owner, "c", "FAILED")
		insertFailedCharge(t, owner, wf, ptr(code))
		require.Equal(t, "charge_failed", viewFields(t, owner, wf)["reason"], "code %q", code)
	}
}

func TestWorkflowView_UnknownWorkflowIsNull(t *testing.T) {
	owner := testutil.NewOwnerPool(t, dsn)
	require.Nil(t, view(t, owner, uuid.Must(uuid.NewV7())))
}

func TestOutbox_OneEventPerWorkflowAndType(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	owner := testutil.NewOwnerPool(t, dsn)
	wf := insertWorkflow(t, owner, "c", "SUCCEEDED")
	insert := `INSERT INTO outbox (id, client_id, workflow_id, type, payload) VALUES ($1, 'c', $2, 'workflow.succeeded', '\x7b7d')`
	_, err := pool.Exec(context.Background(), insert, uuid.Must(uuid.NewV7()), wf)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), insert, uuid.Must(uuid.NewV7()), wf)
	require.Error(t, err)
	require.Equal(t, "23505", pgCode(t, err))
}

// The restricted application role writes and reads the outbox (the relay will
// update it in 2.5).
func TestOutbox_AppRoleMayInsertAndRead(t *testing.T) {
	pool := testutil.NewPool(t, dsn) // the restricted login
	owner := testutil.NewOwnerPool(t, dsn)
	wf := insertWorkflow(t, owner, "c", "FAILED")
	_, err := pool.Exec(context.Background(), `INSERT INTO outbox (id, client_id, workflow_id, type, payload)
		VALUES ($1, 'c', $2, 'workflow.failed', convert_to(workflow_view($2), 'UTF8'))`, uuid.Must(uuid.NewV7()), wf)
	require.NoError(t, err)
	var status string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT status FROM outbox WHERE workflow_id = $1`, wf).Scan(&status))
	require.Equal(t, "PENDING", status)
}

// Review focus 5: a database whose workflows finished before the outbox
// existed gets their events, so I8 holds after the upgrade.
func TestMigrate_00014BackfillsTerminalWorkflows(t *testing.T) {
	ctx := context.Background()
	testutil.NewPool(t, dsn)
	owner := testutil.NewOwnerPool(t, dsn)
	t.Cleanup(func() { require.NoError(t, store.Migrate(ctx, owner)) })
	require.NoError(t, store.MigrateDownTo(ctx, owner, 13))

	succeeded := insertWorkflow(t, owner, "c", "SUCCEEDED")
	failed := insertWorkflow(t, owner, "c", "FAILED")
	insertWorkflow(t, owner, "c", "RUNNING")
	require.NoError(t, store.Migrate(ctx, owner))

	rows, err := owner.Query(ctx, `SELECT o.workflow_id, o.type, convert_from(o.payload, 'UTF8') = workflow_view(o.workflow_id)
		FROM outbox o ORDER BY o.type DESC`)
	require.NoError(t, err)
	type ev struct {
		wf      uuid.UUID
		typ     string
		matches bool
	}
	var got []ev
	for rows.Next() {
		var e ev
		require.NoError(t, rows.Scan(&e.wf, &e.typ, &e.matches))
		got = append(got, e)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []ev{{succeeded, "workflow.succeeded", true}, {failed, "workflow.failed", true}}, got)
}
