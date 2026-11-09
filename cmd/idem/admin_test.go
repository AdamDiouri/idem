package main

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/workflow"
)

var dsn string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunWithPostgres(m, &dsn))
}

// parkedCharge creates a workflow whose charge step is DEAD (no money moved)
// and returns the workflow and step IDs.
func parkedCharge(t *testing.T, pool *pgxpool.Pool) (uuid.UUID, uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	s := workflow.NewSubmitter(pool, 5*time.Second, workflow.FeeConfig{RateBps: 150, FixedCents: 25})
	_, err := s.Submit(ctx, workflow.SubmitRequest{
		ClientID: "c", IdempotencyKey: uuid.NewString(),
		Request: workflow.CreateWorkflowRequest{AmountCents: 10000, Currency: "EUR", CustomerID: "cus_1", MerchantID: "mer_1"},
	})
	require.NoError(t, err)
	var wf, step uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT workflow_id, id FROM steps ORDER BY created_at DESC LIMIT 1`).Scan(&wf, &step))
	_, err = pool.Exec(ctx, `UPDATE steps SET status = 'DEAD', dead_reason = 'attempts_exhausted_no_effect', attempt_count = 8 WHERE id = $1`, step)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE workflows SET status = 'NEEDS_ATTENTION' WHERE id = $1`, wf)
	require.NoError(t, err)
	return wf, step
}

func admin(t *testing.T, args ...string) (int, string) {
	t.Helper()
	t.Setenv("IDEM_DATABASE_URL", dsn)
	t.Setenv("IDEM_ADMIN_ACTOR", "cli-test")
	var out bytes.Buffer
	code := runAdmin(args, &out, &out)
	return code, out.String()
}

func TestAdminCLI_ListTable(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf, _ := parkedCharge(t, pool)

	code, out := admin(t, "list")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "WORKFLOW")
	require.Contains(t, out, "REASON")
	require.Contains(t, out, wf.String())
	require.Contains(t, out, "attempts_exhausted_no_effect")
}

func TestAdminCLI_MissingReasonIsUsageError(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	_, step := parkedCharge(t, pool)

	code, out := admin(t, "retry-step", step.String())
	require.Equal(t, 2, code, out)
	require.Contains(t, out, "reason")
}

func TestAdminCLI_BadUUIDIsUsageError(t *testing.T) {
	testutil.NewPool(t, dsn)
	code, _ := admin(t, "retry-step", "not-a-uuid", "--reason", "x")
	require.Equal(t, 2, code)
}

func TestAdminCLI_NewGenerationNeedsConfirmFlag(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	_, step := parkedCharge(t, pool)

	code, out := admin(t, "new-generation", step.String(), "--reason", "verified")
	require.NotEqual(t, 0, code)
	require.Contains(t, out, "confirm-no-effect")
}

func TestAdminCLI_RetryStep(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf, step := parkedCharge(t, pool)

	code, out := admin(t, "retry-step", step.String(), "--reason", "rate limit lifted")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, step.String())
	var status, wfStatus, actor string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT status FROM steps WHERE id = $1`, step).Scan(&status))
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT status FROM workflows WHERE id = $1`, wf).Scan(&wfStatus))
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT actor FROM admin_actions WHERE workflow_id = $1`, wf).Scan(&actor))
	require.Equal(t, "PENDING", status)
	require.Equal(t, "RUNNING", wfStatus)
	require.Equal(t, "cli-test", actor)
}

func TestAdminCLI_NotActionableExitsOne(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	_, step := parkedCharge(t, pool)
	code, _ := admin(t, "retry-step", step.String(), "--reason", "first")
	require.Equal(t, 0, code)

	code, out := admin(t, "retry-step", step.String(), "--reason", "second") // no longer parked
	require.Equal(t, 1, code)
	require.Contains(t, out, "not actionable")
}
