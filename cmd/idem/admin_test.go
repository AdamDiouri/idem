package main

import (
	"bytes"
	"context"
	"os"
	"strconv"
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

// version is the workflow version an operator sees in `idem admin list`.
func version(t *testing.T, pool *pgxpool.Pool, wf uuid.UUID) string {
	t.Helper()
	var v int64
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT version FROM workflows WHERE id = $1`, wf).Scan(&v))
	return strconv.FormatInt(v, 10)
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
	require.Contains(t, out, "VERSION")
	require.Contains(t, out, wf.String())
	require.Contains(t, out, "attempts_exhausted_no_effect")
}

func TestAdminCLI_MissingReasonIsUsageError(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf, step := parkedCharge(t, pool)

	code, out := admin(t, "retry-step", step.String(), "--expect-version", version(t, pool, wf))
	require.Equal(t, 2, code, out)
	require.Contains(t, out, "reason")
}

func TestAdminCLI_BadUUIDIsUsageError(t *testing.T) {
	testutil.NewPool(t, dsn)
	code, _ := admin(t, "retry-step", "not-a-uuid", "--reason", "x", "--expect-version", "1")
	require.Equal(t, 2, code)
}

func TestAdminCLI_NewGenerationNeedsConfirmFlag(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf, step := parkedCharge(t, pool)

	code, out := admin(t, "new-generation", step.String(), "--reason", "verified", "--expect-version", version(t, pool, wf))
	require.NotEqual(t, 0, code)
	require.Contains(t, out, "confirm-no-effect")
}

func TestAdminCLI_RetryStep(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf, step := parkedCharge(t, pool)

	code, out := admin(t, "retry-step", step.String(), "--reason", "rate limit lifted", "--expect-version", version(t, pool, wf))
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
	wf, step := parkedCharge(t, pool)
	v := version(t, pool, wf)
	code, _ := admin(t, "retry-step", step.String(), "--reason", "first", "--expect-version", v)
	require.Equal(t, 0, code)

	code, out := admin(t, "retry-step", step.String(), "--reason", "second", "--expect-version", v) // no longer parked
	require.Equal(t, 1, code)
	require.Contains(t, out, "not actionable")
}

// Deferred minor 1: every mutating command must name the version the operator
// saw; without it the command is a usage error and nothing changes.
func TestAdminCLI_MutationsRequireExpectVersion(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf, step := parkedCharge(t, pool)
	for _, args := range [][]string{
		{"retry-step", step.String()},
		{"confirm-step", step.String(), "--provider-ref", "ch_1"},
		{"new-generation", step.String(), "--confirm-no-effect"},
		{"compensate", wf.String()},
		{"fail", wf.String()},
	} {
		code, out := admin(t, append(args, "--reason", "r")...)
		require.Equal(t, 2, code, "%v: %s", args, out)
		require.Contains(t, out, "expect-version", args)
	}
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM admin_actions WHERE workflow_id = $1`, wf).Scan(&n))
	require.Zero(t, n)
}

func TestAdminCLI_StaleVersionIsRefused(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf, _ := parkedCharge(t, pool)
	_, err := pool.Exec(context.Background(), `UPDATE workflows SET version = version + 2 WHERE id = $1`, wf) // left and came back
	require.NoError(t, err)
	code, out := admin(t, "fail", wf.String(), "--reason", "r", "--expect-version", "0")
	require.Equal(t, 1, code, out)
	require.Contains(t, out, "stale")
	require.Contains(t, out, "admin list")
}
