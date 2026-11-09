package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
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

func submit(t *testing.T, s *workflow.Submitter, client, key string, req workflow.CreateWorkflowRequest) (workflow.SubmitResult, error) {
	t.Helper()
	return s.Submit(context.Background(), workflow.SubmitRequest{
		ClientID:       client,
		IdempotencyKey: key,
		Request:        req,
	})
}

func count(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n))
	return n
}

func requireCounts(t *testing.T, pool *pgxpool.Pool, want int) {
	t.Helper()
	require.Equal(t, want, count(t, pool, "workflows"), "workflows")
	require.Equal(t, want, count(t, pool, "steps"), "steps")
	require.Equal(t, want, count(t, pool, "api_idempotency_keys"), "api_idempotency_keys")
}

func decodeCreated(t *testing.T, body json.RawMessage) workflow.Created {
	t.Helper()
	var c workflow.Created
	require.NoError(t, json.Unmarshal(body, &c))
	return c
}

func TestSubmit_CreatesWorkflowStepAndKey(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	s := workflow.NewSubmitter(pool, 5*time.Second, defaultFees)

	res, err := submit(t, s, "client_a", "key-1", validReq())
	require.NoError(t, err)
	require.False(t, res.Replayed)

	created := decodeCreated(t, res.Body)
	require.NotEqual(t, uuid.Nil, created.WorkflowID)
	require.Equal(t, "PENDING", created.Status)
	require.Equal(t, int64(10000), created.AmountCents)
	require.Equal(t, int64(175), created.FeeCents)
	require.Equal(t, "EUR", created.Currency)
	require.False(t, created.CreatedAt.IsZero())

	var wfType, wfStatus, wfClient string
	var feeCents int64
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT type, status, client_id, (input->>'fee_cents')::bigint FROM workflows WHERE id = $1`,
		created.WorkflowID).Scan(&wfType, &wfStatus, &wfClient, &feeCents))
	require.Equal(t, "payment_payout", wfType)
	require.Equal(t, "PENDING", wfStatus)
	require.Equal(t, "client_a", wfClient)
	require.Equal(t, int64(175), feeCents)

	var name, status, key string
	var generation, maxAttempts int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT name, generation, status, max_attempts, idempotency_key FROM steps WHERE workflow_id = $1`,
		created.WorkflowID).Scan(&name, &generation, &status, &maxAttempts, &key))
	require.Equal(t, "charge", name)
	require.Equal(t, 0, generation)
	require.Equal(t, "PENDING", status)
	require.Equal(t, 8, maxAttempts)
	require.Equal(t, workflow.StepIdempotencyKey(created.WorkflowID, "charge", 0), key)

	var keyWorkflowID uuid.UUID
	var storedBody []byte
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT workflow_id, response_body FROM api_idempotency_keys WHERE client_id = 'client_a' AND key = 'key-1'`,
	).Scan(&keyWorkflowID, &storedBody))
	require.Equal(t, created.WorkflowID, keyWorkflowID)
	require.JSONEq(t, string(res.Body), string(storedBody))

	requireCounts(t, pool, 1)
}

// F1: a client retry with the same key and body replays the original response.
func TestSubmit_ReplaySameKeySameBody(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := workflow.NewSubmitter(pool, 5*time.Second, defaultFees)

	first, err := submit(t, s, "client_a", "key-1", validReq())
	require.NoError(t, err)
	second, err := submit(t, s, "client_a", "key-1", validReq())
	require.NoError(t, err)

	require.False(t, first.Replayed)
	require.True(t, second.Replayed)
	require.Equal(t, string(first.Body), string(second.Body), "replayed body must be byte-identical")
	requireCounts(t, pool, 1)
}

// F2: reusing a key with a different body is rejected and creates nothing.
func TestSubmit_SameKeyDifferentBody(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := workflow.NewSubmitter(pool, 5*time.Second, defaultFees)

	_, err := submit(t, s, "client_a", "key-1", validReq())
	require.NoError(t, err)

	other := validReq()
	other.AmountCents = 20000
	_, err = submit(t, s, "client_a", "key-1", other)
	require.ErrorIs(t, err, workflow.ErrKeyReuseMismatch)
	requireCounts(t, pool, 1)
}

func TestSubmit_KeyScopedPerClient(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := workflow.NewSubmitter(pool, 5*time.Second, defaultFees)

	a, err := submit(t, s, "client_a", "shared-key", validReq())
	require.NoError(t, err)
	b, err := submit(t, s, "client_b", "shared-key", validReq())
	require.NoError(t, err)

	require.False(t, a.Replayed)
	require.False(t, b.Replayed)
	require.NotEqual(t, decodeCreated(t, a.Body).WorkflowID, decodeCreated(t, b.Body).WorkflowID)
	requireCounts(t, pool, 2)
}

func TestSubmit_ReplayIgnoresNewFeeConfig(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	req := validReq()
	req.AmountCents = 100 // fee 27 under the original config

	original := workflow.NewSubmitter(pool, 5*time.Second, defaultFees)
	_, err := submit(t, original, "client_a", "key-1", req)
	require.NoError(t, err)

	// Under the raised config 100 cents no longer covers the fee; the replay
	// must still succeed because the workflow already exists (review C1).
	raised := workflow.NewSubmitter(pool, 5*time.Second, workflow.FeeConfig{RateBps: 150, FixedCents: 100})
	res, err := submit(t, raised, "client_a", "key-1", req)
	require.NoError(t, err)

	require.True(t, res.Replayed)
	require.Equal(t, int64(27), decodeCreated(t, res.Body).FeeCents)
	requireCounts(t, pool, 1)
}

func TestSubmit_ValidationErrorReleasesKey(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := workflow.NewSubmitter(pool, 5*time.Second, defaultFees)

	bad := validReq()
	bad.Currency = "USD"
	_, err := submit(t, s, "client_a", "key-1", bad)
	var ve *workflow.ValidationError
	require.True(t, errors.As(err, &ve), "expected *ValidationError, got %v", err)
	require.Equal(t, "currency", ve.Field)
	requireCounts(t, pool, 0)

	res, err := submit(t, s, "client_a", "key-1", validReq())
	require.NoError(t, err)
	require.False(t, res.Replayed)
	requireCounts(t, pool, 1)
}

// F3: concurrent identical submissions create exactly one workflow.
func TestSubmit_ConcurrentSameKey(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := workflow.NewSubmitter(pool, 5*time.Second, defaultFees)

	const n = 20
	req := workflow.SubmitRequest{
		ClientID:       "client_a",
		IdempotencyKey: "key-1",
		Request:        validReq(),
	}

	start := make(chan struct{})
	results := make([]workflow.SubmitResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = s.Submit(context.Background(), req)
		}()
	}
	close(start)
	wg.Wait()

	fresh := 0
	var ids []uuid.UUID
	for i := range n {
		require.NoError(t, errs[i])
		if !results[i].Replayed {
			fresh++
		}
		ids = append(ids, decodeCreated(t, results[i].Body).WorkflowID)
	}
	require.Equal(t, 1, fresh, "exactly one request must create the workflow")
	for _, id := range ids {
		require.Equal(t, ids[0], id)
	}
	requireCounts(t, pool, 1)
}

func TestSubmit_KeyHeldByUncommittedTxReturnsErrKeyInUse(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	s := workflow.NewSubmitter(pool, 200*time.Millisecond, defaultFees)

	txA, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = txA.Exec(ctx,
		`INSERT INTO api_idempotency_keys (client_id, key, request_fingerprint) VALUES ('client_a', 'key-1', 'x')`)
	require.NoError(t, err)

	_, err = submit(t, s, "client_a", "key-1", validReq())
	require.ErrorIs(t, err, workflow.ErrKeyInUse)

	require.NoError(t, txA.Rollback(ctx))
	res, err := submit(t, s, "client_a", "key-1", validReq())
	require.NoError(t, err)
	require.False(t, res.Replayed)
	requireCounts(t, pool, 1)
}

// A failure after all inserts but before COMMIT must leave no trace, so the
// client's retry with the same key starts from scratch.
func TestSubmit_FailureBeforeCommitLeavesNoTrace(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := workflow.NewSubmitter(pool, 5*time.Second, defaultFees)

	boom := errors.New("boom")
	workflow.SetBeforeCommit(s, func() error { return boom })
	_, err := submit(t, s, "client_a", "key-1", validReq())
	require.ErrorIs(t, err, boom)
	requireCounts(t, pool, 0)

	workflow.SetBeforeCommit(s, nil)
	res, err := submit(t, s, "client_a", "key-1", validReq())
	require.NoError(t, err)
	require.False(t, res.Replayed)
	requireCounts(t, pool, 1)
}
