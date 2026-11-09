package engine_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/workflow"
)

var dsn string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunWithPostgres(m, &dsn))
}

var charge = []string{"charge"}

// stubStep records hook calls; it never calls a provider.
type stubStep struct {
	mu            sync.Mutex
	successCalls  int
	failureCalls  int
	successErr    error
	successErrFor int // return successErr for the first N OnSuccess calls
	execute       func(context.Context, engine.ClaimedStep) provider.Outcome
}

func (s *stubStep) Name() string { return "charge" }

func (s *stubStep) Execute(ctx context.Context, c engine.ClaimedStep) provider.Outcome {
	if s.execute != nil {
		return s.execute(ctx, c)
	}
	return provider.Outcome{Class: provider.Success, ProviderRef: "ch_stub", HTTPStatus: 201}
}

func (s *stubStep) OnSuccess(context.Context, *db.Queries, engine.ClaimedStep, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.successCalls++
	if s.successErr != nil && s.successCalls <= s.successErrFor {
		return s.successErr
	}
	return nil
}

func (s *stubStep) OnPermanentFailure(context.Context, *db.Queries, engine.ClaimedStep) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failureCalls++
	return nil
}

func submit(t *testing.T, pool *pgxpool.Pool, customer string) uuid.UUID {
	t.Helper()
	s := workflow.NewSubmitter(pool, 5*time.Second, workflow.FeeConfig{RateBps: 150, FixedCents: 25})
	res, err := s.Submit(context.Background(), workflow.SubmitRequest{
		ClientID:       "c",
		IdempotencyKey: uuid.NewString(),
		Request:        workflow.CreateWorkflowRequest{AmountCents: 10000, Currency: "EUR", CustomerID: customer, MerchantID: "mer_1"},
	})
	require.NoError(t, err)
	var created workflow.Created
	require.NoError(t, jsonUnmarshal(res.Body, &created))
	return created.WorkflowID
}

func mustClaim(t *testing.T, pool *pgxpool.Pool, worker string, ttl time.Duration) engine.ClaimedStep {
	t.Helper()
	s, ok, err := engine.Claim(context.Background(), pool, worker, charge, ttl)
	require.NoError(t, err)
	require.True(t, ok, "expected a claimable step")
	return s
}

type stepRow struct {
	status, leaseOwner, providerRef, lastError string
	epoch                                      int64
	attempts                                   int32
	leaseValid                                 bool
}

func readStep(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) stepRow {
	t.Helper()
	var r stepRow
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT status, COALESCE(lease_owner, ''), COALESCE(provider_ref, ''), COALESCE(last_error, ''),
		       lease_epoch, attempt_count, COALESCE(lease_expires_at > now(), false)
		FROM steps WHERE id = $1`, id).
		Scan(&r.status, &r.leaseOwner, &r.providerRef, &r.lastError, &r.epoch, &r.attempts, &r.leaseValid))
	return r
}

func attemptOutcome(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, attempt int32) *string {
	t.Helper()
	var outcome *string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT outcome FROM step_attempts WHERE step_id = $1 AND attempt_no = $2`, id, attempt).Scan(&outcome))
	return outcome
}

func scalar[T any](t *testing.T, pool *pgxpool.Pool, query string, args ...any) T {
	t.Helper()
	var v T
	require.NoError(t, pool.QueryRow(context.Background(), query, args...).Scan(&v))
	return v
}

func TestClaim_NothingClaimable(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	_, ok, err := engine.Claim(context.Background(), pool, "w1", charge, time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestClaim_ClaimsPendingStep(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf := submit(t, pool, "cus_1")

	s := mustClaim(t, pool, "w1", time.Minute)
	require.Equal(t, wf, s.WorkflowID)
	require.Equal(t, "charge", s.Name)
	require.Equal(t, int64(1), s.LeaseEpoch)
	require.Equal(t, int32(1), s.Attempt)
	require.Equal(t, "PENDING", s.PriorStatus)
	require.Equal(t, int64(175), s.Input.FeeCents)
	require.Equal(t, workflow.StepIdempotencyKey(wf, "charge", 0), s.IdempotencyKey)

	row := readStep(t, pool, s.ID)
	require.Equal(t, "RUNNING", row.status)
	require.Equal(t, "w1", row.leaseOwner)
	require.True(t, row.leaseValid)
	require.Equal(t, "RUNNING", scalar[string](t, pool, `SELECT status FROM workflows WHERE id = $1`, wf))
	require.Nil(t, attemptOutcome(t, pool, s.ID, 1))
	require.Equal(t, int64(1), scalar[int64](t, pool, `SELECT lease_epoch FROM step_attempts WHERE step_id = $1`, s.ID))
	require.Equal(t, "w1", scalar[string](t, pool, `SELECT worker_id FROM step_attempts WHERE step_id = $1`, s.ID))
}

func TestClaim_FiltersByName(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	_, ok, err := engine.Claim(context.Background(), pool, "w1", []string{"payout"}, time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestClaim_RespectsRunAfter(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	_, err := pool.Exec(context.Background(), `UPDATE steps SET run_after = now() + interval '1 hour'`)
	require.NoError(t, err)
	_, ok, err := engine.Claim(context.Background(), pool, "w1", charge, time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestClaim_ConcurrentWorkersNeverShareAStep(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	for range 5 {
		submit(t, pool, "cus_1")
	}

	var mu sync.Mutex
	claimed := map[uuid.UUID]int{}
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				s, ok, err := engine.Claim(context.Background(), pool, "w"+string(rune('a'+i)), charge, time.Minute)
				if err != nil || !ok {
					return
				}
				mu.Lock()
				claimed[s.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	require.Len(t, claimed, 5)
	for id, n := range claimed {
		require.Equal(t, 1, n, "step %s claimed %d times", id, n)
	}
}

func TestComplete_Succeeded(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", time.Minute)
	step := &stubStep{}

	require.NoError(t, engine.Complete(context.Background(), pool, s,
		engine.Result{Kind: engine.ResultSucceeded, ProviderRef: "ch_1", HTTPStatus: 201}, step))

	row := readStep(t, pool, s.ID)
	require.Equal(t, "SUCCEEDED", row.status)
	require.Equal(t, "ch_1", row.providerRef)
	require.Equal(t, "", row.leaseOwner)
	require.Equal(t, "SUCCEEDED", *attemptOutcome(t, pool, s.ID, 1))
	require.Equal(t, 1, step.successCalls)
	require.Equal(t, 0, step.failureCalls)
}

func TestComplete_FailedPermanent(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", time.Minute)
	step := &stubStep{}

	require.NoError(t, engine.Complete(context.Background(), pool, s,
		engine.Result{Kind: engine.ResultFailedPermanent, HTTPStatus: 402, Error: "declined"}, step))

	row := readStep(t, pool, s.ID)
	require.Equal(t, "FAILED_PERMANENT", row.status)
	require.Equal(t, "declined", row.lastError)
	require.Equal(t, "FAILED_PERMANENT", *attemptOutcome(t, pool, s.ID, 1))
	require.Equal(t, 0, step.successCalls)
	require.Equal(t, 1, step.failureCalls)
}

func TestComplete_InDoubtSetsRunAfter(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", time.Minute)
	step := &stubStep{}

	require.NoError(t, engine.Complete(context.Background(), pool, s,
		engine.Result{Kind: engine.ResultInDoubt, Error: "timeout", RetryDelay: time.Hour}, step))

	row := readStep(t, pool, s.ID)
	require.Equal(t, "IN_DOUBT", row.status)
	require.Equal(t, "timeout", row.lastError)
	require.True(t, scalar[bool](t, pool, `SELECT run_after > now() + interval '59 minutes' FROM steps WHERE id = $1`, s.ID))
	require.Equal(t, 0, step.successCalls+step.failureCalls)

	_, ok, err := engine.Claim(context.Background(), pool, "w2", charge, time.Minute)
	require.NoError(t, err)
	require.False(t, ok, "not claimable before run_after")
}

func TestComplete_StaleEpochIsFenced(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", time.Minute)
	step := &stubStep{}

	stale := s
	stale.LeaseEpoch--
	err := engine.Complete(context.Background(), pool, stale, engine.Result{Kind: engine.ResultSucceeded, ProviderRef: "ch_1"}, step)
	require.ErrorIs(t, err, engine.ErrFenced)

	row := readStep(t, pool, s.ID)
	require.Equal(t, "RUNNING", row.status)
	require.Equal(t, "", row.providerRef)
	require.Equal(t, 0, step.successCalls)
}

func TestComplete_HookErrorRollsBack(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", time.Minute)
	boom := errors.New("boom")
	step := &stubStep{successErr: boom, successErrFor: 1}

	err := engine.Complete(context.Background(), pool, s, engine.Result{Kind: engine.ResultSucceeded, ProviderRef: "ch_1"}, step)
	require.ErrorIs(t, err, boom)

	row := readStep(t, pool, s.ID)
	require.Equal(t, "RUNNING", row.status, "lease-expiry path must recover it")
	require.Nil(t, attemptOutcome(t, pool, s.ID, 1))
}

func TestReclaim_OnlyExpiredLeases(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	submit(t, pool, "cus_2")
	a := mustClaim(t, pool, "wA", time.Hour)
	b := mustClaim(t, pool, "wB", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	n, err := engine.ReclaimExpiredLeases(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	rb := readStep(t, pool, b.ID)
	require.Equal(t, "IN_DOUBT", rb.status)
	require.Equal(t, int64(2), rb.epoch)
	require.Equal(t, "", rb.leaseOwner)
	require.Equal(t, "ABANDONED", *attemptOutcome(t, pool, b.ID, 1))

	ra := readStep(t, pool, a.ID)
	require.Equal(t, "RUNNING", ra.status)
	require.Equal(t, int64(1), ra.epoch)
}

func TestReclaim_ThenCompleteIsFenced(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, err := engine.ReclaimExpiredLeases(context.Background(), pool)
	require.NoError(t, err)

	err = engine.Complete(context.Background(), pool, s, engine.Result{Kind: engine.ResultSucceeded, ProviderRef: "ch_1"}, &stubStep{})
	require.ErrorIs(t, err, engine.ErrFenced)
	require.Equal(t, "IN_DOUBT", readStep(t, pool, s.ID).status)
}

func TestReclaim_InDoubtIsReclaimableWithSameKey(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	first := mustClaim(t, pool, "w1", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, err := engine.ReclaimExpiredLeases(context.Background(), pool)
	require.NoError(t, err)

	second := mustClaim(t, pool, "w2", time.Minute)
	require.Equal(t, first.ID, second.ID)
	require.Equal(t, "IN_DOUBT", second.PriorStatus)
	require.Equal(t, int64(3), second.LeaseEpoch)
	require.Equal(t, int32(2), second.Attempt)
	require.Equal(t, first.IdempotencyKey, second.IdempotencyKey)
}

func TestReclaim_ConcurrentSweepersBumpOnce(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	var total int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := engine.ReclaimExpiredLeases(context.Background(), pool)
			require.NoError(t, err)
			mu.Lock()
			total += n
			mu.Unlock()
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), total)
	require.Equal(t, int64(2), readStep(t, pool, s.ID).epoch)
}
