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
	panicFor      int // panic in the first N OnSuccess calls
	execute       func(context.Context, engine.ClaimedStep) provider.Outcome
}

func (s *stubStep) Name() string { return "charge" }
func (s *stubStep) Pivot() bool  { return false }

func (s *stubStep) Execute(ctx context.Context, c engine.ClaimedStep) provider.Outcome {
	if s.execute != nil {
		return s.execute(ctx, c)
	}
	return provider.Outcome{Class: provider.Success, ProviderRef: "ch_stub", HTTPStatus: 201}
}

func (s *stubStep) Lookup(context.Context, engine.ClaimedStep) (provider.Outcome, bool) {
	return provider.Outcome{Class: provider.Ambiguous}, false
}

func (s *stubStep) OnSuccess(context.Context, *db.Queries, engine.ClaimedStep, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.successCalls++
	if s.successCalls <= s.panicFor {
		panic("bug in hook")
	}
	if s.successErr != nil && s.successCalls <= s.successErrFor {
		return s.successErr
	}
	return nil
}

func (s *stubStep) successes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.successCalls
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

// claimFromInDoubt returns a claim whose PriorStatus is IN_DOUBT (claimed,
// lease expired, reclaimed, claimed again).
func claimFromInDoubt(t *testing.T, pool *pgxpool.Pool) engine.ClaimedStep {
	t.Helper()
	mustClaim(t, pool, "w1", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	n, err := engine.ReclaimExpiredLeases(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	s := mustClaim(t, pool, "w2", time.Minute)
	require.Equal(t, "IN_DOUBT", s.PriorStatus)
	return s
}

func TestClaim_ReturnsMaxAttemptsAndKeyAge(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	first := mustClaim(t, pool, "w1", 50*time.Millisecond)
	require.Equal(t, int32(8), first.MaxAttempts)
	require.Less(t, first.KeyAge, 100*time.Millisecond)

	time.Sleep(250 * time.Millisecond)
	_, err := engine.ReclaimExpiredLeases(context.Background(), pool)
	require.NoError(t, err)
	second := mustClaim(t, pool, "w2", time.Minute)
	require.GreaterOrEqual(t, second.KeyAge, 200*time.Millisecond, "key age counts from the first attempt")
}

func TestComplete_RetryNoEffectFromPendingIsPending(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", time.Minute)
	step := &stubStep{}

	require.NoError(t, engine.Complete(context.Background(), pool, s,
		engine.Result{Kind: engine.ResultRetryNoEffect, HTTPStatus: 429, Error: "rate limited", RetryDelay: time.Hour}, step))

	require.Equal(t, "PENDING", readStep(t, pool, s.ID).status)
	require.True(t, scalar[bool](t, pool, `SELECT run_after > now() + interval '59 minutes' FROM steps WHERE id = $1`, s.ID))
	require.Equal(t, "RETRY_NO_EFFECT", *attemptOutcome(t, pool, s.ID, 1))
	require.Equal(t, 0, step.successCalls+step.failureCalls)
}

func TestComplete_RetryNoEffectFromInDoubtStaysInDoubt(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	s := claimFromInDoubt(t, pool)

	require.NoError(t, engine.Complete(context.Background(), pool, s,
		engine.Result{Kind: engine.ResultRetryNoEffect, HTTPStatus: 429}, &stubStep{}))
	require.Equal(t, "IN_DOUBT", readStep(t, pool, s.ID).status, "ambiguity is sticky")
}

func TestComplete_DeadSetsNeedsAttention(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf := submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", time.Minute)
	step := &stubStep{}

	require.NoError(t, engine.Complete(context.Background(), pool, s,
		engine.Result{Kind: engine.ResultDead, DeadReason: "attempts_exhausted_in_doubt", Error: "timeout"}, step))

	require.Equal(t, "DEAD", readStep(t, pool, s.ID).status)
	require.Equal(t, "attempts_exhausted_in_doubt", scalar[string](t, pool, `SELECT dead_reason FROM steps WHERE id = $1`, s.ID))
	require.Equal(t, "NEEDS_ATTENTION", scalar[string](t, pool, `SELECT status FROM workflows WHERE id = $1`, wf))
	require.Equal(t, "DEAD", *attemptOutcome(t, pool, s.ID, 1))
	require.Equal(t, 0, step.successCalls+step.failureCalls)

	_, ok, err := engine.Claim(context.Background(), pool, "w2", charge, time.Minute)
	require.NoError(t, err)
	require.False(t, ok, "a DEAD step is never claimed again")
}

func TestComplete_DeadIsFenced(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf := submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", time.Minute)
	stale := s
	stale.LeaseEpoch--

	err := engine.Complete(context.Background(), pool, stale,
		engine.Result{Kind: engine.ResultDead, DeadReason: "attempts_exhausted_in_doubt"}, &stubStep{})
	require.ErrorIs(t, err, engine.ErrFenced)
	require.Equal(t, "RUNNING", readStep(t, pool, s.ID).status)
	require.Equal(t, "RUNNING", scalar[string](t, pool, `SELECT status FROM workflows WHERE id = $1`, wf))
}

func TestSchema_DeadRequiresReason(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	_, err := pool.Exec(context.Background(), `UPDATE steps SET status = 'DEAD'`)
	require.Error(t, err)
	var code string
	require.True(t, func() bool {
		var pgErr interface{ SQLState() string }
		if errors.As(err, &pgErr) {
			code = pgErr.SQLState()
			return true
		}
		return false
	}())
	require.Equal(t, "23514", code)
}

func TestFindStuckWorkflows(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	stuck := submit(t, pool, "cus_1")
	oldDone := submit(t, pool, "cus_2")
	submit(t, pool, "cus_3") // fresh
	_, err := pool.Exec(ctx, `UPDATE workflows SET status = 'RUNNING', updated_at = now() - interval '2 hours' WHERE id = $1`, stuck)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE workflows SET status = 'SUCCEEDED', updated_at = now() - interval '2 hours' WHERE id = $1`, oldDone)
	require.NoError(t, err)

	got, err := engine.FindStuckWorkflows(ctx, pool, time.Hour)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, stuck, got[0].ID)
	require.Equal(t, "RUNNING", got[0].Status)
	require.GreaterOrEqual(t, got[0].Idle, 2*time.Hour-time.Minute)
}

func insertRefundStep(t *testing.T, pool *pgxpool.Pool, wf uuid.UUID, ref *string) error {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO steps (id, workflow_id, name, generation, status, idempotency_key, max_attempts, refund_charge_ref)
		VALUES (gen_random_uuid(), $1, 'refund', 0, 'PENDING', $2, 20, $3)`, wf, uuid.NewString(), ref)
	return err
}

func TestClaim_ReturnsRefundChargeRef(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf := submit(t, pool, "cus_1")
	ref := "ch_1"
	require.NoError(t, insertRefundStep(t, pool, wf, &ref))

	s, ok, err := engine.Claim(context.Background(), pool, "w1", []string{"refund"}, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "ch_1", s.RefundChargeRef)

	c := mustClaim(t, pool, "w1", time.Minute)
	require.Equal(t, "", c.RefundChargeRef)
}

func TestSchema_RefundRequiresChargeRef(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	wf := submit(t, pool, "cus_1")

	err := insertRefundStep(t, pool, wf, nil)
	require.Error(t, err)
	var pgErr interface{ SQLState() string }
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, "23514", pgErr.SQLState())

	_, err = pool.Exec(context.Background(), `UPDATE steps SET refund_charge_ref = 'ch_1' WHERE name = 'charge'`)
	require.Error(t, err, "only refunds carry a charge reference")
}

func TestTransitionWorkflow_RejectsUnexpectedStatus(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	wf := submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", time.Minute)
	_, err := pool.Exec(ctx, `UPDATE workflows SET status = 'NEEDS_ATTENTION' WHERE id = $1`, wf) // a human got there first
	require.NoError(t, err)

	err = engine.Complete(ctx, pool, s, engine.Result{Kind: engine.ResultFailedPermanent, HTTPStatus: 402},
		engine.NewChargeStep(nil))
	require.ErrorIs(t, err, engine.ErrUnexpectedWorkflowStatus)
	require.Equal(t, "RUNNING", readStep(t, pool, s.ID).status, "completion rolled back")
	require.Equal(t, "NEEDS_ATTENTION", scalar[string](t, pool, `SELECT status FROM workflows WHERE id = $1`, wf))
}

// Review finding: a workflow that is parked (NEEDS_ATTENTION) or finished must
// not have its steps claimed — otherwise money moves with nowhere to record it.
func TestClaim_SkipsStepsOfInactiveWorkflows(t *testing.T) {
	for _, status := range []string{"NEEDS_ATTENTION", "SUCCEEDED", "FAILED", "COMPENSATED"} {
		t.Run(status, func(t *testing.T) {
			pool := testutil.NewPool(t, dsn)
			wf := submit(t, pool, "cus_1")
			_, err := pool.Exec(context.Background(), `UPDATE workflows SET status = $2 WHERE id = $1`, wf, status)
			require.NoError(t, err)

			_, ok, err := engine.Claim(context.Background(), pool, "w1", charge, time.Minute)
			require.NoError(t, err)
			require.False(t, ok)
		})
	}
}

func TestComplete_ChargeSuccessOnParkedWorkflowRollsBack(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	wf := submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", time.Minute)
	_, err := pool.Exec(ctx, `UPDATE workflows SET status = 'NEEDS_ATTENTION' WHERE id = $1`, wf)
	require.NoError(t, err)

	err = engine.Complete(ctx, pool, s, engine.Result{Kind: engine.ResultSucceeded, ProviderRef: "ch_1"}, engine.NewChargeStep(nil))
	require.ErrorIs(t, err, engine.ErrUnexpectedWorkflowStatus)
	require.Equal(t, 0, scalar[int](t, pool, `SELECT count(*) FROM steps WHERE workflow_id = $1 AND name = 'payout'`, wf),
		"a parked workflow must not advance to the payout")
}

func TestSchema_RefundChargeRefIsImmutable(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	wf := submit(t, pool, "cus_1")
	ref := "ch_1"
	require.NoError(t, insertRefundStep(t, pool, wf, &ref))

	_, err := pool.Exec(ctx, `UPDATE steps SET refund_charge_ref = 'ch_2' WHERE name = 'refund'`)
	require.Error(t, err)
	var pgErr interface{ SQLState() string }
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, "42501", pgErr.SQLState())

	_, err = pool.Exec(ctx, `UPDATE steps SET status = 'IN_DOUBT' WHERE name = 'refund'`)
	require.NoError(t, err, "other columns stay updatable")
}

// new-generation can leave several charge steps; the refund must reverse the
// one that actually succeeded last.
func TestChargeProviderRef_PicksHighestGeneration(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	wf := submit(t, pool, "cus_1")
	_, err := pool.Exec(ctx, `UPDATE steps SET status = 'SUCCEEDED', provider_ref = 'ch_a' WHERE workflow_id = $1`, wf)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO steps (id, workflow_id, name, generation, status, idempotency_key, max_attempts, provider_ref)
		VALUES (gen_random_uuid(), $1, 'charge', 1, 'SUCCEEDED', $2, 8, 'ch_b'),
		       (gen_random_uuid(), $1, 'payout', 0, 'PENDING', $3, 8, NULL)`, wf, uuid.NewString(), uuid.NewString())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE workflows SET status = 'RUNNING' WHERE id = $1`, wf)
	require.NoError(t, err)

	s, ok, err := engine.Claim(ctx, pool, "w1", []string{"payout"}, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, engine.Complete(ctx, pool, s, engine.Result{Kind: engine.ResultFailedPermanent, HTTPStatus: 422}, engine.NewPayoutStep(nil)))
	require.Equal(t, "ch_b", scalar[string](t, pool, `SELECT refund_charge_ref FROM steps WHERE workflow_id = $1 AND name = 'refund'`, wf))
}

// awaitingStep makes a fresh workflow's charge step look like an accepted
// async request: one finished send attempt (sendOutcome), AWAITING_CALLBACK
// with provider_ref po_test, pollable at now() + runAfter.
func awaitingStep(t *testing.T, pool *pgxpool.Pool, runAfter, sendOutcome string) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	wf := submit(t, pool, "cus_1")
	step := scalar[uuid.UUID](t, pool, `SELECT id FROM steps WHERE workflow_id = $1`, wf)
	_, err := pool.Exec(ctx, `UPDATE workflows SET status = 'RUNNING' WHERE id = $1`, wf)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE steps SET status = 'AWAITING_CALLBACK', provider_ref = 'po_test-'||id::text, attempt_count = 1,
		first_attempted_at = now(), run_after = now() + $2::interval, callback_deadline = now() + $2::interval WHERE id = $1`, step, runAfter)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO step_attempts (step_id, attempt_no, lease_epoch, worker_id, outcome, finished_at)
		VALUES ($1, 1, 1, 'w0', $2, now())`, step, sendOutcome)
	require.NoError(t, err)
	return step
}

// Slice 2.1: an accepted step is pollable only once its callback deadline passed.
func TestClaim_AwaitingCallbackOnlyAfterDeadline(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	step := awaitingStep(t, pool, "1 hour", "AWAITING_CALLBACK")
	_, ok, err := engine.Claim(ctx, pool, "w1", charge, time.Minute)
	require.NoError(t, err)
	require.False(t, ok, "not before the deadline")

	_, err = pool.Exec(ctx, `UPDATE steps SET run_after = now() - interval '1 second' WHERE id = $1`, step)
	require.NoError(t, err)
	s := mustClaim(t, pool, "w1", time.Minute)
	require.Equal(t, step, s.ID)
	require.Equal(t, "AWAITING_CALLBACK", s.PriorStatus)
	require.Equal(t, int32(1), s.PollCount)
	require.Equal(t, int32(1), s.Attempt, "a poll is not a send attempt")
	require.Equal(t, int32(2), s.AttemptNo)
	require.Equal(t, "po_test-"+step.String(), s.ProviderRef)
	require.False(t, s.SendDoubt)
}

// The time budget runs from the acceptance: the latest send attempt's end.
func TestClaim_WaitedForSinceAcceptance(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	step := awaitingStep(t, pool, "-1 second", "AWAITING_CALLBACK")
	_, err := pool.Exec(context.Background(), `UPDATE step_attempts SET finished_at = now() - interval '2 hours' WHERE step_id = $1`, step)
	require.NoError(t, err)
	s := mustClaim(t, pool, "w1", time.Minute)
	require.InDelta(t, (2 * time.Hour).Seconds(), s.WaitedFor.Seconds(), 60)
}

func TestClaim_PollAttemptRecordedAsPoll(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	step := awaitingStep(t, pool, "-1 second", "AWAITING_CALLBACK")
	s := mustClaim(t, pool, "w1", time.Minute)
	require.Equal(t, "poll", scalar[string](t, pool, `SELECT kind FROM step_attempts WHERE step_id = $1 AND attempt_no = $2`, step, s.AttemptNo))
	require.Equal(t, "send", scalar[string](t, pool, `SELECT kind FROM step_attempts WHERE step_id = $1 AND attempt_no = 1`, step))
}

func TestComplete_AwaitingCallbackSetsDeadline(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	submit(t, pool, "cus_1")
	s := mustClaim(t, pool, "w1", time.Minute)
	require.NoError(t, engine.Complete(ctx, pool, s,
		engine.Result{Kind: engine.ResultAwaitingCallback, ProviderRef: "po_1", RetryDelay: time.Minute}, &stubStep{}))

	r := readStep(t, pool, s.ID)
	require.Equal(t, "AWAITING_CALLBACK", r.status)
	require.Equal(t, "po_1", r.providerRef)
	require.Empty(t, r.leaseOwner)
	require.True(t, scalar[bool](t, pool, `SELECT callback_deadline = run_after AND run_after BETWEEN now() + interval '50 seconds' AND now() + interval '70 seconds' FROM steps WHERE id = $1`, s.ID))
	require.Equal(t, "AWAITING_CALLBACK", *attemptOutcome(t, pool, s.ID, s.AttemptNo))
}

// Review focus 1: a crashed poll sent nothing, so it must not create doubt.
func TestReclaim_PollReturnsToAwaitingCallback(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	step := awaitingStep(t, pool, "-1 second", "AWAITING_CALLBACK")
	s := mustClaim(t, pool, "w1", 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	n, err := engine.ReclaimExpiredLeases(context.Background(), pool)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	require.Equal(t, "AWAITING_CALLBACK", readStep(t, pool, step).status)
	require.True(t, scalar[bool](t, pool, `SELECT run_after <= now() AND callback_deadline <= now() FROM steps WHERE id = $1`, step))
	require.Equal(t, "ABANDONED", *attemptOutcome(t, pool, step, s.AttemptNo))
	require.Equal(t, "poll", scalar[string](t, pool, `SELECT kind FROM step_attempts WHERE step_id = $1 AND attempt_no = $2`, step, s.AttemptNo))
}

func TestClaim_SendDoubtFromHistory(t *testing.T) {
	ctx := context.Background()
	t.Run("an ambiguous send attempt", func(t *testing.T) {
		pool := testutil.NewPool(t, dsn)
		awaitingStep(t, pool, "-1 second", "IN_DOUBT")
		require.True(t, mustClaim(t, pool, "w1", time.Minute).SendDoubt)
	})
	t.Run("only an abandoned poll", func(t *testing.T) {
		pool := testutil.NewPool(t, dsn)
		step := awaitingStep(t, pool, "-1 second", "AWAITING_CALLBACK")
		mustClaim(t, pool, "w1", 10*time.Millisecond)
		time.Sleep(30 * time.Millisecond)
		_, err := engine.ReclaimExpiredLeases(ctx, pool)
		require.NoError(t, err)
		require.Equal(t, "AWAITING_CALLBACK", readStep(t, pool, step).status)
		require.False(t, mustClaim(t, pool, "w1", time.Minute).SendDoubt, "a poll never creates doubt")
	})
}

// Review focus 3: confirming a step with its own provider reference is not a reuse.
func TestProviderRefInUse_ExcludesOwnStep(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	step := awaitingStep(t, pool, "1 hour", "AWAITING_CALLBACK")
	ref := "po_test-" + step.String()
	q := db.New(pool)
	used, err := q.ProviderRefInUse(ctx, db.ProviderRefInUseParams{ProviderRef: &ref, StepID: step})
	require.NoError(t, err)
	require.False(t, used)
	used, err = q.ProviderRefInUse(ctx, db.ProviderRefInUseParams{ProviderRef: &ref, StepID: uuid.New()})
	require.NoError(t, err)
	require.True(t, used)
}
