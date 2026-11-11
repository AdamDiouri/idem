package recon_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/ledger"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/recon"
	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/workflow"
)

var dsn string

func TestMain(m *testing.M) { os.Exit(testutil.RunWithPostgres(m, &dsn)) }

type env struct {
	pool   *pgxpool.Pool
	fake   *fakeprovider.Provider
	client *provider.Client
	start  time.Time
}

func newEnv(t *testing.T, opts fakeprovider.Options) env {
	t.Helper()
	opts.Hang = 2 * time.Second
	fake := fakeprovider.New(opts)
	srv := httptest.NewServer(fake.Handler())
	t.Cleanup(srv.Close)
	return env{pool: testutil.NewPool(t, dsn), fake: fake, client: provider.NewClient(srv.URL, time.Second),
		start: time.Now().Add(-time.Minute)}
}

func (e env) submit(t *testing.T, customer, merchant string) uuid.UUID {
	t.Helper()
	s := workflow.NewSubmitter(e.pool, 5*time.Second, workflow.FeeConfig{RateBps: 150, FixedCents: 25})
	res, err := s.Submit(context.Background(), workflow.SubmitRequest{ClientID: "c", IdempotencyKey: uuid.NewString(),
		Request: workflow.CreateWorkflowRequest{AmountCents: 10000, Currency: "EUR", CustomerID: customer, MerchantID: merchant}})
	require.NoError(t, err)
	var c workflow.Created
	require.NoError(t, json.Unmarshal(res.Body, &c))
	return c.WorkflowID
}

// drain runs a worker until nothing is due (payouts are polled at once).
func (e env) drain(t *testing.T, tweak func(*engine.WorkerOptions)) {
	t.Helper()
	opts := engine.WorkerOptions{ID: "w", LeaseTTL: 5 * time.Second, PollInterval: 10 * time.Millisecond,
		ShutdownGrace: time.Second, Logger: slog.New(slog.DiscardHandler), MaxPolls: 8,
		Steps: []engine.Step{engine.NewChargeStep(e.client), engine.NewPayoutStep(e.client), engine.NewRefundStep(e.client)}}
	if tweak != nil {
		tweak(&opts)
	}
	w := engine.NewWorker(e.pool, opts)
	for range 200 {
		res, err := w.RunOnce(context.Background())
		require.NoError(t, err)
		if !res.Claimed {
			return
		}
	}
	t.Fatal("drain: still claiming")
}

func (e env) run(t *testing.T, from, to time.Time) recon.Result {
	t.Helper()
	res, err := recon.Run(context.Background(), e.pool, e.client, from, to, recon.Options{Margin: time.Hour})
	require.NoError(t, err)
	return res
}

func (e env) discrepancies(t *testing.T, run uuid.UUID) map[string]string {
	t.Helper()
	rows, err := e.pool.Query(context.Background(), `SELECT COALESCE(workflow_id::text, ''), effect_type, kind, parked FROM recon_discrepancies WHERE run_id = $1`, run)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var wf, typ, kind string
		var parked bool
		require.NoError(t, rows.Scan(&wf, &typ, &kind, &parked))
		if parked {
			kind += " (parked)"
		}
		out[wf+" "+typ] = kind
	}
	return out
}

func (e env) runStatus(t *testing.T, run uuid.UUID) (status, errText string) {
	t.Helper()
	require.NoError(t, e.pool.QueryRow(context.Background(), `SELECT status, COALESCE(error, '') FROM recon_runs WHERE id = $1`, run).Scan(&status, &errText))
	return
}

// Slice 2.6: honest flows of every outcome reconcile to nothing.
func TestRun_CleanFlowsHaveNoDiscrepancy(t *testing.T) {
	e := newEnv(t, fakeprovider.Options{})
	e.submit(t, "cus_1", "mer_1")
	e.submit(t, "cus_decline_1", "mer_1")
	e.submit(t, "cus_1", "mer_invalid_1")
	e.drain(t, nil)

	res := e.run(t, e.start, time.Now().Add(time.Minute))
	// The declined workflow moved nothing on either side: nothing anchors it.
	require.Equal(t, 2, res.Workflows)
	require.Zero(t, res.Discrepancies)
	require.Empty(t, e.discrepancies(t, res.RunID))
	status, _ := e.runStatus(t, res.RunID)
	require.Equal(t, "SUCCEEDED", status)
}

// F34, one way: money moved at the provider that idem never recorded (a charge
// made outside idem, naming a workflow idem does not have).
func TestRun_ProviderMovementWithoutPosting_F34(t *testing.T) {
	e := newEnv(t, fakeprovider.Options{})
	stranger := uuid.Must(uuid.NewV7())
	out := e.client.CreateCharge(context.Background(), "outside-idem", provider.ChargeRequest{AmountCents: 500, Currency: "EUR", CustomerID: "cus_1", WorkflowID: stranger})
	require.Equal(t, provider.Success, out.Class)

	res := e.run(t, e.start, time.Now().Add(time.Minute))
	require.Equal(t, map[string]string{stranger.String() + " charge": "missing_in_ledger"}, e.discrepancies(t, res.RunID))
}

// A charge moved although idem recorded it refused.
func TestRun_StatusMismatch(t *testing.T) {
	e := newEnv(t, fakeprovider.Options{})
	wf := e.submit(t, "cus_decline_1", "mer_1")
	e.drain(t, nil)
	out := e.client.CreateCharge(context.Background(), "outside-idem", provider.ChargeRequest{AmountCents: 10000, Currency: "EUR", CustomerID: "cus_1", WorkflowID: wf})
	require.Equal(t, provider.Success, out.Class)

	res := e.run(t, e.start, time.Now().Add(time.Minute))
	require.Equal(t, map[string]string{wf.String() + " charge": "status_mismatch"}, e.discrepancies(t, res.RunID))
}

// F34, the other way: a posting with no movement behind it.
func TestRun_PostingWithoutProviderMovement_F34(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, fakeprovider.Options{})
	wf := e.submit(t, "cus_decline_1", "mer_1")
	e.drain(t, nil)
	var step uuid.UUID
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT id FROM steps WHERE workflow_id = $1`, wf).Scan(&step))
	tx, err := e.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	require.NoError(t, ledger.Post(ctx, db.New(tx), ledger.Txn{WorkflowID: wf, StepID: step, Kind: "charge", Entries: ledger.ChargeEntries(10000, 175)}))
	require.NoError(t, tx.Commit(ctx))

	res := e.run(t, e.start, time.Now().Add(time.Minute))
	require.Equal(t, map[string]string{wf.String() + " charge": "missing_at_provider"}, e.discrepancies(t, res.RunID))
}

// Not final yet is never a discrepancy.
func TestRun_SkipsInFlightWorkflows(t *testing.T) {
	e := newEnv(t, fakeprovider.Options{PayoutSettle: time.Hour})
	e.submit(t, "cus_1", "mer_1")
	e.drain(t, func(o *engine.WorkerOptions) { o.CallbackDeadline = time.Hour }) // the payout waits
	res := e.run(t, e.start, time.Now().Add(time.Minute))
	require.Zero(t, res.Discrepancies, "a pending payout moved nothing; its charge matches")
}

// Review focus 1: a payout that settled after the window's end is matched
// with its posting (the report is read with a margin).
func TestRun_LateSettlementWithinMargin(t *testing.T) {
	e := newEnv(t, fakeprovider.Options{})
	wf := e.submit(t, "cus_1", "mer_1")
	e.drain(t, nil)
	var payoutAt time.Time
	for _, eff := range e.fake.Effects() {
		if eff.WorkflowID == wf.String() && eff.Type == "payout" {
			payoutAt = eff.At
		}
	}
	require.False(t, payoutAt.IsZero())
	to := payoutAt.Add(-time.Second) // the window ends before the payout moved
	res := e.run(t, e.start, to)
	require.Zero(t, res.Discrepancies, "%v", e.discrepancies(t, res.RunID))
}

// Review focus 5: a parked workflow whose payout truly moved is reported,
// flagged parked.
func TestRun_ParkedWorkflowIsReportedAndFlagged(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, fakeprovider.Options{})
	e.fake.SetFaults(fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "payouts", Mode: fakeprovider.ModeTimeoutAfterEffect}}})
	wf := e.submit(t, "cus_1", "mer_1")
	e.drain(t, func(o *engine.WorkerOptions) { o.Steps = []engine.Step{engine.NewChargeStep(e.client)} }) // only the charge
	_, err := e.pool.Exec(ctx, `UPDATE steps SET max_attempts = 1 WHERE workflow_id = $1 AND name = 'payout'`, wf)
	require.NoError(t, err)
	e.drain(t, nil) // the payout times out after taking effect and parks
	e.fake.SettleNow()
	var status string
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT status FROM workflows WHERE id = $1`, wf).Scan(&status))
	require.Equal(t, "NEEDS_ATTENTION", status)

	res := e.run(t, e.start, time.Now().Add(time.Minute))
	require.Equal(t, map[string]string{wf.String() + " payout": "missing_in_ledger (parked)"}, e.discrepancies(t, res.RunID))
}

// Review focus 3: an unreachable provider fails the run, records why, and
// reports nothing.
func TestRun_ProviderDownFailsTheRun(t *testing.T) {
	e := newEnv(t, fakeprovider.Options{})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	url := "http://" + l.Addr().String()
	require.NoError(t, l.Close())
	res, err := recon.Run(context.Background(), e.pool, provider.NewClient(url, time.Second), e.start, time.Now(), recon.Options{Margin: time.Hour})
	require.Error(t, err)
	status, errText := e.runStatus(t, res.RunID)
	require.Equal(t, "FAILED", status)
	require.Contains(t, errText, "balance transactions")
	require.Empty(t, e.discrepancies(t, res.RunID))
}

// D15: the job changes nothing but its own tables.
func TestRun_ChangesNothingElse(t *testing.T) {
	e := newEnv(t, fakeprovider.Options{})
	e.submit(t, "cus_1", "mer_1")
	e.submit(t, "cus_decline_1", "mer_1")
	e.drain(t, nil)
	snapshot := func() string {
		var s string
		require.NoError(t, e.pool.QueryRow(context.Background(), `SELECT md5(
			(SELECT string_agg(w::text, '|' ORDER BY id) FROM workflows w) ||
			(SELECT string_agg(s::text, '|' ORDER BY id) FROM steps s) ||
			(SELECT string_agg(t::text, '|' ORDER BY id) FROM ledger_transactions t) ||
			(SELECT string_agg(x::text, '|' ORDER BY id) FROM ledger_entries x) ||
			(SELECT string_agg(o::text, '|' ORDER BY id) FROM outbox o))`).Scan(&s))
		return s
	}
	before := snapshot()
	e.run(t, e.start, time.Now().Add(time.Minute))
	require.Equal(t, before, snapshot())
}

// Final review, C1: a payout settling long after its charge (beyond the
// margin) is honest; only the window's own items are judged.
func TestRun_SettlementLongerThanTheMargin(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, fakeprovider.Options{PayoutSettle: 4 * time.Second})
	wf := e.submit(t, "cus_1", "mer_1")
	e.drain(t, func(o *engine.WorkerOptions) { o.CallbackDeadline = 5 * time.Second }) // charge, payout accepted
	time.Sleep(5 * time.Second)
	e.drain(t, nil) // polled: paid and posted
	var status string
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT status FROM workflows WHERE id = $1`, wf).Scan(&status))
	require.Equal(t, "SUCCEEDED", status)
	var payoutAt time.Time
	for _, eff := range e.fake.Effects() {
		if eff.Type == "payout" {
			payoutAt = eff.At
		}
	}
	res, err := recon.Run(ctx, e.pool, e.client, payoutAt.Add(-500*time.Millisecond), time.Now().Add(time.Minute), recon.Options{Margin: 2 * time.Second})
	require.NoError(t, err)
	require.Zero(t, res.Discrepancies, "%v", e.discrepancies(t, res.RunID))
}

// lateReporter reads the report, then lets the world move on (a payout
// settles and is posted) before the run reads anything else.
type lateReporter struct {
	inner *provider.Client
	then  func()
}

func (l lateReporter) ListBalanceTransactions(ctx context.Context, from, to time.Time) ([]provider.BalanceTransaction, error) {
	rows, err := l.inner.ListBalanceTransactions(ctx, from, to)
	l.then()
	return rows, err
}

// Final review, I3: idem's state is read before the report, in one snapshot,
// so a posting made while the report is read is never "missing at provider".
func TestRun_SnapshotBeforeReport(t *testing.T) {
	e := newEnv(t, fakeprovider.Options{PayoutSettle: time.Hour})
	e.submit(t, "cus_1", "mer_1")
	e.drain(t, func(o *engine.WorkerOptions) { o.CallbackDeadline = time.Hour }) // the payout waits
	reporter := lateReporter{inner: e.client, then: func() {
		e.fake.SettleNow()
		_, err := e.pool.Exec(context.Background(), `UPDATE steps SET run_after = now(), callback_deadline = now() WHERE name = 'payout'`)
		require.NoError(t, err)
		e.drain(t, nil) // polled: paid and posted, after the report was read
	}}
	res, err := recon.Run(context.Background(), e.pool, reporter, e.start, time.Now().Add(time.Minute), recon.Options{Margin: time.Hour})
	require.NoError(t, err)
	require.Zero(t, res.Discrepancies, "%v", e.discrepancies(t, res.RunID))
}
