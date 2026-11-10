package chaos

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/api"
	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/workflow"
)

const testToken = "chaos-token-0123456789"

// startAPI serves the real API on addr ("" picks a port) and returns its base
// URL and a stop function.
func startAPI(t *testing.T, pool *pgxpool.Pool, addr string) (string, func()) {
	t.Helper()
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	var l net.Listener
	var err error
	require.Eventually(t, func() bool { l, err = net.Listen("tcp", addr); return err == nil }, 2*time.Second, 20*time.Millisecond)
	srv := &http.Server{
		Handler: api.NewServer(api.Options{
			Tokens: map[string]string{testToken: "chaos"},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}, workflow.NewSubmitter(pool, 2*time.Second, workflow.FeeConfig{RateBps: 150, FixedCents: 25}), pool),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = srv.Serve(l) }()
	stop := func() { _ = srv.Close() }
	t.Cleanup(stop)
	return "http://" + l.Addr().String(), stop
}

func workflowCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM workflows`).Scan(&n))
	return n
}

func clientConfig(url string, n int, seed int64) ClientConfig {
	return ClientConfig{BaseURLs: []string{url}, Token: testToken, Workflows: n, Concurrency: 8, Seed: seed}
}

func TestClients_RetriesConvergeOnOneWorkflow(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	url, _ := startAPI(t, pool, "")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res := RunClients(ctx, clientConfig(url, 40, 1))
	require.Equal(t, 40, res.Submitted)
	require.Equal(t, 40-res.Behaviors[Malformed], len(res.Accepted))
	require.Equal(t, res.Behaviors[Malformed], res.Rejected)
	require.Empty(t, res.Conflicts)
	require.Empty(t, res.Unfinished)
	require.Empty(t, res.Unexpected)
	require.Equal(t, len(res.Accepted), workflowCount(t, pool), "one workflow per accepted key, however many retries")
}

func TestClients_MalformedIsRejectedNotRetried(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	url, _ := startAPI(t, pool, "")
	var seed int64
	for seed = 1; seed < 100; seed++ { // find a seed with malformed clients
		if PlanBehaviors(seed, 20)[Malformed] > 0 {
			break
		}
	}
	res := RunClients(context.Background(), clientConfig(url, 20, seed))
	require.Positive(t, res.Rejected)
	require.Equal(t, len(res.Accepted), workflowCount(t, pool))
}

// Review finding I9: the outage must actually be hit. The API is down when
// the clients start; arrivals are spread so traffic continues after it returns.
func TestClients_SurviveAPIOutage(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	url, stop := startAPI(t, pool, "")
	addr := url[len("http://"):]
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := clientConfig(url, 30, 3)
	cfg.Spread = time.Second
	done := make(chan ClientResult, 1)
	go func() { done <- RunClients(ctx, cfg) }()
	time.Sleep(500 * time.Millisecond)
	startAPI(t, pool, addr)

	res := <-done
	require.Positive(t, res.Retries, "some requests must have hit the outage")
	require.Empty(t, res.Unfinished)
	require.Empty(t, res.Conflicts)
	require.Empty(t, res.Unexpected)
	require.Equal(t, len(res.Accepted), workflowCount(t, pool))
}

// Review finding C1: arrivals are spread over the campaign, so faults meet
// work in flight instead of an idle system.
func TestClients_ArrivalsAreSpread(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	url, _ := startAPI(t, pool, "")
	cfg := clientConfig(url, 20, 5)
	cfg.Spread = time.Second

	start := time.Now()
	res := RunClients(context.Background(), cfg)
	require.GreaterOrEqual(t, time.Since(start), 600*time.Millisecond, "all requests were sent at once")
	require.Empty(t, res.Unfinished)
}

// The orchestrator reads Outstanding to tell whether an event met work in flight.
func TestClients_OutstandingCounter(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	url, _ := startAPI(t, pool, "")
	var outstanding atomic.Int64
	var peak atomic.Int64
	cfg := clientConfig(url, 30, 9)
	cfg.Spread = 500 * time.Millisecond
	cfg.Outstanding = &outstanding

	done := make(chan struct{})
	go func() {
		defer close(done)
		RunClients(context.Background(), cfg)
	}()
	for {
		select {
		case <-done:
			require.Positive(t, peak.Load(), "outstanding never rose above zero")
			require.Zero(t, outstanding.Load(), "outstanding must return to zero")
			return
		default:
			if n := outstanding.Load(); n > peak.Load() {
				peak.Store(n)
			}
			time.Sleep(time.Millisecond)
		}
	}
}

func TestClients_SeededMixIsDeterministic(t *testing.T) {
	require.Equal(t, PlanBehaviors(7, 200), PlanBehaviors(7, 200))
	mix := PlanBehaviors(7, 200)
	for _, b := range []Behavior{Polite, Impatient, DoubleClick, Reformatted, Malformed} {
		require.Positive(t, mix[b], "behavior %s missing from a 200-client mix", b)
	}
}

func TestClients_StopWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	// Nothing listens here: every request fails, clients keep retrying until ctx ends.
	res := RunClients(ctx, clientConfig("http://127.0.0.1:1", 5, 1))
	require.NotEmpty(t, res.Unfinished)
	require.True(t, errors.Is(ctx.Err(), context.DeadlineExceeded))
}

// Slice 2.3: clients rotate across API replicas, so retries and double
// clicks of one key usually reach both.
func TestClients_RotateAcrossReplicas(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	a, _ := startAPI(t, pool, "")
	b, _ := startAPI(t, pool, "")
	cfg := clientConfig(a, 20, 5)
	cfg.BaseURLs = []string{a, b}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res := RunClients(ctx, cfg)
	require.Empty(t, res.Conflicts)
	require.Empty(t, res.Unfinished)
	require.Positive(t, res.PerReplica[a])
	require.Positive(t, res.PerReplica[b])
	require.Equal(t, len(res.Accepted), workflowCount(t, pool))
}

// Review focus 3: one replica down (restarting); every workflow is still
// accepted through the other.
func TestClients_SurviveOneReplicaDown(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	a, _ := startAPI(t, pool, "")
	b, stopB := startAPI(t, pool, "")
	stopB()
	cfg := clientConfig(a, 20, 6)
	cfg.BaseURLs = []string{a, b}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res := RunClients(ctx, cfg)
	require.Empty(t, res.Unfinished)
	require.Empty(t, res.Conflicts)
	require.Empty(t, res.Unexpected)
	require.Zero(t, res.PerReplica[b], "a replica that is down answers nothing")
	require.Equal(t, len(res.Accepted), workflowCount(t, pool))
}

// Minor fix: no replica to send to is a reported failure, not a panic.
func TestClients_NoReplicaIsReportedNotPanic(t *testing.T) {
	cfg := clientConfig("", 3, 1)
	cfg.BaseURLs = nil
	var res ClientResult
	require.NotPanics(t, func() { res = RunClients(context.Background(), cfg) })
	require.NotEmpty(t, res.Unexpected)
	require.Contains(t, res.Unexpected[0], "no API replica")
}
