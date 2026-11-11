package relay_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/metrics"
	"github.com/AdamDiouri/idem/internal/relay"
	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/webhook"
)

var dsn string

func TestMain(m *testing.M) { os.Exit(testutil.RunWithPostgres(m, &dsn)) }

const secret = "whsec-test-0123456789"

// delivery is one request the test shop received.
type delivery struct {
	eventID, eventType, signature string
	body                          []byte
}

// shop is an httptest server recording deliveries and answering with handle.
type shop struct {
	mu   sync.Mutex
	got  []delivery
	srv  *httptest.Server
	hits atomic.Int32
}

func newShop(t *testing.T, handle func(w http.ResponseWriter, r *http.Request)) *shop {
	t.Helper()
	s := &shop{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.got = append(s.got, delivery{r.Header.Get("Idem-Event-Id"), r.Header.Get("Idem-Event-Type"), r.Header.Get("Idem-Signature"), body})
		s.mu.Unlock()
		if handle != nil {
			handle(w, r)
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *shop) deliveries() []delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]delivery(nil), s.got...)
}

func options(url string) relay.Options {
	clients := map[string]relay.Endpoint{}
	if url != "" {
		clients["c"] = relay.Endpoint{URL: url, Secret: []byte(secret)}
	}
	return relay.Options{
		ID: "relay-test", Clients: clients, LeaseTTL: 5 * time.Second, Timeout: time.Second,
		PollInterval: 10 * time.Millisecond, ShutdownGrace: 3 * time.Second, MaxAttempts: 15,
		Retry:  engine.Backoff{Base: time.Second, Cap: time.Minute, Rand: func() float64 { return 0.5 }},
		Logger: slog.New(slog.DiscardHandler),
	}
}

// newEvent ends a workflow of client "c" and returns its outbox event's ID.
func newEvent(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	wf := uuid.Must(uuid.NewV7())
	_, err := pool.Exec(ctx, `INSERT INTO workflows (id, client_id, type, status, input) VALUES ($1, 'c', 'payment_payout', 'RUNNING',
		'{"amount_cents":10000,"fee_cents":175,"currency":"EUR","customer_id":"cus_1","merchant_id":"mer_1"}')`, wf)
	require.NoError(t, err)
	testutil.SetWorkflowStatus(t, pool, wf, "SUCCEEDED")
	var id uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT id FROM outbox WHERE workflow_id = $1`, wf).Scan(&id))
	return id
}

type row struct {
	status, lastError string
	attempts          int32
	epoch             int64
	due, delivered    bool
}

func eventRow(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) row {
	t.Helper()
	var r row
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT status, COALESCE(last_error, ''), attempts, lease_epoch,
		next_attempt_at <= now(), delivered_at IS NOT NULL FROM outbox WHERE id = $1`, id).
		Scan(&r.status, &r.lastError, &r.attempts, &r.epoch, &r.due, &r.delivered))
	return r
}

func runOnce(t *testing.T, r *relay.Relay) relay.Result {
	t.Helper()
	res, err := r.RunOnce(context.Background())
	require.NoError(t, err)
	return res
}

func makeDue(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `UPDATE outbox SET next_attempt_at = now(), lease_expires_at = NULL WHERE status = 'PENDING'`)
	require.NoError(t, err)
}

func payload(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) []byte {
	t.Helper()
	var b []byte
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT payload FROM outbox WHERE id = $1`, id).Scan(&b))
	return b
}

// Slice 2.5 (M2 design §5.3): the frozen payload, signed with the client's
// secret, with the event's ID and type.
func TestRelay_DeliversSignedEvent(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := newShop(t, nil)
	id := newEvent(t, pool)

	res := runOnce(t, relay.New(pool, options(s.srv.URL)))
	require.Equal(t, relay.Result{Claimed: true, EventID: id, Outcome: "delivered"}, res)
	got := s.deliveries()
	require.Len(t, got, 1)
	require.Equal(t, id.String(), got[0].eventID)
	require.Equal(t, "workflow.succeeded", got[0].eventType)
	require.Equal(t, payload(t, pool, id), got[0].body)
	require.NoError(t, webhook.Verify(got[0].signature, got[0].body, [][]byte{[]byte(secret)}, time.Now(), time.Minute))
	r := eventRow(t, pool, id)
	require.Equal(t, "DELIVERED", r.status)
	require.True(t, r.delivered)
	require.Equal(t, int32(1), r.attempts)
}

// D19: a client without a URL is a legitimate integration (it polls GET).
func TestRelay_NoEndpoint(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := newShop(t, nil)
	id := newEvent(t, pool)
	opts := options("")
	opts.Clients["other"] = relay.Endpoint{URL: s.srv.URL, Secret: []byte(secret)}

	require.Equal(t, "no_endpoint", runOnce(t, relay.New(pool, opts)).Outcome)
	require.Equal(t, "NO_ENDPOINT", eventRow(t, pool, id).status)
	require.Zero(t, s.hits.Load())
}

// F31: a shop that fails is retried later, with backoff.
func TestRelay_RetriesWithBackoff_F31(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	var fail atomic.Bool
	fail.Store(true)
	s := newShop(t, func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	id := newEvent(t, pool)
	r := relay.New(pool, options(s.srv.URL))

	require.Equal(t, "retry", runOnce(t, r).Outcome)
	row := eventRow(t, pool, id)
	require.Equal(t, "PENDING", row.status)
	require.Equal(t, int32(1), row.attempts)
	require.False(t, row.due, "the retry waits for its backoff")
	require.Contains(t, row.lastError, "500")
	require.False(t, runOnce(t, r).Claimed, "not due yet")

	fail.Store(false)
	makeDue(t, pool)
	require.Equal(t, "delivered", runOnce(t, r).Outcome)
	require.Equal(t, int32(2), eventRow(t, pool, id).attempts)
}

func TestRelay_DeadAfterMaxAttempts(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	url := "http://" + l.Addr().String()
	require.NoError(t, l.Close()) // nothing listens: connection refused
	id := newEvent(t, pool)
	opts := options(url)
	opts.MaxAttempts = 2
	r := relay.New(pool, opts)

	require.Equal(t, "retry", runOnce(t, r).Outcome)
	makeDue(t, pool)
	require.Equal(t, "dead", runOnce(t, r).Outcome)
	row := eventRow(t, pool, id)
	require.Equal(t, "DEAD", row.status)
	require.NotEmpty(t, row.lastError)
}

// Review focus 2: a 2xx after the timeout is a failure; the shop deduplicates
// the retry.
func TestRelay_TimeoutIsAFailure(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := newShop(t, func(_ http.ResponseWriter, _ *http.Request) { time.Sleep(500 * time.Millisecond) })
	id := newEvent(t, pool)
	opts := options(s.srv.URL)
	opts.Timeout = 100 * time.Millisecond

	require.Equal(t, "retry", runOnce(t, relay.New(pool, opts)).Outcome)
	require.Equal(t, "PENDING", eventRow(t, pool, id).status)
}

// Review focus 4: a shop cannot point idem at another host.
func TestRelay_RedirectIsAFailure(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	elsewhere := newShop(t, nil)
	s := newShop(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.srv.URL, http.StatusFound)
	})
	id := newEvent(t, pool)

	require.Equal(t, "retry", runOnce(t, relay.New(pool, options(s.srv.URL))).Outcome)
	require.Zero(t, elsewhere.hits.Load())
	require.Contains(t, eventRow(t, pool, id).lastError, "302")
}

// Review focus 4: a huge answer is not read.
func TestRelay_ReadsAtMost4KiB(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	var written atomic.Int64
	s := newShop(t, func(w http.ResponseWriter, _ *http.Request) {
		chunk := make([]byte, 64<<10)
		for range 160 { // 10 MiB
			n, err := w.Write(chunk)
			written.Add(int64(n))
			if err != nil {
				return
			}
		}
	})
	newEvent(t, pool)
	start := time.Now()
	require.Equal(t, "delivered", runOnce(t, relay.New(pool, options(s.srv.URL))).Outcome)
	require.Less(t, time.Since(start), 2*time.Second)
	s.srv.CloseClientConnections()
	require.Eventually(t, func() bool { return written.Load() < 10<<20 }, time.Second, 10*time.Millisecond,
		"the relay stopped reading long before 10 MiB")
}

// Review focus 1: a relay that lost its lease while delivering changes nothing.
func TestRelay_LostLeaseIsFenced(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := newShop(t, nil)
	id := newEvent(t, pool)
	failpoint.Enable(relay.FPAfterSend, func() { // another relay re-claimed it meanwhile
		_, err := pool.Exec(context.Background(), `UPDATE outbox SET lease_epoch = lease_epoch + 1 WHERE id = $1`, id)
		require.NoError(t, err)
	})
	t.Cleanup(failpoint.Reset)

	require.Equal(t, "fenced", runOnce(t, relay.New(pool, options(s.srv.URL))).Outcome)
	row := eventRow(t, pool, id)
	require.Equal(t, "PENDING", row.status, "the new owner decides")
	require.False(t, row.delivered)
}

// F32: a relay that crashes after the shop's 2xx, before recording it, sends
// again: same event ID, which the shop deduplicates.
func TestRelay_CrashAfterSendRedelivers_F32(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := newShop(t, nil)
	id := newEvent(t, pool)
	failpoint.Enable(relay.FPAfterSend, failpoint.CrashOnce())
	t.Cleanup(failpoint.Reset)
	r := relay.New(pool, options(s.srv.URL))

	require.True(t, crashes(func() { _, _ = r.RunOnce(context.Background()) }))
	require.Equal(t, "PENDING", eventRow(t, pool, id).status)
	makeDue(t, pool) // the lease expires
	require.Equal(t, "delivered", runOnce(t, r).Outcome)
	got := s.deliveries()
	require.Len(t, got, 2)
	require.Equal(t, got[0].eventID, got[1].eventID)
	require.Equal(t, got[0].body, got[1].body)
}

func crashes(f func()) (crashed bool) {
	defer func() {
		if r := recover(); r != nil {
			if !failpoint.IsCrash(r) {
				panic(r)
			}
			crashed = true
		}
	}()
	f()
	return false
}

func TestRelay_SkipsNonPending(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := newShop(t, nil)
	for _, status := range []string{"DELIVERED", "DEAD", "NO_ENDPOINT", "SKIPPED"} {
		id := newEvent(t, pool)
		_, err := pool.Exec(context.Background(), `UPDATE outbox SET status = $2 WHERE id = $1`, id, status)
		require.NoError(t, err)
	}
	require.False(t, runOnce(t, relay.New(pool, options(s.srv.URL))).Claimed)
	require.Zero(t, s.hits.Load())
}

// Run stops claiming when cancelled and returns.
func TestRelay_RunStopsOnCancel(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := newShop(t, nil)
	id := newEvent(t, pool)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.New(pool, options(s.srv.URL)).Run(ctx) }()
	require.Eventually(t, func() bool { return eventRow(t, pool, id).status == "DELIVERED" }, 5*time.Second, 20*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// newEventFor is newEvent for another client.
func newEventFor(t *testing.T, pool *pgxpool.Pool, client string) uuid.UUID {
	t.Helper()
	id := newEvent(t, pool)
	_, err := pool.Exec(context.Background(), `UPDATE workflows SET client_id = $2 WHERE id = (SELECT workflow_id FROM outbox WHERE id = $1)`, id, client)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `UPDATE outbox SET client_id = $2 WHERE id = $1`, id, client)
	require.NoError(t, err)
	return id
}

// Final review, Important 1: a shop that hangs to the timeout must not delay
// every other shop's outcomes behind it.
func TestRelay_SlowShopDoesNotDelayOthers(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	stuck := newShop(t, func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }) // answers nothing
	healthy := newShop(t, nil)
	for range 6 {
		newEventFor(t, pool, "slow")
	}
	fast := newEventFor(t, pool, "fast") // queued behind all of them
	opts := options("")
	opts.Clients["slow"] = relay.Endpoint{URL: stuck.srv.URL, Secret: []byte(secret)}
	opts.Clients["fast"] = relay.Endpoint{URL: healthy.srv.URL, Secret: []byte(secret)}
	opts.Timeout = 2 * time.Second
	opts.Concurrency, opts.PerClient = 4, 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- relay.New(pool, opts).Run(ctx) }()
	start := time.Now()
	require.Eventually(t, func() bool { return eventRow(t, pool, fast).status == "DELIVERED" }, 5*time.Second, 20*time.Millisecond)
	require.Less(t, time.Since(start), time.Second, "the healthy shop did not wait for the stuck one")
	require.LessOrEqual(t, stuck.hits.Load(), int32(2), "one shop holds at most PerClient deliveries at once")
	cancel()
	<-done
}

// Final review, minor 4: a failure to record the result is counted as an
// error, never under an empty label.
func TestRelay_FinishErrorIsCountedAsError(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := newShop(t, nil)
	newEvent(t, pool)
	m := metrics.New()
	opts := options(s.srv.URL)
	opts.Metrics = m
	ctx, cancel := context.WithCancel(context.Background())
	failpoint.Enable(relay.FPAfterSend, func() { cancel() }) // the database write that follows fails
	t.Cleanup(failpoint.Reset)

	res, err := relay.New(pool, opts).RunOnce(ctx)
	require.Error(t, err)
	require.Equal(t, "error", res.Outcome)
	require.Equal(t, 1.0, promtestutil.ToFloat64(m.RelayDeliveries.WithLabelValues("error")))
	require.Zero(t, promtestutil.ToFloat64(m.RelayDeliveries.WithLabelValues("")))
}

// Final review, minor 7 (review focus 1, for real): relay A freezes past its
// lease, relay B takes the event over and delivers it; when A wakes up its
// send reaches the shop again (same event ID) and its result is fenced.
func TestRelay_TakeoverByAnotherRelay(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	s := newShop(t, nil)
	id := newEvent(t, pool)
	block, entered, release := failpoint.BlockOnce()
	failpoint.Enable(relay.FPBeforeSend, block)
	t.Cleanup(failpoint.Reset)
	optsA, optsB := options(s.srv.URL), options(s.srv.URL)
	optsA.ID, optsB.ID = "relay-a", "relay-b"
	optsA.LeaseTTL = time.Second

	resA := make(chan relay.Result, 1)
	go func() {
		res, _ := relay.New(pool, optsA).RunOnce(context.Background())
		resA <- res
	}()
	<-entered                           // A holds the event and is frozen before sending
	time.Sleep(1200 * time.Millisecond) // its lease expires
	require.Equal(t, "delivered", runOnce(t, relay.New(pool, optsB)).Outcome)
	release()

	require.Equal(t, "fenced", (<-resA).Outcome)
	require.Equal(t, "DELIVERED", eventRow(t, pool, id).status)
	got := s.deliveries()
	require.Len(t, got, 2)
	require.Equal(t, got[0].eventID, got[1].eventID, "the shop deduplicates on it")
}
