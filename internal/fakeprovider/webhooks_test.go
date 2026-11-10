package fakeprovider_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/webhook"
)

var whSecret = []byte("whsec_test")

type received struct {
	at   time.Time
	sig  string
	body []byte
}

// receiver records webhook requests; respond picks each answer (nil = 200).
type receiver struct {
	mu      sync.Mutex
	got     []received
	respond func(n int) int
}

func (r *receiver) handler(w http.ResponseWriter, req *http.Request) {
	b, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	r.got = append(r.got, received{at: time.Now(), sig: req.Header.Get(webhook.ProviderSignatureHeader), body: b})
	n := len(r.got)
	r.mu.Unlock()
	status := http.StatusOK
	if r.respond != nil {
		status = r.respond(n)
	}
	w.WriteHeader(status)
}

func (r *receiver) all() []received {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]received(nil), r.got...)
}

func (r *receiver) events(t *testing.T) []webhook.ProviderEvent {
	t.Helper()
	var out []webhook.ProviderEvent
	for _, g := range r.all() {
		var e webhook.ProviderEvent
		require.NoError(t, json.Unmarshal(g.body, &e))
		out = append(out, e)
	}
	return out
}

// webhookProvider starts a provider that delivers webhooks to a receiver.
func webhookProvider(t *testing.T, rcv *receiver, maxAttempts int, run bool) (*fakeprovider.Provider, string) {
	t.Helper()
	rsrv := httptest.NewServer(http.HandlerFunc(rcv.handler))
	t.Cleanup(rsrv.Close)
	p, srv := newServerWith(t, fakeprovider.Options{
		WebhookURLs: []string{rsrv.URL}, WebhookSecret: whSecret, WebhookRetryBase: 20 * time.Millisecond,
		WebhookMaxAttempts: maxAttempts, Tick: 5 * time.Millisecond,
	})
	if run {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go p.Run(ctx)
	}
	return p, srv.URL
}

func TestWebhook_PaidPayoutSendsSignedEvent(t *testing.T) {
	rcv := &receiver{}
	_, url := webhookProvider(t, rcv, 8, true)
	r := post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
	id := idOf(t, r.body, "po_", "pending")

	require.Eventually(t, func() bool { return len(rcv.all()) == 1 }, 2*time.Second, 5*time.Millisecond)
	g := rcv.all()[0]
	require.NoError(t, webhook.Verify(g.sig, g.body, [][]byte{whSecret}, time.Now(), 5*time.Minute))
	e := rcv.events(t)[0]
	require.Equal(t, webhook.EventPayoutPaid, e.Type)
	require.Regexp(t, `^evt_`, e.ID)
	require.Equal(t, id, e.Data.PayoutID)
	require.Equal(t, "paid", e.Data.Status)
	require.Equal(t, "k1", e.Data.IdempotencyKey)
	require.Equal(t, wfID, e.Data.Metadata.WorkflowID)
	require.Equal(t, int64(9825), e.Data.AmountCents)
}

func TestWebhook_FailedPayoutSendsFailedEvent(t *testing.T) {
	rcv := &receiver{}
	p, url := webhookProvider(t, rcv, 8, true)
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModePayoutFail, Times: 1}))
	post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
	require.Eventually(t, func() bool { return len(rcv.all()) == 1 }, 2*time.Second, 5*time.Millisecond)
	e := rcv.events(t)[0]
	require.Equal(t, webhook.EventPayoutFailed, e.Type)
	require.Equal(t, "account_closed", e.Data.FailureCode)
}

func TestWebhook_RetriesUntil2xx(t *testing.T) {
	rcv := &receiver{respond: func(n int) int {
		if n <= 2 {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	}}
	p, url := webhookProvider(t, rcv, 8, true)
	post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
	require.Eventually(t, func() bool { d := p.Webhooks(); return len(d) == 1 && d[0].Delivered }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, 3, p.Webhooks()[0].Attempts)
}

func TestWebhook_GivesUpAfterMaxAttempts(t *testing.T) {
	rcv := &receiver{respond: func(int) int { return http.StatusInternalServerError }}
	p, url := webhookProvider(t, rcv, 3, true)
	post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
	require.Eventually(t, func() bool { return len(rcv.all()) == 3 }, 3*time.Second, 5*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	require.Len(t, rcv.all(), 3, "no fourth attempt")
	require.False(t, p.Webhooks()[0].Delivered)
}

func TestWebhookFaults(t *testing.T) {
	webhookRule := func(mode string) fakeprovider.Faults {
		return faults(fakeprovider.Rule{Endpoint: "webhooks", Mode: mode, Times: 1})
	}
	t.Run("dup", func(t *testing.T) {
		rcv := &receiver{}
		p, url := webhookProvider(t, rcv, 8, true)
		p.SetFaults(webhookRule(fakeprovider.ModeDupWebhook))
		post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
		require.Eventually(t, func() bool { return len(rcv.all()) == 2 }, 2*time.Second, 5*time.Millisecond)
		ev := rcv.events(t)
		require.Equal(t, ev[0].ID, ev[1].ID, "the same event, twice")
	})
	t.Run("drop", func(t *testing.T) {
		rcv := &receiver{}
		p, url := webhookProvider(t, rcv, 8, true)
		p.SetFaults(webhookRule(fakeprovider.ModeDropWebhook))
		post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
		require.Eventually(t, func() bool { return len(p.Webhooks()) == 1 }, 2*time.Second, 5*time.Millisecond)
		time.Sleep(200 * time.Millisecond)
		require.Empty(t, rcv.all())
		require.Equal(t, fakeprovider.ModeDropWebhook, p.Webhooks()[0].Fault)
		require.Equal(t, 1, p.FaultHits()["webhooks/"+fakeprovider.ModeDropWebhook])
	})
	t.Run("bad signature, then correct", func(t *testing.T) {
		rcv := &receiver{}
		rcv.respond = func(n int) int {
			g := rcv.all()[n-1]
			if webhook.Verify(g.sig, g.body, [][]byte{whSecret}, time.Now(), 5*time.Minute) != nil {
				return http.StatusBadRequest
			}
			return http.StatusOK
		}
		p, url := webhookProvider(t, rcv, 8, true)
		p.SetFaults(webhookRule(fakeprovider.ModeBadSignature))
		post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
		require.Eventually(t, func() bool { d := p.Webhooks(); return len(d) == 1 && d[0].Delivered }, 2*time.Second, 5*time.Millisecond)
		all := rcv.all()
		require.Len(t, all, 2)
		require.Error(t, webhook.Verify(all[0].sig, all[0].body, [][]byte{whSecret}, time.Now(), 5*time.Minute))
		require.NoError(t, webhook.Verify(all[1].sig, all[1].body, [][]byte{whSecret}, time.Now(), 5*time.Minute))
	})
	t.Run("reorder", func(t *testing.T) {
		rcv := &receiver{}
		p, url := webhookProvider(t, rcv, 8, true)
		p.SetFaults(webhookRule(fakeprovider.ModeReorder))
		post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
		require.Eventually(t, func() bool { return len(rcv.all()) == 2 }, 2*time.Second, 5*time.Millisecond)
		ev := rcv.events(t)
		require.Equal(t, webhook.EventPayoutPaid, ev[0].Type)
		require.Equal(t, webhook.EventPayoutFailed, ev[1].Type, "a stale contradiction")
		require.NotEqual(t, ev[0].ID, ev[1].ID)
		require.Equal(t, ev[0].Created-1, ev[1].Created)
		require.True(t, p.Webhooks()[1].Stale)
	})
	t.Run("fast", func(t *testing.T) {
		rcv := &receiver{}
		p, url := webhookProvider(t, rcv, 8, false) // no Run: delivery must be inline
		p.SetFaults(webhookRule(fakeprovider.ModeFastWebhook))
		r := post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
		require.Equal(t, http.StatusAccepted, r.status)
		require.Len(t, rcv.all(), 1, "the webhook arrived before the 202 was written")
		require.Equal(t, webhook.EventPayoutPaid, rcv.events(t)[0].Type)
	})
	t.Run("delay", func(t *testing.T) {
		rcv := &receiver{}
		p, url := webhookProvider(t, rcv, 8, true)
		p.SetFaults(webhookRule(fakeprovider.ModeDelayWebhook))
		start := time.Now()
		post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
		require.Eventually(t, func() bool { return len(rcv.all()) == 1 }, 2*time.Second, 5*time.Millisecond)
		require.GreaterOrEqual(t, rcv.all()[0].at.Sub(start), 80*time.Millisecond) // 4 × 20ms
	})
}

func TestFaultValidate_WebhookModesOnlyOnWebhooks(t *testing.T) {
	require.Error(t, fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "payouts", Mode: fakeprovider.ModeDupWebhook}}}.Validate())
	require.Error(t, fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "webhooks", Mode: fakeprovider.ModeDecline}}}.Validate())
	require.NoError(t, fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "webhooks", Mode: fakeprovider.ModeReorder}}}.Validate())
}

func TestWebhook_NoURLMeansNoDelivery(t *testing.T) {
	p, srv := newServerWith(t, fakeprovider.Options{Tick: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Run(ctx)
	id := idOf(t, post(t, srv.URL, "/payouts", "k1", payoutBody("mer_1", 9825)).body, "po_", "pending")
	require.Eventually(t, func() bool { return len(p.Effects()) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, id, p.Effects()[0].ID)
	require.Empty(t, p.Webhooks())
}

// Minor fix: fast_webhook must arrive before the 202 even while Run is
// ticking (it used to be able to settle and send it first, asynchronously).
func TestWebhook_FastBeatsTheResponseEvenWithRun(t *testing.T) {
	rcv := &receiver{respond: func(int) int { time.Sleep(20 * time.Millisecond); return http.StatusOK }}
	rsrv := httptest.NewServer(http.HandlerFunc(rcv.handler))
	t.Cleanup(rsrv.Close)
	p, srv := newServerWith(t, fakeprovider.Options{
		WebhookURLs: []string{rsrv.URL}, WebhookSecret: whSecret, WebhookRetryBase: 20 * time.Millisecond, Tick: time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Run(ctx)
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "webhooks", Mode: fakeprovider.ModeFastWebhook}))
	for i := range 30 {
		post(t, srv.URL, "/payouts", fmt.Sprintf("k%d", i), payoutBody("mer_1", 9825))
		require.Len(t, rcv.all(), i+1, "payout %d: its webhook must be delivered before the 202", i)
	}
}

// Minor fix: one slow receiver must not hold up every other webhook.
func TestWebhook_DeliveriesDoNotQueueBehindASlowOne(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	rcv := &receiver{}
	rcv.respond = func(n int) int {
		if n == 1 {
			<-release // the first delivery hangs
		}
		return http.StatusOK
	}
	_, url := webhookProvider(t, rcv, 8, true)
	// Registered after the servers' cleanups, so it runs first (LIFO): a
	// receiver still blocked would otherwise keep its server from closing.
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
	require.Eventually(t, func() bool { return len(rcv.all()) == 1 }, time.Second, 5*time.Millisecond)
	post(t, url, "/payouts", "k2", payoutBody("mer_1", 9825))
	require.Eventually(t, func() bool { return len(rcv.all()) == 2 }, time.Second, 5*time.Millisecond,
		"the second webhook arrives while the first is still hanging")
	once.Do(func() { close(release) })
}

// Slice 2.3: with several webhook URLs (API replicas), a retry goes to the
// next one, so deduplication is exercised across replicas.
func TestWebhook_RetryGoesToTheNextURL(t *testing.T) {
	a := &receiver{respond: func(int) int { return http.StatusServiceUnavailable }}
	b := &receiver{}
	asrv := httptest.NewServer(http.HandlerFunc(a.handler))
	bsrv := httptest.NewServer(http.HandlerFunc(b.handler))
	t.Cleanup(asrv.Close)
	t.Cleanup(bsrv.Close)
	p, srv := newServerWith(t, fakeprovider.Options{
		WebhookURLs: []string{asrv.URL, bsrv.URL}, WebhookSecret: whSecret,
		WebhookRetryBase: 20 * time.Millisecond, Tick: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Run(ctx)
	post(t, srv.URL, "/payouts", "k1", payoutBody("mer_1", 9825))

	require.Eventually(t, func() bool { d := p.Webhooks(); return len(d) == 1 && d[0].Delivered }, 2*time.Second, 5*time.Millisecond)
	require.Len(t, a.all(), 1)
	require.Len(t, b.all(), 1)
	require.Equal(t, []string{asrv.URL, bsrv.URL}, p.Webhooks()[0].Targets)
}

// Slice 2.3b: chaos waits on the delivery log, so it must say which
// deliveries are finished without succeeding.
func TestGetWebhooks_ReportsGaveUp(t *testing.T) {
	rcv := &receiver{}
	p, url := webhookProvider(t, rcv, 8, true)
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "webhooks", Mode: fakeprovider.ModeDropWebhook, Times: 1}))
	post(t, url, "/payouts", "k1", payoutBody("mer_1", 9825))
	var ds []fakeprovider.WebhookDelivery
	require.Eventually(t, func() bool {
		var err error
		ds, err = fakeprovider.FetchWebhooks(context.Background(), url)
		require.NoError(t, err)
		return len(ds) == 1
	}, 2*time.Second, 5*time.Millisecond)
	require.True(t, ds[0].GaveUp)
	require.False(t, ds[0].Delivered)
}

// Final review, minor 7: the chaos settle loop is bounded by its deadline only
// if every read of the provider's oracle is bounded too.
func TestFetchWebhooks_TimesOutOnAHungProvider(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	defer fakeprovider.SetFetchTimeout(200 * time.Millisecond)()

	start := time.Now()
	_, err := fakeprovider.FetchWebhooks(context.Background(), srv.URL)
	require.Error(t, err)
	require.Less(t, time.Since(start), 2*time.Second)
}
