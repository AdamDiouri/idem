package fakeprovider_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
)

const wfID = "01936f3e-8c2a-7b1e-9f00-000000000001"

func newServer(t *testing.T) (*fakeprovider.Provider, *httptest.Server) {
	t.Helper()
	p := fakeprovider.New(fakeprovider.Options{Hang: 5 * time.Second})
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)
	return p, srv
}

func body(customer string, amount int64) string {
	return fmt.Sprintf(`{"amount_cents":%d,"currency":"EUR","customer_id":%q,"workflow_id":%q}`, amount, customer, wfID)
}

type resp struct {
	status int
	body   string
}

func charge(t *testing.T, client *http.Client, url, key, b string) (resp, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/charges", strings.NewReader(b))
	require.NoError(t, err)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	r, err := client.Do(req)
	if err != nil {
		return resp{}, err
	}
	defer r.Body.Close()
	data, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return resp{status: r.StatusCode, body: string(data)}, nil
}

func mustCharge(t *testing.T, url, key, b string) resp {
	t.Helper()
	r, err := charge(t, http.DefaultClient, url, key, b)
	require.NoError(t, err)
	return r
}

var shortClient = &http.Client{Timeout: 100 * time.Millisecond}

func chargeID(t *testing.T, b string) string {
	t.Helper()
	var c struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal([]byte(b), &c), b)
	require.Equal(t, "succeeded", c.Status)
	require.True(t, strings.HasPrefix(c.ID, "ch_"), c.ID)
	return c.ID
}

func faults(rules ...fakeprovider.Rule) fakeprovider.Faults {
	return fakeprovider.Faults{Rules: rules}
}

func TestCharge_CreatesEffectOnceAndReplays(t *testing.T) {
	p, srv := newServer(t)
	first := mustCharge(t, srv.URL, "k1", body("cus_1", 10000))
	second := mustCharge(t, srv.URL, "k1", body("cus_1", 10000))

	require.Equal(t, http.StatusCreated, first.status, first.body)
	require.Equal(t, first, second)
	chargeID(t, first.body)

	effects := p.Effects()
	require.Len(t, effects, 1)
	require.Equal(t, "charge", effects[0].Type)
	require.Equal(t, wfID, effects[0].WorkflowID)
	require.Equal(t, "k1", effects[0].IdempotencyKey)
	require.Equal(t, int64(10000), effects[0].AmountCents)
	require.Equal(t, chargeID(t, first.body), effects[0].ID)
}

func TestCharge_KeyReuseDifferentBody(t *testing.T) {
	p, srv := newServer(t)
	mustCharge(t, srv.URL, "k1", body("cus_1", 10000))
	r := mustCharge(t, srv.URL, "k1", body("cus_1", 20000))
	require.Equal(t, http.StatusUnprocessableEntity, r.status)
	require.Contains(t, r.body, "idempotency_key_reused")
	require.Len(t, p.Effects(), 1)
}

func TestCharge_ConcurrentSameKey(t *testing.T) {
	p, srv := newServer(t)
	// Every new request hangs with its key in flight. Whichever request arrives
	// first hangs; the polling request uses a short timeout so if it wins the race
	// it gives up (releasing the key) and tries again.
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "charges", Mode: "timeout_before_effect"}))

	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		// Retry until this request is the one hanging: if it arrives while a
		// poll holds the key it gets 409 and must try again.
		for ctx.Err() == nil {
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/charges", strings.NewReader(body("cus_1", 10000)))
			req.Header.Set("Idempotency-Key", "k1")
			r, err := http.DefaultClient.Do(req)
			if err != nil {
				return // cancelled while hanging
			}
			r.Body.Close()
		}
	}()

	require.Eventually(t, func() bool {
		r, err := charge(t, shortClient, srv.URL, "k1", body("cus_1", 10000))
		return err == nil && r.status == http.StatusConflict && strings.Contains(r.body, "idempotency_key_in_flight")
	}, 3*time.Second, 20*time.Millisecond, "second request must see the key in flight")

	cancel()
	<-firstDone
	p.SetFaults(fakeprovider.Faults{})

	require.Eventually(t, func() bool {
		return mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status == http.StatusCreated
	}, 2*time.Second, 20*time.Millisecond, "key must be released after the hung request is cancelled")
	require.Len(t, p.Effects(), 1)
}

func TestCharge_DeclineCustomer(t *testing.T) {
	p, srv := newServer(t)
	first := mustCharge(t, srv.URL, "k1", body("cus_decline_1", 10000))
	require.Equal(t, http.StatusPaymentRequired, first.status)
	require.Contains(t, first.body, "card_declined")
	require.Equal(t, first, mustCharge(t, srv.URL, "k1", body("cus_decline_1", 10000)))
	require.Empty(t, p.Effects())
}

func TestCharge_BadRequests(t *testing.T) {
	p, srv := newServer(t)
	require.Equal(t, http.StatusBadRequest, mustCharge(t, srv.URL, "", body("cus_1", 10000)).status)
	require.Equal(t, http.StatusBadRequest, mustCharge(t, srv.URL, "k1", `{"nope":1}`).status)
	require.Equal(t, http.StatusBadRequest, mustCharge(t, srv.URL, "k2", `not json`).status)
	require.Empty(t, p.Effects())
}

func TestFault_TimeoutBeforeEffect(t *testing.T) {
	p, srv := newServer(t)
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "charges", Mode: "timeout_before_effect", Times: 1}))

	_, err := charge(t, shortClient, srv.URL, "k1", body("cus_1", 10000))
	require.Error(t, err)
	require.Empty(t, p.Effects())

	require.Eventually(t, func() bool {
		return mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status == http.StatusCreated
	}, 2*time.Second, 20*time.Millisecond)
	require.Len(t, p.Effects(), 1)
}

func TestFault_TimeoutAfterEffect(t *testing.T) {
	p, srv := newServer(t)
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "charges", Mode: "timeout_after_effect", Times: 1}))

	_, err := charge(t, shortClient, srv.URL, "k1", body("cus_1", 10000))
	require.Error(t, err)
	require.Len(t, p.Effects(), 1, "the money moved even though the client timed out")

	retry := mustCharge(t, srv.URL, "k1", body("cus_1", 10000))
	require.Equal(t, http.StatusCreated, retry.status)
	require.Equal(t, p.Effects()[0].ID, chargeID(t, retry.body))
	require.Len(t, p.Effects(), 1)
}

func TestFault_ErrorBeforeEffect(t *testing.T) {
	p, srv := newServer(t)
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "charges", Mode: "error_before_effect"}))
	require.Equal(t, http.StatusInternalServerError, mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status)
	require.Empty(t, p.Effects())
}

func TestFault_ErrorAfterEffect(t *testing.T) {
	p, srv := newServer(t)
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "charges", Mode: "error_after_effect", Times: 1}))

	require.Equal(t, http.StatusInternalServerError, mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status)
	require.Len(t, p.Effects(), 1)

	retry := mustCharge(t, srv.URL, "k1", body("cus_1", 10000))
	require.Equal(t, http.StatusCreated, retry.status)
	require.Equal(t, p.Effects()[0].ID, chargeID(t, retry.body))
	require.Len(t, p.Effects(), 1)
}

func TestFault_DeclineMode(t *testing.T) {
	p, srv := newServer(t)
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "charges", Mode: "decline", Times: 1}))
	require.Equal(t, http.StatusPaymentRequired, mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status)
	require.Equal(t, http.StatusPaymentRequired, mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status, "decline is stored and replayed")
	require.Empty(t, p.Effects())
}

func TestFault_RateLimit(t *testing.T) {
	p, srv := newServer(t)
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "charges", Mode: fakeprovider.ModeRateLimit, Times: 1}))

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/charges", strings.NewReader(body("cus_1", 10000)))
	require.NoError(t, err)
	req.Header.Set("Idempotency-Key", "k1")
	r, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	r.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, r.StatusCode)
	require.Equal(t, "1", r.Header.Get("Retry-After"))
	require.Empty(t, p.Effects())

	require.Equal(t, http.StatusCreated, mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status)
	require.Len(t, p.Effects(), 1)
}

// A real provider forgets idempotency keys after its retention period; reusing
// an expired key performs a NEW charge. The engine must look up instead.
func TestKeyRetention(t *testing.T) {
	p := fakeprovider.New(fakeprovider.Options{Hang: time.Second, KeyRetention: 100 * time.Millisecond})
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)

	first := mustCharge(t, srv.URL, "k1", body("cus_1", 10000))
	time.Sleep(150 * time.Millisecond)

	r, err := http.Get(srv.URL + "/charges?idempotency_key=k1")
	require.NoError(t, err)
	r.Body.Close()
	require.Equal(t, http.StatusNotFound, r.StatusCode)

	second := mustCharge(t, srv.URL, "k1", body("cus_1", 10000))
	require.Equal(t, http.StatusCreated, second.status)
	require.NotEqual(t, chargeID(t, first.body), chargeID(t, second.body))
	require.Len(t, p.Effects(), 2)
}

func TestFault_TimesLimitsRule(t *testing.T) {
	p, srv := newServer(t)
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "charges", Mode: "error_before_effect", Times: 2}))
	require.Equal(t, http.StatusInternalServerError, mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status)
	require.Equal(t, http.StatusInternalServerError, mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status)
	require.Equal(t, http.StatusCreated, mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status)
}

func TestFault_ZeroProbabilityMeansAlways(t *testing.T) {
	p, srv := newServer(t)
	p.SetFaults(faults(fakeprovider.Rule{Endpoint: "charges", Mode: "error_before_effect", Probability: 0}))
	for i := range 5 {
		require.Equal(t, http.StatusInternalServerError, mustCharge(t, srv.URL, fmt.Sprintf("k%d", i), body("cus_1", 10000)).status)
	}
}

func TestLookupByKey(t *testing.T) {
	_, srv := newServer(t)
	get := func(key string) resp {
		r, err := http.Get(srv.URL + "/charges?idempotency_key=" + key)
		require.NoError(t, err)
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return resp{r.StatusCode, string(b)}
	}
	require.Equal(t, http.StatusNotFound, get("k1").status)
	created := mustCharge(t, srv.URL, "k1", body("cus_1", 10000))
	found := get("k1")
	require.Equal(t, http.StatusOK, found.status)
	require.Equal(t, created.body, found.body)
}

// Review finding: a typo in a fault rule must fail loudly, never turn into a
// silent no-op that lets a fault-injection run "pass" without injecting anything.
func TestControlAPI_RejectsInvalidFaults(t *testing.T) {
	p, srv := newServer(t)
	ctx := context.Background()
	for _, r := range []fakeprovider.Rule{
		{Endpoint: "charges", Mode: "timeout_after_efect"},
		{Endpoint: "charges", Mode: "latency"},
		{Endpoint: "payouts", Mode: fakeprovider.ModeErrorBeforeEffect},
		{Endpoint: "charges", Mode: fakeprovider.ModeErrorBeforeEffect, Probability: 1.5},
		{Endpoint: "charges", Mode: fakeprovider.ModeErrorBeforeEffect, Probability: -0.1},
		{Endpoint: "charges", Mode: fakeprovider.ModeErrorBeforeEffect, Times: -1},
	} {
		err := fakeprovider.PutFaults(ctx, srv.URL, faults(r))
		require.Error(t, err, "%+v", r)
		require.Contains(t, err.Error(), "400")
	}
	// Nothing was installed: charges behave normally.
	require.Equal(t, http.StatusCreated, mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status)

	require.Panics(t, func() { p.SetFaults(faults(fakeprovider.Rule{Endpoint: "charges", Mode: "nope"})) })
}

func TestCalls(t *testing.T) {
	p, srv := newServer(t)
	mustCharge(t, srv.URL, "k1", body("cus_1", 10000))
	mustCharge(t, srv.URL, "k1", body("cus_1", 10000))
	r, err := http.Get(srv.URL + "/charges?idempotency_key=k1")
	require.NoError(t, err)
	r.Body.Close()
	require.Equal(t, map[string]int{"POST /charges": 2, "GET /charges": 1}, p.Calls())
	p.Reset()
	require.Empty(t, p.Calls())
}

func TestControlAPI(t *testing.T) {
	p, srv := newServer(t)
	ctx := context.Background()

	require.NoError(t, fakeprovider.PutFaults(ctx, srv.URL, faults(fakeprovider.Rule{Endpoint: "charges", Mode: "error_before_effect", Times: 1})))
	require.Equal(t, http.StatusInternalServerError, mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status)
	require.Equal(t, http.StatusCreated, mustCharge(t, srv.URL, "k1", body("cus_1", 10000)).status)

	effects, err := fakeprovider.FetchEffects(ctx, srv.URL)
	require.NoError(t, err)
	require.Equal(t, p.Effects(), effects)

	r, err := http.Post(srv.URL+"/_reset", "", nil)
	require.NoError(t, err)
	r.Body.Close()
	require.Equal(t, http.StatusNoContent, r.StatusCode)
	require.Empty(t, p.Effects())
}
