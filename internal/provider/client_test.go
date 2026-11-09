package provider_test

import (
	"context"
	"io"
	"sync/atomic"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/provider"
)

var wfID = uuid.MustParse("01936f3e-8c2a-7b1e-9f00-000000000001")

func chargeReq(customer string) provider.ChargeRequest {
	return provider.ChargeRequest{AmountCents: 10000, Currency: "EUR", CustomerID: customer, WorkflowID: wfID}
}

func fake(t *testing.T) (*fakeprovider.Provider, string) {
	t.Helper()
	p := fakeprovider.New(fakeprovider.Options{Hang: 5 * time.Second})
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)
	return p, srv.URL
}

func TestCreateCharge_SuccessAgainstFakeProvider(t *testing.T) {
	p, url := fake(t)
	c := provider.NewClient(url, time.Second)

	out := c.CreateCharge(context.Background(), "key-1", chargeReq("cus_1"))
	require.Equal(t, provider.Success, out.Class, "err: %v", out.Err)
	require.True(t, strings.HasPrefix(out.ProviderRef, "ch_"))
	require.Equal(t, http.StatusCreated, out.HTTPStatus)
	require.NoError(t, out.Err)

	effects := p.Effects()
	require.Len(t, effects, 1)
	require.Equal(t, "key-1", effects[0].IdempotencyKey)
	require.Equal(t, wfID.String(), effects[0].WorkflowID)
	require.Equal(t, out.ProviderRef, effects[0].ID)
}

func TestCreateCharge_DeclineIsPermanent(t *testing.T) {
	_, url := fake(t)
	out := provider.NewClient(url, time.Second).CreateCharge(context.Background(), "key-1", chargeReq("cus_decline_1"))
	require.Equal(t, provider.Permanent, out.Class)
	require.Equal(t, http.StatusPaymentRequired, out.HTTPStatus)
	require.Error(t, out.Err)
}

func TestCreateCharge_TimeoutIsAmbiguous(t *testing.T) {
	p, url := fake(t)
	p.SetFaults(fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "charges", Mode: fakeprovider.ModeTimeoutAfterEffect, Times: 1}}})

	out := provider.NewClient(url, 100*time.Millisecond).CreateCharge(context.Background(), "key-1", chargeReq("cus_1"))
	require.Equal(t, provider.Ambiguous, out.Class)
	require.Error(t, out.Err)
	require.Len(t, p.Effects(), 1, "money moved even though the outcome is ambiguous")
}

// A refused connection means the request never left this machine: no effect.
func TestCreateCharge_ServerGoneIsNoEffect(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	out := provider.NewClient(url, time.Second).CreateCharge(context.Background(), "key-1", chargeReq("cus_1"))
	require.Equal(t, provider.NoEffect, out.Class)
	require.Error(t, out.Err)
}

// Review finding (critical): Go's transport silently retries a POST carrying an
// Idempotency-Key on a new connection when a reused connection drops. If the
// provider read the first send and then died, the caller only sees the retry's
// "connection refused" — but the charge may have happened. Must be Ambiguous.
func TestCreateCharge_RequestSentThenProviderDiesIsAmbiguous(t *testing.T) {
	var requests atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"ch_1","status":"succeeded","amount_cents":10000,"currency":"EUR"}`))
			return
		}
		// Second request: read in full, then the provider dies before answering.
		_ = srv.Listener.Close()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(srv.Close)

	c := provider.NewClient(srv.URL, 2*time.Second)
	first := c.CreateCharge(context.Background(), "key-1", chargeReq("cus_1"))
	require.Equal(t, provider.Success, first.Class, "err: %v", first.Err)

	second := c.CreateCharge(context.Background(), "key-2", chargeReq("cus_1")) // reuses the keep-alive connection
	require.GreaterOrEqual(t, requests.Load(), int32(2), "the provider received the second charge")
	require.Equal(t, provider.Ambiguous, second.Class, "err: %v", second.Err)
}

func TestCreateCharge_CancelledContextIsAmbiguous(t *testing.T) {
	_, url := fake(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := provider.NewClient(url, time.Second).CreateCharge(ctx, "key-1", chargeReq("cus_1"))
	require.Equal(t, provider.Ambiguous, out.Class)
}

func TestClassify(t *testing.T) {
	const valid = `{"id":"ch_1","status":"succeeded","amount_cents":10000,"currency":"EUR"}`
	cases := []struct {
		name   string
		status int
		body   string
		want     provider.Class
		inFlight bool
	}{
		{"201 valid", 201, valid, provider.Success, false},
		{"200 valid", 200, valid, provider.Success, false},
		{"unparseable 2xx", 201, `not json`, provider.Ambiguous, false},
		{"amount mismatch", 201, `{"id":"ch_1","status":"succeeded","amount_cents":9999}`, provider.Ambiguous, false},
		{"pending status", 201, `{"id":"ch_1","status":"pending","amount_cents":10000}`, provider.Ambiguous, false},
		{"missing id", 201, `{"status":"succeeded","amount_cents":10000}`, provider.Ambiguous, false},
		{"400", 400, `{"error":{"code":"invalid_request"}}`, provider.Permanent, false},
		{"402", 402, `{"error":{"code":"card_declined"}}`, provider.Permanent, false},
		{"422 key reused", 422, `{"error":{"code":"idempotency_key_reused"}}`, provider.Ambiguous, false},
		{"422 other", 422, `{"error":{"code":"invalid_amount"}}`, provider.Permanent, false},
		{"409", 409, `{"error":{"code":"idempotency_key_in_flight"}}`, provider.Ambiguous, true},
		{"429", 429, ``, provider.NoEffect, false},
		{"currency mismatch", 201, `{"id":"ch_1","status":"succeeded","amount_cents":10000,"currency":"USD"}`, provider.Ambiguous, false},
		{"500", 500, ``, provider.Ambiguous, false},
		{"502", 502, ``, provider.Ambiguous, false},
		{"503", 503, ``, provider.Ambiguous, false},
		{"301", 301, ``, provider.Ambiguous, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var gotKey string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotKey = r.Header.Get("Idempotency-Key")
				w.WriteHeader(c.status)
				_, _ = w.Write([]byte(c.body))
			}))
			defer srv.Close()

			out := provider.NewClient(srv.URL, time.Second).CreateCharge(context.Background(), "key-1", chargeReq("cus_1"))
			require.Equal(t, c.want, out.Class, "err: %v", out.Err)
			require.Equal(t, c.inFlight, out.InFlight)
			require.Equal(t, c.status, out.HTTPStatus)
			require.Equal(t, "key-1", gotKey)
			if c.want != provider.Success {
				require.Error(t, out.Err)
			}
		})
	}
}

func TestRetryAfter(t *testing.T) {
	cases := []struct {
		header string
		want   time.Duration
	}{
		{"1", time.Second},
		{"", 0},
		{"abc", 0},
		{"-5", 0},
		{"99999999999", time.Hour},
		{"Wed, 21 Oct 2015 07:28:00 GMT", 0}, // HTTP-date form is not supported
	}
	for _, c := range cases {
		t.Run(c.header, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if c.header != "" {
					w.Header().Set("Retry-After", c.header)
				}
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			defer srv.Close()
			out := provider.NewClient(srv.URL, time.Second).CreateCharge(context.Background(), "key-1", chargeReq("cus_1"))
			require.Equal(t, provider.NoEffect, out.Class)
			require.Equal(t, c.want, out.RetryAfter)
		})
	}
}

func TestLookupCharge(t *testing.T) {
	ctx := context.Background()
	_, url := fake(t)
	c := provider.NewClient(url, time.Second)

	out, found := c.LookupCharge(ctx, "key-1", chargeReq("cus_1"))
	require.False(t, found)
	require.Equal(t, provider.Permanent, out.Class)
	require.Error(t, out.Err)

	created := c.CreateCharge(ctx, "key-1", chargeReq("cus_1"))
	out, found = c.LookupCharge(ctx, "key-1", chargeReq("cus_1"))
	require.True(t, found)
	require.Equal(t, provider.Success, out.Class)
	require.Equal(t, created.ProviderRef, out.ProviderRef)

	c.CreateCharge(ctx, "key-2", chargeReq("cus_decline_1"))
	out, found = c.LookupCharge(ctx, "key-2", chargeReq("cus_decline_1"))
	require.True(t, found)
	require.Equal(t, provider.Permanent, out.Class)

	srv := httptest.NewServer(http.NotFoundHandler())
	gone := srv.URL
	srv.Close()
	out, found = provider.NewClient(gone, time.Second).LookupCharge(ctx, "key-1", chargeReq("cus_1"))
	require.False(t, found)
	require.Equal(t, provider.Ambiguous, out.Class)
}

func TestClassString(t *testing.T) {
	require.Equal(t, "no_effect", provider.NoEffect.String())
	require.Equal(t, "success", provider.Success.String())
	require.Equal(t, "permanent", provider.Permanent.String())
	require.Equal(t, "ambiguous", provider.Ambiguous.String())
}

func payoutReq(merchant string) provider.PayoutRequest {
	return provider.PayoutRequest{AmountCents: 9825, Currency: "EUR", MerchantID: merchant, WorkflowID: wfID}
}

func refundReq(chargeID string) provider.RefundRequest {
	return provider.RefundRequest{AmountCents: 10000, Currency: "EUR", ChargeID: chargeID, WorkflowID: wfID}
}

func TestCreatePayout_SuccessAndInvalidAccount(t *testing.T) {
	ctx := context.Background()
	p, url := fake(t)
	c := provider.NewClient(url, time.Second)

	ok := c.CreatePayout(ctx, "po-key-1", payoutReq("mer_1"))
	require.Equal(t, provider.Success, ok.Class, "err: %v", ok.Err)
	require.True(t, strings.HasPrefix(ok.ProviderRef, "po_"))
	require.Equal(t, "payout", p.Effects()[0].Type)

	bad := c.CreatePayout(ctx, "po-key-2", payoutReq("mer_invalid_1"))
	require.Equal(t, provider.Permanent, bad.Class)
	require.Equal(t, http.StatusUnprocessableEntity, bad.HTTPStatus)
}

func TestCreateRefund_SuccessAndInvalid(t *testing.T) {
	ctx := context.Background()
	_, url := fake(t)
	c := provider.NewClient(url, time.Second)
	charge := c.CreateCharge(ctx, "ch-key-1", chargeReq("cus_1"))
	require.Equal(t, provider.Success, charge.Class)

	ok := c.CreateRefund(ctx, "re-key-1", refundReq(charge.ProviderRef))
	require.Equal(t, provider.Success, ok.Class, "err: %v", ok.Err)
	require.True(t, strings.HasPrefix(ok.ProviderRef, "re_"))

	bad := c.CreateRefund(ctx, "re-key-2", refundReq("ch_missing"))
	require.Equal(t, provider.Permanent, bad.Class)
}

func TestPayout_WrongStatusIsAmbiguous(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"po_1","status":"pending","amount_cents":9825,"currency":"EUR"}`))
	}))
	defer srv.Close()
	out := provider.NewClient(srv.URL, time.Second).CreatePayout(context.Background(), "k", payoutReq("mer_1"))
	require.Equal(t, provider.Ambiguous, out.Class)
}

func TestLookupPayoutAndRefund(t *testing.T) {
	ctx := context.Background()
	_, url := fake(t)
	c := provider.NewClient(url, time.Second)

	out, found := c.LookupPayout(ctx, "po-key-1", payoutReq("mer_1"))
	require.False(t, found)
	require.Equal(t, provider.Permanent, out.Class)
	created := c.CreatePayout(ctx, "po-key-1", payoutReq("mer_1"))
	out, found = c.LookupPayout(ctx, "po-key-1", payoutReq("mer_1"))
	require.True(t, found)
	require.Equal(t, provider.Success, out.Class)
	require.Equal(t, created.ProviderRef, out.ProviderRef)

	charge := c.CreateCharge(ctx, "ch-key-1", chargeReq("cus_1"))
	refund := c.CreateRefund(ctx, "re-key-1", refundReq(charge.ProviderRef))
	out, found = c.LookupRefund(ctx, "re-key-1", refundReq(charge.ProviderRef))
	require.True(t, found)
	require.Equal(t, refund.ProviderRef, out.ProviderRef)
}

func TestLookup_InFlightIsAmbiguous(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"code":"idempotency_key_in_flight"}}`))
	}))
	defer srv.Close()
	out, found := provider.NewClient(srv.URL, time.Second).LookupCharge(context.Background(), "k", chargeReq("cus_1"))
	require.False(t, found)
	require.Equal(t, provider.Ambiguous, out.Class)
}
