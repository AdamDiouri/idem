package provider_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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

// Slice 2.4: the provider's refusal code is kept, so a failed workflow can
// tell its shop why (e.g. card_declined).
func TestCreateCharge_DeclineKeepsCode(t *testing.T) {
	_, url := fake(t)
	out := provider.NewClient(url, time.Second).CreateCharge(context.Background(), "key-1", chargeReq("cus_decline_1"))
	require.Equal(t, provider.Permanent, out.Class)
	require.Equal(t, "card_declined", out.Code)
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
		name     string
		status   int
		body     string
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
		{"Wed, 21 Oct 2015 07:28:00 GMT", 0}, // an HTTP date in the past
		{"Thu, 01 Jan 2099 00:00:00 GMT", time.Hour}, // far future: capped
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
	require.Equal(t, "accepted", provider.Accepted.String())
	require.Equal(t, "pending", provider.Pending.String())
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
	require.Equal(t, provider.Accepted, ok.Class, "err: %v", ok.Err)
	require.Equal(t, http.StatusAccepted, ok.HTTPStatus)
	require.True(t, strings.HasPrefix(ok.ProviderRef, "po_"))
	require.Equal(t, "payout", p.Effects()[0].Type) // settles at once (PayoutSettle 0)

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

func stub(t *testing.T, status int, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// M2 (design §4.2): 202 pending with a payout ID means the provider accepted
// the payout; the result comes later.
func TestCreatePayout_AcceptedIsAccepted(t *testing.T) {
	url := stub(t, http.StatusAccepted, `{"id":"po_1","status":"pending","amount_cents":9825,"currency":"EUR"}`)
	out := provider.NewClient(url, time.Second).CreatePayout(context.Background(), "k", payoutReq("mer_1"))
	require.Equal(t, provider.Accepted, out.Class, "err: %v", out.Err)
	require.Equal(t, "po_1", out.ProviderRef)
	require.NoError(t, out.Err)
}

func TestCreatePayout_AcceptedWithoutIDIsAmbiguous(t *testing.T) {
	for name, body := range map[string]string{
		"no id":           `{"status":"pending","amount_cents":9825,"currency":"EUR"}`,
		"amount mismatch": `{"id":"po_1","status":"pending","amount_cents":1,"currency":"EUR"}`,
		"unparseable":     `nope`,
	} {
		t.Run(name, func(t *testing.T) {
			out := provider.NewClient(stub(t, http.StatusAccepted, body), time.Second).CreatePayout(context.Background(), "k", payoutReq("mer_1"))
			require.Equal(t, provider.Ambiguous, out.Class)
		})
	}
}

func TestGetPayout_Classifies(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   provider.Class
		errHas string
	}{
		{"paid", 200, `{"id":"po_1","status":"paid","amount_cents":9825,"currency":"EUR"}`, provider.Success, ""},
		{"failed", 200, `{"id":"po_1","status":"failed","failure_code":"account_closed","amount_cents":9825,"currency":"EUR"}`, provider.Permanent, "account_closed"},
		{"pending", 200, `{"id":"po_1","status":"pending","amount_cents":9825,"currency":"EUR"}`, provider.Pending, ""},
		{"unknown payout", 404, `{"error":{"code":"not_found"}}`, provider.Ambiguous, ""},
		{"amount mismatch", 200, `{"id":"po_1","status":"paid","amount_cents":1,"currency":"EUR"}`, provider.Ambiguous, "amount"},
		{"unparseable", 200, `nope`, provider.Ambiguous, ""},
		{"server error", 500, `{}`, provider.Ambiguous, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := provider.NewClient(stub(t, tc.status, tc.body), time.Second).GetPayout(context.Background(), "po_1", payoutReq("mer_1"))
			require.Equal(t, tc.want, out.Class, "err: %v", out.Err)
			if tc.want == provider.Success {
				require.Equal(t, "po_1", out.ProviderRef)
			}
			if tc.errHas != "" {
				require.ErrorContains(t, out.Err, tc.errHas)
			}
		})
	}
}

func TestGetPayout_AgainstFakeProvider(t *testing.T) {
	ctx := context.Background()
	_, url := fake(t)
	c := provider.NewClient(url, time.Second)
	accepted := c.CreatePayout(ctx, "po-key-1", payoutReq("mer_1"))
	require.Equal(t, provider.Accepted, accepted.Class)
	out := c.GetPayout(ctx, accepted.ProviderRef, payoutReq("mer_1"))
	require.Equal(t, provider.Success, out.Class, "err: %v", out.Err)
	require.Equal(t, accepted.ProviderRef, out.ProviderRef)
}

func TestLookupPayout_PendingIsAccepted(t *testing.T) {
	ctx := context.Background()
	p := fakeprovider.New(fakeprovider.Options{Hang: 5 * time.Second, PayoutSettle: time.Hour})
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)
	c := provider.NewClient(srv.URL, time.Second)
	accepted := c.CreatePayout(ctx, "po-key-1", payoutReq("mer_1"))
	out, found := c.LookupPayout(ctx, "po-key-1", payoutReq("mer_1"))
	require.True(t, found)
	require.Equal(t, provider.Accepted, out.Class, "err: %v", out.Err)
	require.Equal(t, accepted.ProviderRef, out.ProviderRef)
}

// Final review, Important 2: a lookup that finds an accepted payout settled
// failed is a definite refusal, not "unknown".
func TestLookupPayout_FailedIsPermanent(t *testing.T) {
	ctx := context.Background()
	p := fakeprovider.New(fakeprovider.Options{Hang: 5 * time.Second})
	p.SetFaults(fakeprovider.Faults{Rules: []fakeprovider.Rule{{Endpoint: "payouts", Mode: fakeprovider.ModePayoutFail, Times: 1}}})
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)
	c := provider.NewClient(srv.URL, time.Second)
	require.Equal(t, provider.Accepted, c.CreatePayout(ctx, "po-key-1", payoutReq("mer_1")).Class)

	out, found := c.LookupPayout(ctx, "po-key-1", payoutReq("mer_1"))
	require.True(t, found)
	require.Equal(t, provider.Permanent, out.Class, "err: %v", out.Err)
	require.ErrorContains(t, out.Err, "account_closed")
}

// Slice 2.3b (review R12): Retry-After may be an HTTP date (RFC 9110).
func TestRetryAfter_HTTPDate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", time.Now().Add(90*time.Second).UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	out := provider.NewClient(srv.URL, time.Second).CreateCharge(context.Background(), "key-1", chargeReq("cus_1"))
	require.Equal(t, provider.NoEffect, out.Class)
	require.InDelta(t, 90*time.Second, out.RetryAfter, float64(2*time.Second))
}

// Slice 2.3b (review R12): a lookup that is rate limited says nothing about the
// request, but the provider's Retry-After must still reach the retry delay.
func TestLookup_RateLimitedKeepsRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	out, found := provider.NewClient(srv.URL, time.Second).LookupCharge(context.Background(), "key-1", chargeReq("cus_1"))
	require.False(t, found)
	require.Equal(t, provider.Ambiguous, out.Class)
	require.Equal(t, 120*time.Second, out.RetryAfter)
}
