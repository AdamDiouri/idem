package provider_test

import (
	"context"
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

func TestCreateCharge_ServerGoneIsAmbiguous(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	out := provider.NewClient(url, time.Second).CreateCharge(context.Background(), "key-1", chargeReq("cus_1"))
	require.Equal(t, provider.Ambiguous, out.Class)
	require.Error(t, out.Err)
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
		want   provider.Class
	}{
		{"201 valid", 201, valid, provider.Success},
		{"200 valid", 200, valid, provider.Success},
		{"unparseable 2xx", 201, `not json`, provider.Ambiguous},
		{"amount mismatch", 201, `{"id":"ch_1","status":"succeeded","amount_cents":9999}`, provider.Ambiguous},
		{"pending status", 201, `{"id":"ch_1","status":"pending","amount_cents":10000}`, provider.Ambiguous},
		{"missing id", 201, `{"status":"succeeded","amount_cents":10000}`, provider.Ambiguous},
		{"400", 400, `{"error":{"code":"invalid_request"}}`, provider.Permanent},
		{"402", 402, `{"error":{"code":"card_declined"}}`, provider.Permanent},
		{"422 key reused", 422, `{"error":{"code":"idempotency_key_reused"}}`, provider.Ambiguous},
		{"422 other", 422, `{"error":{"code":"invalid_amount"}}`, provider.Permanent},
		{"409", 409, `{"error":{"code":"idempotency_key_in_flight"}}`, provider.Ambiguous},
		{"429", 429, ``, provider.Ambiguous},
		{"500", 500, ``, provider.Ambiguous},
		{"502", 502, ``, provider.Ambiguous},
		{"503", 503, ``, provider.Ambiguous},
		{"301", 301, ``, provider.Ambiguous},
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
			require.Equal(t, c.status, out.HTTPStatus)
			require.Equal(t, "key-1", gotKey)
			if c.want != provider.Success {
				require.Error(t, out.Err)
			}
		})
	}
}

func TestClassString(t *testing.T) {
	require.Equal(t, "success", provider.Success.String())
	require.Equal(t, "permanent", provider.Permanent.String())
	require.Equal(t, "ambiguous", provider.Ambiguous.String())
}
