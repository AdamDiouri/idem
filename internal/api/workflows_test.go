package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/api"
	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/workflow"
)

var dsn string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunWithPostgres(m, &dsn))
}

const (
	token     = "tok-aaaaaaaaaaaaaaaa"
	validBody = `{"amount_cents":10000,"currency":"EUR","customer_id":"cus_1","merchant_id":"mer_1"}`
)

var defaultFees = workflow.FeeConfig{RateBps: 150, FixedCents: 25}

func options() api.Options {
	return api.Options{
		Tokens: map[string]string{token: "client_a"},
		Logger: slog.New(slog.DiscardHandler),
	}
}

type fakeSubmitter struct {
	calls []workflow.SubmitRequest
	res   workflow.SubmitResult
	err   error
}

func (f *fakeSubmitter) Submit(_ context.Context, req workflow.SubmitRequest) (workflow.SubmitResult, error) {
	f.calls = append(f.calls, req)
	return f.res, f.err
}

type fakePinger struct{ err error }

func (p fakePinger) Ping(context.Context) error { return p.err }

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Field   string `json:"field"`
	} `json:"error"`
}

func post(t *testing.T, h http.Handler, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/workflows", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", "key-1")
	for k, v := range headers {
		if v == "" {
			req.Header.Del(k)
		} else {
			req.Header.Set(k, v)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) errorBody {
	t.Helper()
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var e errorBody
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &e), "body: %s", rec.Body.String())
	return e
}

func TestPostWorkflows_RejectedBeforeSubmit(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		headers map[string]string
		status  int
		code    string
		field   string
	}{
		{"no auth header", validBody, map[string]string{"Authorization": ""}, 401, "unauthorized", ""},
		{"wrong token", validBody, map[string]string{"Authorization": "Bearer tok-bbbbbbbbbbbbbbbb"}, 401, "unauthorized", ""},
		{"not bearer scheme", validBody, map[string]string{"Authorization": "Basic " + token}, 401, "unauthorized", ""},
		{"missing key", validBody, map[string]string{"Idempotency-Key": ""}, 400, "idempotency_key_required", ""},
		{"key too long", validBody, map[string]string{"Idempotency-Key": strings.Repeat("k", 256)}, 400, "idempotency_key_invalid", ""},
		{"key with space", validBody, map[string]string{"Idempotency-Key": "abc def"}, 400, "idempotency_key_invalid", ""},
		{"key non-ascii", validBody, map[string]string{"Idempotency-Key": "ключ"}, 400, "idempotency_key_invalid", ""},
		{"body too large", `{"customer_id":"` + strings.Repeat("a", 65*1024) + `"}`, nil, 413, "body_too_large", ""},
		{"malformed json", `{"amount_cents":`, nil, 400, "invalid_json", ""},
		{"empty body", ``, nil, 400, "invalid_json", ""},
		{"unknown field", `{"amount_cents":10000,"currency":"EUR","customer_id":"c","merchant_id":"m","extra":1}`, nil, 400, "invalid_json", ""},
		{"trailing data", validBody + `{"x":1}`, nil, 400, "invalid_json", ""},
		{"fractional amount", `{"amount_cents":10.5,"currency":"EUR","customer_id":"c","merchant_id":"m"}`, nil, 400, "invalid_json", ""},
		{"amount as string", `{"amount_cents":"100","currency":"EUR","customer_id":"c","merchant_id":"m"}`, nil, 400, "invalid_json", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sub := &fakeSubmitter{}
			h := api.NewServer(options(), sub, fakePinger{})
			rec := post(t, h, c.body, c.headers)

			require.Equal(t, c.status, rec.Code, "body: %s", rec.Body.String())
			e := decodeError(t, rec)
			require.Equal(t, c.code, e.Error.Code)
			require.Equal(t, c.field, e.Error.Field)
			require.NotEmpty(t, e.Error.Message)
			require.Empty(t, sub.calls, "submitter must not be called")
		})
	}
}

func TestPostWorkflows_SubmitErrorsMapped(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"key reused", workflow.ErrKeyReuseMismatch, 422, "idempotency_key_reused"},
		{"key in use", workflow.ErrKeyInUse, 409, "idempotency_key_in_use"},
		{"internal", errors.New("db exploded"), 500, "internal"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := api.NewServer(options(), &fakeSubmitter{err: c.err}, fakePinger{})
			rec := post(t, h, validBody, nil)
			require.Equal(t, c.status, rec.Code)
			require.Equal(t, c.code, decodeError(t, rec).Error.Code)
			require.NotContains(t, rec.Body.String(), "db exploded", "internal errors must not leak")
		})
	}
}

func TestPostWorkflows_PassesDecodedRequestToSubmitter(t *testing.T) {
	sub := &fakeSubmitter{res: workflow.SubmitResult{Body: json.RawMessage(`{"workflow_id":"x"}`)}}
	h := api.NewServer(options(), sub, fakePinger{})
	rec := post(t, h, validBody, nil)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Len(t, sub.calls, 1)
	require.Equal(t, workflow.SubmitRequest{
		ClientID:       "client_a",
		IdempotencyKey: "key-1",
		Request:        workflow.CreateWorkflowRequest{AmountCents: 10000, Currency: "EUR", CustomerID: "cus_1", MerchantID: "mer_1"},
	}, sub.calls[0])
}

func TestPostWorkflows_ValidationErrorFromSubmitter(t *testing.T) {
	sub := &fakeSubmitter{err: &workflow.ValidationError{Field: "amount_cents", Message: "amount does not cover fee"}}
	h := api.NewServer(options(), sub, fakePinger{})
	rec := post(t, h, validBody, nil)

	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	e := decodeError(t, rec)
	require.Equal(t, "validation_failed", e.Error.Code)
	require.Equal(t, "amount_cents", e.Error.Field)
	require.Equal(t, "amount does not cover fee", e.Error.Message)
}

func TestPostWorkflows_CreatedAndReplayed(t *testing.T) {
	body := `{"workflow_id":"x"}`

	h := api.NewServer(options(), &fakeSubmitter{res: workflow.SubmitResult{Body: json.RawMessage(body)}}, fakePinger{})
	rec := post(t, h, validBody, nil)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.Equal(t, body, rec.Body.String())
	require.Empty(t, rec.Header().Get("Idempotent-Replayed"))

	h = api.NewServer(options(), &fakeSubmitter{res: workflow.SubmitResult{Body: json.RawMessage(body), Replayed: true}}, fakePinger{})
	rec = post(t, h, validBody, nil)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, body, rec.Body.String())
	require.Equal(t, "true", rec.Header().Get("Idempotent-Replayed"))
}

type blockingSubmitter struct{}

func (blockingSubmitter) Submit(ctx context.Context, _ workflow.SubmitRequest) (workflow.SubmitResult, error) {
	<-ctx.Done()
	return workflow.SubmitResult{}, fmt.Errorf("acquire connection: %w", ctx.Err())
}

// Review I1: database work must stop at the request deadline and answer with a
// status, instead of running past the server's write timeout.
func TestPostWorkflows_RequestTimeout(t *testing.T) {
	opts := options()
	opts.RequestTimeout = 50 * time.Millisecond
	h := api.NewServer(opts, blockingSubmitter{}, fakePinger{})

	start := time.Now()
	rec := post(t, h, validBody, nil)

	require.Less(t, time.Since(start), 2*time.Second)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	e := decodeError(t, rec)
	require.Equal(t, "request_timeout", e.Error.Code)
	require.Contains(t, e.Error.Message, "same Idempotency-Key")
}

func TestHealthz(t *testing.T) {
	h := api.NewServer(options(), &fakeSubmitter{}, fakePinger{err: errors.New("down")})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil)) // no auth
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"status":"ok"}`, rec.Body.String())
}

func TestReadyz(t *testing.T) {
	h := api.NewServer(options(), &fakeSubmitter{}, fakePinger{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"status":"ok"}`, rec.Body.String())

	h = api.NewServer(options(), &fakeSubmitter{}, fakePinger{err: errors.New("connection refused")})
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(t, `{"status":"unavailable"}`, rec.Body.String())
}
