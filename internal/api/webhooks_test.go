package api_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/api"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/metrics"
	idemtest "github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/webhook"
	"github.com/AdamDiouri/idem/internal/workflow"
)

var (
	whCurrent  = []byte("whsec_current")
	whPrevious = []byte("whsec_previous")
	eventBody  = `{"id":"evt_1","type":"payout.paid","created":1800000000,"data":{"payout_id":"po_1","status":"paid"}}`
)

// memInbox is an in-memory inbox.
type memInbox struct {
	mu   sync.Mutex
	rows map[string][]byte
	err  error
}

func (m *memInbox) Store(_ context.Context, id, _ string, payload []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return false, m.err
	}
	if m.rows == nil {
		m.rows = map[string][]byte{}
	}
	if _, ok := m.rows[id]; ok {
		return false, nil
	}
	m.rows[id] = payload
	return true, nil
}

func (m *memInbox) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rows)
}

func webhookServer(t *testing.T, inbox api.WebhookInbox, m *metrics.Metrics, secrets ...[]byte) string {
	t.Helper()
	opts := options()
	opts.WebhookSecrets, opts.Inbox, opts.Metrics = secrets, inbox, m
	srv := httptest.NewServer(api.NewServer(opts, nil, nil))
	t.Cleanup(srv.Close)
	return srv.URL
}

func sendWebhook(t *testing.T, url, body, signature string) (int, string, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/webhooks/provider", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set(webhook.ProviderSignatureHeader, signature)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), nil
}

func signed(secret []byte, body string) string { return webhook.Sign(secret, time.Now(), []byte(body)) }

func TestWebhooks_AcceptsSignedEvent(t *testing.T) {
	in := &memInbox{}
	url := webhookServer(t, in, nil, whCurrent)
	code, body, err := sendWebhook(t, url, eventBody, signed(whCurrent, eventBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, "accepted")
	require.Equal(t, []byte(eventBody), in.rows["evt_1"])
}

// F15: the provider stops retrying only on a 2xx, so a duplicate gets 200 too.
func TestWebhooks_DuplicateIs200(t *testing.T) {
	in := &memInbox{}
	url := webhookServer(t, in, nil, whCurrent)
	_, _, _ = sendWebhook(t, url, eventBody, signed(whCurrent, eventBody))
	code, body, err := sendWebhook(t, url, eventBody, signed(whCurrent, eventBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "duplicate")
	require.Equal(t, 1, in.count())
}

// F19: a forged webhook is refused and nothing is stored.
func TestWebhooks_RejectsBadSignature(t *testing.T) {
	in := &memInbox{}
	url := webhookServer(t, in, nil, whCurrent)
	code, body, err := sendWebhook(t, url, eventBody, signed([]byte("forged"), eventBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "bad_signature")
	require.Zero(t, in.count())
}

// Review focus 3 (F19): a correctly signed but replayed webhook is refused.
func TestWebhooks_RejectsStaleTimestamp(t *testing.T) {
	in := &memInbox{}
	url := webhookServer(t, in, nil, whCurrent)
	code, body, err := sendWebhook(t, url, eventBody, webhook.Sign(whCurrent, time.Now().Add(-6*time.Minute), []byte(eventBody)))
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body, "stale_signature")
	require.Zero(t, in.count())
}

// Review focus 2: during a secret rotation both secrets are accepted.
func TestWebhooks_AcceptsPreviousSecret(t *testing.T) {
	in := &memInbox{}
	url := webhookServer(t, in, nil, whCurrent, whPrevious)
	code, _, err := sendWebhook(t, url, eventBody, signed(whPrevious, eventBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	other := strings.Replace(eventBody, "evt_1", "evt_2", 1)
	code, _, err = sendWebhook(t, url, other, signed([]byte("neither"), other))
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, code)
	require.Equal(t, 1, in.count())
}

func TestWebhooks_RejectsMalformedAndOversized(t *testing.T) {
	in := &memInbox{}
	url := webhookServer(t, in, nil, whCurrent)
	for _, bad := range []string{"not json", `{"type":"payout.paid"}`} {
		code, body, err := sendWebhook(t, url, bad, signed(whCurrent, bad))
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, code, bad)
		require.Contains(t, body, "malformed")
	}
	big := `{"id":"evt_big","pad":"` + strings.Repeat("x", 65<<10) + `"}`
	code, _, err := sendWebhook(t, url, big, signed(whCurrent, big))
	require.NoError(t, err)
	require.Equal(t, http.StatusRequestEntityTooLarge, code)
	require.Zero(t, in.count())
}

func TestWebhooks_StoreErrorIs503(t *testing.T) {
	m := metrics.New()
	url := webhookServer(t, &memInbox{err: errors.New("db down")}, m, whCurrent)
	code, _, err := sendWebhook(t, url, eventBody, signed(whCurrent, eventBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, code, "the provider must retry")
	require.Equal(t, 1.0, testutil.ToFloat64(m.WebhooksReceived.WithLabelValues("store_error")))
}

func TestWebhooks_OffWithoutSecret(t *testing.T) {
	url := webhookServer(t, &memInbox{}, nil)
	code, _, err := sendWebhook(t, url, eventBody, signed(whCurrent, eventBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, code)
}

func TestWebhooks_CountsResults(t *testing.T) {
	m := metrics.New()
	url := webhookServer(t, &memInbox{}, m, whCurrent)
	_, _, _ = sendWebhook(t, url, eventBody, signed(whCurrent, eventBody))
	_, _, _ = sendWebhook(t, url, eventBody, signed([]byte("x"), eventBody))
	require.Equal(t, 1.0, testutil.ToFloat64(m.WebhooksReceived.WithLabelValues("accepted")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.WebhooksReceived.WithLabelValues("bad_signature")))
}

// F28: the API crashes after storing the event, before answering. The
// provider retries; the retry is a duplicate and the event is stored once.
func TestWebhooks_CrashAfterInsertIsRetriedSafely_F28(t *testing.T) {
	pool := idemtest.NewPool(t, dsn)
	t.Cleanup(failpoint.Reset)
	opts := options()
	opts.WebhookSecrets, opts.Inbox = [][]byte{whCurrent}, webhook.NewInbox(pool)
	srv := httptest.NewServer(api.NewServer(opts, workflow.NewSubmitter(pool, 5*time.Second, defaultFees), pool))
	t.Cleanup(srv.Close)
	failpoint.Enable(api.FPWebhookAfterInsert, failpoint.CrashOnce())

	code, _, err := sendWebhook(t, srv.URL, eventBody, signed(whCurrent, eventBody))
	require.True(t, err != nil || code >= 500, "no 2xx: the provider will retry (code %d, err %v)", code, err)
	code, body, err := sendWebhook(t, srv.URL, eventBody, signed(whCurrent, eventBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "duplicate")
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM webhook_inbox`).Scan(&n))
	require.Equal(t, 1, n)
}
