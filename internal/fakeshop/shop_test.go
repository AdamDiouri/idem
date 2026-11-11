package fakeshop_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeshop"
	"github.com/AdamDiouri/idem/internal/webhook"
)

const secret = "whsec-shop-0123456789"

func newShop(t *testing.T, opts fakeshop.Options) (*fakeshop.Shop, string) {
	t.Helper()
	opts.Secret = []byte(secret)
	s := fakeshop.New(opts)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return s, srv.URL
}

// deliver sends one event the way idem's relay does.
func deliver(t *testing.T, url, eventID, body, signingSecret string, timeout time.Duration) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/hooks", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Idem-Event-Id", eventID)
	req.Header.Set("Idem-Event-Type", "workflow.succeeded")
	req.Header.Set("Idem-Signature", webhook.Sign([]byte(signingSecret), time.Now(), []byte(body)))
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	return resp, err
}

func view(wf string) string {
	return `{"workflow_id":"` + wf + `","status":"succeeded","amount_cents":10000,"fee_cents":175,"currency":"EUR"}`
}

func seen(t *testing.T, url string) fakeshop.Seen {
	t.Helper()
	s, err := fakeshop.FetchSeen(context.Background(), url)
	require.NoError(t, err)
	return s
}

func setFaults(t *testing.T, url string, rules ...fakeshop.Rule) {
	t.Helper()
	require.NoError(t, fakeshop.PutFaults(context.Background(), url, fakeshop.Faults{Seed: 1, Rules: rules}))
}

// Slice 2.5 (M2 design §5.4): the shop deduplicates on the event ID.
func TestShop_RecordsSignedEventOnce(t *testing.T) {
	_, url := newShop(t, fakeshop.Options{})
	id, wf := uuid.NewString(), uuid.NewString()
	for range 2 {
		resp, err := deliver(t, url, id, view(wf), secret, time.Second)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
	got := seen(t, url)
	require.Equal(t, []fakeshop.SeenEvent{{EventID: id, WorkflowID: wf, Type: "workflow.succeeded", Status: "succeeded", Deliveries: 2}}, got.Events)
	require.Equal(t, 1, got.Duplicates)
	require.Zero(t, got.BadSignatures)
}

func TestShop_RejectsBadSignature(t *testing.T) {
	_, url := newShop(t, fakeshop.Options{})
	resp, err := deliver(t, url, uuid.NewString(), view(uuid.NewString()), "whsec-wrong-0123456789", time.Second)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	got := seen(t, url)
	require.Empty(t, got.Events)
	require.Equal(t, 1, got.BadSignatures)
}

func TestShop_Down(t *testing.T) {
	_, url := newShop(t, fakeshop.Options{})
	setFaults(t, url, fakeshop.Rule{Mode: fakeshop.ModeDown})
	_, err := deliver(t, url, uuid.NewString(), view(uuid.NewString()), secret, time.Second)
	require.Error(t, err, "the connection is closed without an answer")
	require.Empty(t, seen(t, url).Events)
}

func TestShop_5xxRecordsNothing(t *testing.T) {
	_, url := newShop(t, fakeshop.Options{})
	setFaults(t, url, fakeshop.Rule{Mode: fakeshop.Mode5xx})
	resp, err := deliver(t, url, uuid.NewString(), view(uuid.NewString()), secret, time.Second)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Empty(t, seen(t, url).Events)
}

// The shop processed the event but its answer says otherwise: the retry is a
// duplicate it must recognise.
func TestShop_AckThen5xxRecords(t *testing.T) {
	_, url := newShop(t, fakeshop.Options{})
	setFaults(t, url, fakeshop.Rule{Mode: fakeshop.ModeAckThen5xx})
	resp, err := deliver(t, url, uuid.NewString(), view(uuid.NewString()), secret, time.Second)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Len(t, seen(t, url).Events, 1)
}

func TestShop_AckAfterTimeoutRecordsThenAnswersLate(t *testing.T) {
	_, url := newShop(t, fakeshop.Options{LateAck: 300 * time.Millisecond})
	setFaults(t, url, fakeshop.Rule{Mode: fakeshop.ModeAckAfterTimeout})
	_, err := deliver(t, url, uuid.NewString(), view(uuid.NewString()), secret, 100*time.Millisecond)
	require.Error(t, err, "the sender gave up before the answer")
	require.Len(t, seen(t, url).Events, 1, "but the shop did the work")
}

func TestShop_SlowAnswers200(t *testing.T) {
	_, url := newShop(t, fakeshop.Options{Slow: 100 * time.Millisecond})
	setFaults(t, url, fakeshop.Rule{Mode: fakeshop.ModeSlow})
	start := time.Now()
	resp, err := deliver(t, url, uuid.NewString(), view(uuid.NewString()), secret, time.Second)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
	require.Len(t, seen(t, url).Events, 1)
}

func TestFaults_ValidateRejectsUnknownMode(t *testing.T) {
	require.Error(t, fakeshop.Faults{Rules: []fakeshop.Rule{{Mode: "explode"}}}.Validate())
	require.Error(t, fakeshop.Faults{Rules: []fakeshop.Rule{{Mode: fakeshop.ModeDown, Probability: 2}}}.Validate())
	require.NoError(t, fakeshop.Faults{Rules: []fakeshop.Rule{{Mode: fakeshop.ModeSlow, Probability: 0.5, Times: 3}}}.Validate())
	_, url := newShop(t, fakeshop.Options{})
	require.Error(t, fakeshop.PutFaults(context.Background(), url, fakeshop.Faults{Rules: []fakeshop.Rule{{Mode: "explode"}}}))
}

func TestShop_CountsFaultHits(t *testing.T) {
	_, url := newShop(t, fakeshop.Options{})
	setFaults(t, url, fakeshop.Rule{Mode: fakeshop.Mode5xx, Times: 1})
	for range 2 {
		_, _ = deliver(t, url, uuid.NewString(), view(uuid.NewString()), secret, time.Second)
	}
	hits, err := fakeshop.FetchFaultHits(context.Background(), url)
	require.NoError(t, err)
	require.Equal(t, map[string]int{fakeshop.Mode5xx: 1}, hits)
	require.Len(t, seen(t, url).Events, 1, "the second delivery passed")
}
