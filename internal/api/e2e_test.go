package api_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/api"
	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/workflow"
)

type response struct {
	status   int
	replayed string
	body     string
}

func newE2EServer(t *testing.T) *httptest.Server {
	t.Helper()
	pool := testutil.NewPool(t, dsn)
	srv := httptest.NewServer(api.NewServer(options(), workflow.NewSubmitter(pool, 5*time.Second, defaultFees), pool))
	t.Cleanup(srv.Close)
	return srv
}

func postHTTP(t *testing.T, url, key, body string) response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/workflows", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return response{status: resp.StatusCode, replayed: resp.Header.Get("Idempotent-Replayed"), body: string(b)}
}

func workflowID(t *testing.T, body string) string {
	t.Helper()
	var c struct {
		WorkflowID string `json:"workflow_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &c))
	require.NotEmpty(t, c.WorkflowID)
	return c.WorkflowID
}

func TestE2E_F1_RetryReplays(t *testing.T) {
	srv := newE2EServer(t)
	first := postHTTP(t, srv.URL, "e2e-1", validBody)
	second := postHTTP(t, srv.URL, "e2e-1", validBody)

	require.Equal(t, http.StatusCreated, first.status, first.body)
	require.Empty(t, first.replayed)
	require.Equal(t, http.StatusCreated, second.status, second.body)
	require.Equal(t, "true", second.replayed)
	require.Equal(t, first.body, second.body)
}

func TestE2E_RetryWithReformattedJSON(t *testing.T) {
	srv := newE2EServer(t)
	first := postHTTP(t, srv.URL, "e2e-1", validBody)
	reformatted := "{\n  \"merchant_id\": \"mer_1\",\n  \"customer_id\": \"cus_1\",\n  \"currency\": \"EUR\",\n  \"amount_cents\": 10000\n}"
	second := postHTTP(t, srv.URL, "e2e-1", reformatted)

	require.Equal(t, http.StatusCreated, second.status, second.body)
	require.Equal(t, "true", second.replayed)
	require.Equal(t, first.body, second.body)
}

func TestE2E_F2_DifferentBody(t *testing.T) {
	srv := newE2EServer(t)
	postHTTP(t, srv.URL, "e2e-1", validBody)
	second := postHTTP(t, srv.URL, "e2e-1", strings.Replace(validBody, "10000", "20000", 1))

	require.Equal(t, http.StatusUnprocessableEntity, second.status)
	require.Contains(t, second.body, `"idempotency_key_reused"`)
}

// Regression (review C1): a fee increase between the original request and a
// retry must not turn the retry of an existing workflow into a 422.
func TestE2E_RetryAfterFeeIncreaseReplays(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	body := `{"amount_cents":100,"currency":"EUR","customer_id":"cus_1","merchant_id":"mer_1"}`

	before := httptest.NewServer(api.NewServer(options(), workflow.NewSubmitter(pool, 5*time.Second, defaultFees), pool))
	t.Cleanup(before.Close)
	first := postHTTP(t, before.URL, "fee-change", body)
	require.Equal(t, http.StatusCreated, first.status, first.body)

	raised := workflow.FeeConfig{RateBps: 150, FixedCents: 100} // 100 cents no longer covers the fee
	after := httptest.NewServer(api.NewServer(options(), workflow.NewSubmitter(pool, 5*time.Second, raised), pool))
	t.Cleanup(after.Close)
	retry := postHTTP(t, after.URL, "fee-change", body)

	require.Equal(t, http.StatusCreated, retry.status, retry.body)
	require.Equal(t, "true", retry.replayed)
	require.Equal(t, first.body, retry.body)
}

func TestE2E_F3_ConcurrentPosts(t *testing.T) {
	srv := newE2EServer(t)
	const n = 10

	start := make(chan struct{})
	results := make([]response, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = postHTTP(t, srv.URL, "e2e-concurrent", validBody)
		}()
	}
	close(start)
	wg.Wait()

	fresh := 0
	id := workflowID(t, results[0].body)
	for _, r := range results {
		require.Equal(t, http.StatusCreated, r.status, r.body)
		require.Equal(t, id, workflowID(t, r.body))
		if r.replayed == "" {
			fresh++
		}
	}
	require.Equal(t, 1, fresh, "exactly one response must be the original")
}
