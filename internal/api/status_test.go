package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/api"
	"github.com/AdamDiouri/idem/internal/metrics"
	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/workflow"
)

const tokenB = "tok-bbbbbbbbbbbbbbbb"

// statusServer is the real API on a real database, for two clients.
func statusServer(t *testing.T) (http.Handler, *pgxpool.Pool, *metrics.Metrics) {
	t.Helper()
	pool := testutil.NewPool(t, dsn)
	m := metrics.New()
	opts := options()
	opts.Tokens[tokenB] = "client_b"
	opts.Metrics = m
	opts.Workflows = workflow.NewReader(pool)
	return api.NewServer(opts, workflow.NewSubmitter(pool, 5*time.Second, defaultFees), pool), pool, m
}

func get(t *testing.T, h http.Handler, path, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// created POSTs a workflow as client_a and returns its ID.
func created(t *testing.T, h http.Handler, key string) uuid.UUID {
	t.Helper()
	rec := post(t, h, validBody, map[string]string{"Idempotency-Key": key})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var c workflow.Created
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &c))
	return c.WorkflowID
}

func setStatus(t *testing.T, id uuid.UUID, status string) {
	t.Helper()
	_, err := testutil.NewOwnerPool(t, dsn).Exec(context.Background(), `UPDATE workflows SET status = $2 WHERE id = $1`, id, status)
	require.NoError(t, err)
}

// Slice 2.4 (spec §5.1).
func TestGetWorkflow_ReturnsView(t *testing.T) {
	h, _, _ := statusServer(t)
	id := created(t, h, "k-1")

	rec := get(t, h, "/workflows/"+id.String(), token)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var v map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v))
	require.Equal(t, id.String(), v["workflow_id"])
	require.Equal(t, "processing", v["status"])
	require.EqualValues(t, 10000, v["amount_cents"])
	require.EqualValues(t, 175, v["fee_cents"])
	require.Equal(t, "EUR", v["currency"])
}

// The shop reads exactly what its event will carry (D13: one definition).
func TestGetWorkflow_TerminalMatchesOutboxPayload(t *testing.T) {
	ctx := context.Background()
	h, pool, _ := statusServer(t)
	id := created(t, h, "k-1")
	setStatus(t, id, "FAILED")
	_, err := db.New(pool).InsertOutboxEvent(ctx, db.InsertOutboxEventParams{ID: uuid.Must(uuid.NewV7()), WorkflowID: id})
	require.NoError(t, err)
	events, err := db.New(pool).OutboxEventsForWorkflow(ctx, id)
	require.NoError(t, err)
	require.Len(t, events, 1)

	rec := get(t, h, "/workflows/"+id.String(), token)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, string(events[0].Payload), rec.Body.String())
	require.Contains(t, rec.Body.String(), `"status":"failed","reason":"charge_failed"`)
}

// Parking is internal: a parked workflow is still processing for the shop.
func TestGetWorkflow_NeedsAttentionIsProcessing(t *testing.T) {
	h, _, _ := statusServer(t)
	id := created(t, h, "k-1")
	setStatus(t, id, "NEEDS_ATTENTION")
	rec := get(t, h, "/workflows/"+id.String(), token)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"status":"processing"`)
	require.NotContains(t, strings.ToLower(rec.Body.String()), "attention")
}

// Review focus 3: another client's workflow is indistinguishable from none.
func TestGetWorkflow_OtherClientAndUnknownAreIdentical404(t *testing.T) {
	h, _, _ := statusServer(t)
	id := created(t, h, "k-1")

	other := get(t, h, "/workflows/"+id.String(), tokenB)
	unknown := get(t, h, "/workflows/"+uuid.Must(uuid.NewV7()).String(), tokenB)
	require.Equal(t, http.StatusNotFound, other.Code)
	require.Equal(t, http.StatusNotFound, unknown.Code)
	require.Equal(t, unknown.Body.String(), other.Body.String())
	require.Equal(t, "workflow_not_found", decodeError(t, other).Error.Code)
}

func TestGetWorkflow_MalformedID(t *testing.T) {
	h, _, _ := statusServer(t)
	for _, id := range []string{"nope", "1234", "01936f3e-8c2a-7b1e-9f00-00000000000"} {
		rec := get(t, h, "/workflows/"+id, token)
		require.Equal(t, http.StatusBadRequest, rec.Code, id)
		require.Equal(t, "invalid_workflow_id", decodeError(t, rec).Error.Code, id)
	}
}

func TestGetWorkflow_RequiresToken(t *testing.T) {
	h, _, _ := statusServer(t)
	id := created(t, h, "k-1")
	require.Equal(t, http.StatusUnauthorized, get(t, h, "/workflows/"+id.String(), "").Code)
	require.Equal(t, http.StatusUnauthorized, get(t, h, "/workflows/"+id.String(), "tok-wrong-wrong-wrong").Code)
}

func TestGetWorkflow_CountsResults(t *testing.T) {
	h, _, m := statusServer(t)
	id := created(t, h, "k-1")
	get(t, h, "/workflows/"+id.String(), token)
	get(t, h, "/workflows/"+id.String(), tokenB)
	get(t, h, "/workflows/nope", token)
	for _, result := range []string{"found", "not_found", "invalid_id"} {
		require.Equal(t, 1.0, promtestutil.ToFloat64(m.StatusRequests.WithLabelValues(result)), result)
	}
}

type slowReader struct{}

func (slowReader) View(ctx context.Context, _ string, _ uuid.UUID) ([]byte, bool, error) {
	<-ctx.Done()
	return nil, false, ctx.Err()
}

// Bounded by IDEM_REQUEST_TIMEOUT like every other request.
func TestGetWorkflow_RequestTimeout(t *testing.T) {
	opts := options()
	opts.RequestTimeout = 50 * time.Millisecond
	opts.Workflows = slowReader{}
	rec := get(t, api.NewServer(opts, &fakeSubmitter{}, fakePinger{}), "/workflows/"+uuid.Must(uuid.NewV7()).String(), token)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "request_timeout", decodeError(t, rec).Error.Code)
}
