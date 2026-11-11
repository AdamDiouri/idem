package workflow_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/testutil"
	"github.com/AdamDiouri/idem/internal/workflow"
)

// Slice 2.4 (spec §5.1): a client reads only its own workflows; anything else
// looks exactly like a workflow that does not exist.
func TestReader_ViewIsScopedToClient(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewPool(t, dsn)
	s := workflow.NewSubmitter(pool, 5*time.Second, workflow.FeeConfig{RateBps: 150, FixedCents: 25})
	res, err := s.Submit(ctx, workflow.SubmitRequest{ClientID: "client_a", IdempotencyKey: "k-view",
		Request: workflow.CreateWorkflowRequest{AmountCents: 10000, Currency: "EUR", CustomerID: "cus_1", MerchantID: "mer_1"}})
	require.NoError(t, err)
	var created workflow.Created
	require.NoError(t, json.Unmarshal(res.Body, &created))
	id := created.WorkflowID

	r := workflow.NewReader(pool)
	view, found, err := r.View(ctx, "client_a", id)
	require.NoError(t, err)
	require.True(t, found)
	var want string
	require.NoError(t, pool.QueryRow(ctx, `SELECT workflow_view($1)`, id).Scan(&want))
	require.Equal(t, want, string(view))

	view, found, err = r.View(ctx, "client_b", id)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, view)

	_, found, err = r.View(ctx, "client_a", uuid.Must(uuid.NewV7()))
	require.NoError(t, err)
	require.False(t, found)
}
