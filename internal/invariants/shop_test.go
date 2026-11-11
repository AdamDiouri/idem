package invariants_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeshop"
	"github.com/AdamDiouri/idem/internal/invariants"
)

// seenAs is what an honest shop records for wf's event.
func seenAs(t *testing.T, e env, wf uuid.UUID) fakeshop.SeenEvent {
	t.Helper()
	var ev fakeshop.SeenEvent
	require.NoError(t, e.pool.QueryRow(context.Background(), `SELECT o.id::text, o.workflow_id::text, o.type,
		workflow_view(o.workflow_id)::json ->> 'status' FROM outbox o WHERE o.workflow_id = $1`, wf).
		Scan(&ev.EventID, &ev.WorkflowID, &ev.Type, &ev.Status))
	ev.Deliveries = 1
	return ev
}

func checkShop(t *testing.T, e env, seen fakeshop.Seen, clients ...string) []invariants.Violation {
	t.Helper()
	v, err := invariants.CheckShop(context.Background(), e.pool, seen, clients)
	require.NoError(t, err)
	return v
}

// Slice 2.5, I9: the shop's deduplicated view equals idem's outcomes.
func TestI9_Clean(t *testing.T) {
	e := setup(t)
	a, b := finished(t, e, "SUCCEEDED"), finished(t, e, "COMPENSATED")
	newWorkflow(t, e.pool) // not finished: nothing to announce
	seen := fakeshop.Seen{Events: []fakeshop.SeenEvent{seenAs(t, e, a), seenAs(t, e, b)}, Duplicates: 3}
	require.Empty(t, checkShop(t, e, seen, "c"))
}

func TestI9_ShopMissesOutcome(t *testing.T) {
	e := setup(t)
	wf := finished(t, e, "FAILED")
	v := checkShop(t, e, fakeshop.Seen{}, "c")
	require.Len(t, v, 1)
	require.Equal(t, "I9", v[0].Invariant)
	require.Equal(t, wf.String(), v[0].WorkflowID)
	require.Contains(t, v[0].Detail, "never learned")
}

func TestI9_ShopSawUnknownEvent(t *testing.T) {
	e := setup(t)
	wf := finished(t, e, "SUCCEEDED")
	forged := seenAs(t, e, wf)
	forged.EventID = uuid.NewString()
	v := checkShop(t, e, fakeshop.Seen{Events: []fakeshop.SeenEvent{seenAs(t, e, wf), forged}}, "c")
	require.Len(t, v, 1)
	require.Contains(t, v[0].Detail, "idem never wrote")
}

func TestI9_StatusContradiction(t *testing.T) {
	e := setup(t)
	wf := finished(t, e, "SUCCEEDED")
	ev := seenAs(t, e, wf)
	ev.Status = "failed"
	v := checkShop(t, e, fakeshop.Seen{Events: []fakeshop.SeenEvent{ev}}, "c")
	require.Len(t, v, 1)
	require.Contains(t, v[0].Detail, "failed")
}

func TestI9_BadSignaturesCounted(t *testing.T) {
	e := setup(t)
	v := checkShop(t, e, fakeshop.Seen{BadSignatures: 2}, "c")
	require.Len(t, v, 1)
	require.Contains(t, v[0].Detail, "signature")
}

// Only the shop's own client's workflows are its business.
func TestI9_OtherClientsIgnored(t *testing.T) {
	e := setup(t)
	finished(t, e, "SUCCEEDED") // client "c"
	require.Empty(t, checkShop(t, e, fakeshop.Seen{}, "someone_else"))
}

// I4 (M2 design §9): after quiescence no outcome is still waiting to be sent.
func TestI4_PendingOutboxEvent(t *testing.T) {
	e := setup(t)
	wf := finished(t, e, "SUCCEEDED")
	v, err := invariants.CheckLiveness(context.Background(), e.pool)
	require.NoError(t, err)
	var outbox []invariants.Violation
	for _, x := range v {
		if x.WorkflowID == wf.String() {
			outbox = append(outbox, x)
		}
	}
	require.Len(t, outbox, 1)
	require.Equal(t, "I4", outbox[0].Invariant)
	require.Contains(t, outbox[0].Detail, "PENDING")
	_, err = e.pool.Exec(context.Background(), `UPDATE outbox SET status = 'DELIVERED' WHERE workflow_id = $1`, wf)
	require.NoError(t, err)
	v, err = invariants.CheckLiveness(context.Background(), e.pool)
	require.NoError(t, err)
	require.Empty(t, v)
}
