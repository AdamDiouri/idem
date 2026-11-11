package engine_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
)

// outboxEvent ends a new workflow and sets its event's status and attempts.
func (h harness) outboxEventWith(t *testing.T, status string, attempts int32) (eventID, workflowID uuid.UUID) {
	t.Helper()
	wf := submitTo(t, h, "cus_1", "mer_1")
	setWorkflowStatus(t, h.pool, wf, "FAILED")
	require.NoError(t, h.pool.QueryRow(context.Background(), `UPDATE outbox SET status = $2, attempts = $3,
		last_error = CASE WHEN $2 = 'DEAD' THEN 'shop answered 503' END WHERE workflow_id = $1 RETURNING id`,
		wf, status, attempts).Scan(&eventID))
	return eventID, wf
}

func (h harness) redeliverAudits(t *testing.T, wf uuid.UUID) int {
	t.Helper()
	return h.count(t, `SELECT count(*) FROM admin_actions WHERE workflow_id = $1 AND action = 'redeliver_event'`, wf)
}

// Slice 2.5 (M2 design §5.5): what has not reached its shop.
func TestAdmin_ListOutbox(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	pending, _ := h.outboxEventWith(t, "PENDING", 1)
	dead, _ := h.outboxEventWith(t, "DEAD", 15)
	h.outboxEventWith(t, "DELIVERED", 1)

	all, err := h.admin().ListOutbox(ctx, false)
	require.NoError(t, err)
	require.Len(t, all, 2)
	require.Equal(t, pending, all[0].ID, "oldest first")
	require.Equal(t, dead, all[1].ID)
	require.Equal(t, "DEAD", all[1].Status)
	require.Equal(t, int32(15), all[1].Attempts)
	require.Equal(t, "shop answered 503", all[1].LastError)
	require.Equal(t, "workflow.failed", all[1].Type)

	onlyDead, err := h.admin().ListOutbox(ctx, true)
	require.NoError(t, err)
	require.Len(t, onlyDead, 1)
	require.Equal(t, dead, onlyDead[0].ID)
}

func TestAdmin_RedeliverDeadEvent(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	id, wf := h.outboxEventWith(t, "DEAD", 15)
	epoch := scalar[int64](t, h.pool, `SELECT lease_epoch FROM outbox WHERE id = $1`, id)

	require.NoError(t, h.admin().Redeliver(ctx, id, 15, "shop is back"))
	require.Equal(t, "PENDING", scalar[string](t, h.pool, `SELECT status FROM outbox WHERE id = $1`, id))
	require.Zero(t, scalar[int32](t, h.pool, `SELECT attempts FROM outbox WHERE id = $1`, id), "a fresh budget")
	require.True(t, scalar[bool](t, h.pool, `SELECT next_attempt_at <= now() AND last_error IS NULL AND lease_owner IS NULL FROM outbox WHERE id = $1`, id))
	require.Equal(t, epoch+1, scalar[int64](t, h.pool, `SELECT lease_epoch FROM outbox WHERE id = $1`, id))
	require.Equal(t, 1, h.redeliverAudits(t, wf))
}

// Review focus 5: the relay is working on it.
func TestAdmin_RedeliverRefusesPending(t *testing.T) {
	h := newHarness(t)
	id, wf := h.outboxEventWith(t, "PENDING", 3)
	require.ErrorIs(t, h.admin().Redeliver(context.Background(), id, 3, "x"), engine.ErrNotActionable)
	require.Zero(t, h.redeliverAudits(t, wf))
}

func TestAdmin_RedeliverRefusesStaleAttempts(t *testing.T) {
	h := newHarness(t)
	id, wf := h.outboxEventWith(t, "DEAD", 15)
	require.ErrorIs(t, h.admin().Redeliver(context.Background(), id, 14, "x"), engine.ErrStale)
	require.Zero(t, h.redeliverAudits(t, wf))
}

func TestAdmin_RedeliverUnknownEvent(t *testing.T) {
	h := newHarness(t)
	require.ErrorIs(t, h.admin().Redeliver(context.Background(), uuid.Must(uuid.NewV7()), 0, "x"), engine.ErrNotActionable)
}

// Review focus 5: two operators at once; one wins, one audit row.
func TestAdmin_RedeliverRace(t *testing.T) {
	h := newHarness(t)
	id, wf := h.outboxEventWith(t, "DEAD", 15)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = h.admin().Redeliver(context.Background(), id, 15, "shop is back")
		}()
	}
	wg.Wait()
	ok := 0
	for _, err := range errs {
		if err == nil {
			ok++
		}
	}
	require.Equal(t, 1, ok, "errors: %v", errs)
	require.Equal(t, 1, h.redeliverAudits(t, wf))
}

// D19 and the 2.5 backfill decision: both can be sent by hand.
func TestAdmin_RedeliverNoEndpointAndSkipped(t *testing.T) {
	h := newHarness(t)
	for _, status := range []string{"NO_ENDPOINT", "SKIPPED", "DELIVERED"} {
		id, _ := h.outboxEventWith(t, status, 0)
		require.NoError(t, h.admin().Redeliver(context.Background(), id, 0, "client asked"), status)
	}
}

func TestAdmin_RedeliverRequiresReason(t *testing.T) {
	h := newHarness(t)
	id, _ := h.outboxEventWith(t, "DEAD", 15)
	require.Error(t, h.admin().Redeliver(context.Background(), id, 15, "  "))
}
