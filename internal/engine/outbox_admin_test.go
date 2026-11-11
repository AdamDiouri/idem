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
	return h.outboxEventFor(t, "c", status, attempts)
}

// outboxEventFor is outboxEventWith for a given client.
func (h harness) outboxEventFor(t *testing.T, client, status string, attempts int32) (eventID, workflowID uuid.UUID) {
	t.Helper()
	wf := submitTo(t, h, "cus_1", "mer_1")
	_, err := h.pool.Exec(context.Background(), `UPDATE workflows SET client_id = $2 WHERE id = $1`, wf, client)
	require.NoError(t, err)
	setWorkflowStatus(t, h.pool, wf, "FAILED")
	require.NoError(t, h.pool.QueryRow(context.Background(), `UPDATE outbox SET status = $2, attempts = $3,
		last_error = CASE WHEN $2 = 'DEAD' THEN 'shop answered 503' END WHERE workflow_id = $1 RETURNING id`,
		wf, status, attempts).Scan(&eventID))
	return eventID, wf
}

// version is the VERSION ListOutbox shows: the event's lease epoch.
func (h harness) eventVersion(t *testing.T, id uuid.UUID) int64 {
	t.Helper()
	return scalar[int64](t, h.pool, `SELECT lease_epoch FROM outbox WHERE id = $1`, id)
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

	all, err := h.admin().ListOutbox(ctx, engine.OutboxFilter{})
	require.NoError(t, err)
	require.Len(t, all, 2)
	require.Equal(t, pending, all[0].ID, "oldest first")
	require.Equal(t, dead, all[1].ID)
	require.Equal(t, "DEAD", all[1].Status)
	require.Equal(t, int32(15), all[1].Attempts)
	require.Equal(t, "shop answered 503", all[1].LastError)
	require.Equal(t, "workflow.failed", all[1].Type)

	onlyDead, err := h.admin().ListOutbox(ctx, engine.OutboxFilter{OnlyDead: true})
	require.NoError(t, err)
	require.Len(t, onlyDead, 1)
	require.Equal(t, dead, onlyDead[0].ID)

	// Final review, minor 2: a delivered event can be listed too (to resend it).
	everything, err := h.admin().ListOutbox(ctx, engine.OutboxFilter{All: true})
	require.NoError(t, err)
	require.Len(t, everything, 3)
}

func TestAdmin_RedeliverDeadEvent(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	id, wf := h.outboxEventWith(t, "DEAD", 15)
	epoch := scalar[int64](t, h.pool, `SELECT lease_epoch FROM outbox WHERE id = $1`, id)

	require.NoError(t, h.admin().Redeliver(ctx, id, epoch, "shop is back"))
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
	require.ErrorIs(t, h.admin().Redeliver(context.Background(), id, h.eventVersion(t, id), "x"), engine.ErrNotActionable)
	require.Zero(t, h.redeliverAudits(t, wf))
}

func TestAdmin_RedeliverRefusesStaleVersion(t *testing.T) {
	h := newHarness(t)
	id, wf := h.outboxEventWith(t, "DEAD", 15)
	require.ErrorIs(t, h.admin().Redeliver(context.Background(), id, h.eventVersion(t, id)-1, "x"), engine.ErrStale)
	require.Zero(t, h.redeliverAudits(t, wf))
}

// Final review, minor 1: attempts go back to the same number after a
// redelivery (reset to 0, then 1 on the next claim), so they cannot tell a
// second operator that the event moved on; the version only grows.
func TestAdmin_SecondRedeliverOnOldViewIsStale(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	id, wf := h.outboxEventWith(t, "NO_ENDPOINT", 1)
	seen := h.eventVersion(t, id) // both operators list the event
	require.NoError(t, h.admin().Redeliver(ctx, id, seen, "client got a URL"))
	_, err := h.pool.Exec(ctx, `UPDATE outbox SET status = 'DELIVERED', attempts = 1, lease_epoch = lease_epoch + 1 WHERE id = $1`, id) // the relay delivers it
	require.NoError(t, err)
	require.ErrorIs(t, h.admin().Redeliver(ctx, id, seen, "client got a URL"), engine.ErrStale)
	require.Equal(t, 1, h.redeliverAudits(t, wf))
}

func TestAdmin_RedeliverUnknownEvent(t *testing.T) {
	h := newHarness(t)
	require.ErrorIs(t, h.admin().Redeliver(context.Background(), uuid.Must(uuid.NewV7()), 0, "x"), engine.ErrNotActionable)
}

// Review focus 5: two operators at once; one wins, one audit row.
func TestAdmin_RedeliverRace(t *testing.T) {
	h := newHarness(t)
	id, wf := h.outboxEventWith(t, "DEAD", 15)
	version := h.eventVersion(t, id)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = h.admin().Redeliver(context.Background(), id, version, "shop is back")
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
		require.NoError(t, h.admin().Redeliver(context.Background(), id, h.eventVersion(t, id), "client asked"), status)
	}
}

func TestAdmin_RedeliverRequiresReason(t *testing.T) {
	h := newHarness(t)
	id, _ := h.outboxEventWith(t, "DEAD", 15)
	require.Error(t, h.admin().Redeliver(context.Background(), id, h.eventVersion(t, id), "  "))
}

// Final review, Important 2: after an outage (DEAD) or a client's URL fixed
// (NO_ENDPOINT), resend all of one client's events at once, each audited.
func TestAdmin_RedeliverAllForClient(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	_, wf1 := h.outboxEventFor(t, "acme", "DEAD", 15)
	_, wf2 := h.outboxEventFor(t, "acme", "DEAD", 15)
	pending, _ := h.outboxEventFor(t, "acme", "PENDING", 2)
	other, _ := h.outboxEventFor(t, "zeta", "DEAD", 15)

	n, err := h.admin().RedeliverAll(ctx, "acme", "DEAD", "acme back after outage")
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, 1, h.redeliverAudits(t, wf1))
	require.Equal(t, 1, h.redeliverAudits(t, wf2))
	require.Equal(t, 0, h.count(t, `SELECT count(*) FROM outbox WHERE client_id = 'acme' AND status = 'DEAD'`))
	require.Equal(t, int32(2), scalar[int32](t, h.pool, `SELECT attempts FROM outbox WHERE id = $1`, pending), "untouched")
	require.Equal(t, "DEAD", scalar[string](t, h.pool, `SELECT status FROM outbox WHERE id = $1`, other), "another client's")

	for _, status := range []string{"PENDING", "DELIVERED", "bogus"} {
		_, err := h.admin().RedeliverAll(ctx, "acme", status, "x")
		require.Error(t, err, status)
	}
	_, err = h.admin().RedeliverAll(ctx, "acme", "DEAD", " ")
	require.Error(t, err)
}
