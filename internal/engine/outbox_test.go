package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/webhook"
)

type outboxEvent struct {
	typ, status   string
	matchesView   bool
	payloadReason string
}

// outboxEvents lists wf's outbox rows, checking each payload against the live
// view (a terminal workflow's view never changes after its event).
func (h harness) outboxEvents(t *testing.T, wf uuid.UUID) []outboxEvent {
	t.Helper()
	rows, err := h.pool.Query(context.Background(), `SELECT type, status,
		convert_from(payload, 'UTF8') = workflow_view(workflow_id),
		COALESCE(convert_from(payload, 'UTF8')::json ->> 'reason', '')
		FROM outbox WHERE workflow_id = $1`, wf)
	require.NoError(t, err)
	defer rows.Close()
	var out []outboxEvent
	for rows.Next() {
		var e outboxEvent
		require.NoError(t, rows.Scan(&e.typ, &e.status, &e.matchesView, &e.payloadReason))
		out = append(out, e)
	}
	require.NoError(t, rows.Err())
	return out
}

// Slice 2.4 (D13, review focus 2): every path to a terminal state writes
// exactly one event, in the transition's own transaction.
func TestOutbox_EveryTerminalPathWritesOneEvent(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		settle     time.Duration // how long the fake provider keeps payouts pending
		run        func(t *testing.T, h harness) uuid.UUID
		wantType   string
		wantReason string
	}{
		{"charge declined", 0, func(t *testing.T, h harness) uuid.UUID {
			wf := submitTo(t, h, "cus_decline_1", "mer_1")
			h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
			return wf
		}, "workflow.failed", "card_declined"},
		{"payout paid by poll", time.Hour, func(t *testing.T, h harness) uuid.UUID {
			wf := submitTo(t, h, "cus_1", "mer_1")
			w := h.workerWith("w1", polling(100*time.Millisecond, 0), h.allSteps()...)
			h.drain(t, w)
			h.fake.SettleNow()
			h.runUntil(t, w, wf, "SUCCEEDED")
			return wf
		}, "workflow.succeeded", ""},
		{"payout paid by webhook", time.Hour, func(t *testing.T, h harness) uuid.UUID {
			wf := waitingPayout(t, h)
			h.fake.SettleNow()
			h.storeEvent(t, wf, webhook.EventPayoutPaid, "")
			require.Equal(t, "applied", h.inboxOnce(t, h.inboxProcessor()))
			return wf
		}, "workflow.succeeded", ""},
		{"payout refused, refunded", 0, func(t *testing.T, h harness) uuid.UUID {
			h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModePayoutFail, Times: 1})
			wf := submitTo(t, h, "cus_1", "mer_1")
			h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
			return wf
		}, "workflow.compensated", "payout_failed"},
		{"admin fail", 0, func(t *testing.T, h harness) uuid.UUID {
			wf := parkCharge(t, h, fakeprovider.ModeErrorBeforeEffect)
			require.Empty(t, h.outboxEvents(t, wf), "parking is not an outcome")
			require.NoError(t, h.admin().Fail(ctx, wf, h.version(t, wf), "provider confirms no charge", true))
			return wf
		}, "workflow.failed", "charge_failed"},
		{"admin confirm-step payout", 0, func(t *testing.T, h harness) uuid.UUID {
			h.setFaults(fakeprovider.Rule{Endpoint: "payouts", Mode: fakeprovider.ModeTimeoutAfterEffect})
			wf := submitTo(t, h, "cus_1", "mer_1")
			h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
			h.setStepMaxAttempts(t, wf, "payout", 1)
			h.drain(t, h.workerWith("w1", nil, h.allSteps()...))
			require.Equal(t, "NEEDS_ATTENTION", h.workflowStatus(t, wf))
			paid := h.effectsOf(wf, "payout")
			require.Len(t, paid, 1)
			step := h.parkedStep(t, wf).StepID
			require.NoError(t, h.admin().ConfirmStep(ctx, step, h.version(t, step), paid[0].ID, "dashboard shows it"))
			return wf
		}, "workflow.succeeded", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := asyncHarness(t, tc.settle)
			wf := tc.run(t, h)
			require.Equal(t, []outboxEvent{{tc.wantType, "PENDING", true, tc.wantReason}}, h.outboxEvents(t, wf))
			h.assertNoViolations(t)
		})
	}
}

// Transitions that are not an outcome announce nothing.
func TestOutbox_NonTerminalTransitionsWriteNothing(t *testing.T) {
	h := newHarness(t)
	compensating := submitTo(t, h, "cus_1", "mer_invalid_1") // payout refused → COMPENSATING
	h.drain(t, h.workerWith("w1", nil, h.chargeStep(), h.payoutStep()))
	require.Equal(t, "COMPENSATING", h.workflowStatus(t, compensating))
	require.Empty(t, h.outboxEvents(t, compensating))

	parked := parkPayoutAfterDoubt(t, h) // → NEEDS_ATTENTION
	require.Empty(t, h.outboxEvents(t, parked))
}

// Review focus 1: no event without its transition.
func TestOutbox_RolledBackTransitionLeavesNoEvent(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep())) // RUNNING, payout pending

	tx, err := h.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }() // a failed require must not leave the pool blocked
	require.NoError(t, engine.TransitionWorkflow(ctx, tx, wf, []string{"RUNNING"}, "SUCCEEDED"))
	require.Equal(t, 1, scalarTx(t, tx, wf), "the event is written with the transition")
	require.NoError(t, tx.Rollback(ctx))

	require.Equal(t, "RUNNING", h.workflowStatus(t, wf))
	require.Empty(t, h.outboxEvents(t, wf))
}

// A second terminal event for a workflow is a bug: it fails the transition
// rather than being swallowed.
func TestOutbox_SecondTerminalEventFails(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	wf := submitTo(t, h, "cus_1", "mer_1")
	h.drain(t, h.workerWith("w1", nil, h.chargeStep()))
	_, err := h.pool.Exec(ctx, `INSERT INTO outbox (id, client_id, workflow_id, type, payload)
		VALUES ($1, 'c', $2, 'workflow.succeeded', '\x7b7d')`, uuid.Must(uuid.NewV7()), wf)
	require.NoError(t, err)

	tx, err := h.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	err = engine.TransitionWorkflow(ctx, tx, wf, []string{"RUNNING"}, "SUCCEEDED")
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "got %v", err)
	require.Equal(t, "23505", pgErr.Code)
}

func scalarTx(t *testing.T, tx pgx.Tx, wf uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, tx.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE workflow_id = $1`, wf).Scan(&n))
	return n
}
