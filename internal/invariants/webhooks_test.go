package invariants_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/invariants"
)

// inboxRow stores one inbox row, optionally tied to a step.
func (e env) inboxRow(t *testing.T, id, status, note string, step *uuid.UUID) {
	t.Helper()
	_, err := e.pool.Exec(context.Background(), `INSERT INTO webhook_inbox (provider_event_id, type, payload, status, note, step_id)
		VALUES ($1, 'payout.paid', '{}', $2, NULLIF($3, ''), $4)`, id, status, note, step)
	require.NoError(t, err)
}

// webhookAttempt records that the inbox applied an event to step, as the
// apply path does (an attempt of kind webhook).
func (e env) webhookAttempt(t *testing.T, step uuid.UUID) {
	t.Helper()
	_, err := e.pool.Exec(context.Background(), `INSERT INTO step_attempts (step_id, attempt_no, lease_epoch, worker_id, kind, outcome, finished_at)
		VALUES ($1, 99, 1, 'inbox-1', 'webhook', 'SUCCEEDED', now())`, step)
	require.NoError(t, err)
}

func (e env) checkWebhooks(t *testing.T, ds []fakeprovider.WebhookDelivery) []invariants.Violation {
	t.Helper()
	v, err := invariants.CheckWebhooks(context.Background(), e.pool, ds, time.Time{})
	require.NoError(t, err)
	return v
}

// Slice 2.3b: the inbox checked against the provider's delivery log. Polling
// finishes every payout anyway, so without this a broken webhook path passes.
func TestCheckWebhooks(t *testing.T) {
	delivered := func(id string) fakeprovider.WebhookDelivery {
		return fakeprovider.WebhookDelivery{EventID: id, Delivered: true, Attempts: 1}
	}

	t.Run("healthy inbox", func(t *testing.T) {
		e := setup(t)
		wf, step := newWorkflow(t, e.pool)
		e.setStatus(t, wf, "NEEDS_ATTENTION")
		e.inboxRow(t, "evt_applied", "PROCESSED", "applied", &step)
		e.webhookAttempt(t, step)
		e.inboxRow(t, "evt_dup", "PROCESSED", "duplicate", &step)
		e.inboxRow(t, "evt_stale", "IGNORED", "stale", &step)
		// A stale event that reached a step still waiting is applied with the
		// provider's re-checked state, never its own claim (I3/I7 judge the result).
		e.inboxRow(t, "evt_stale_waiting", "PROCESSED", "duplicate", &step)
		e.inboxRow(t, "evt_evidence", "EVIDENCE", "provider says paid po_1", &step)
		stale := delivered("evt_stale")
		stale.Stale = true
		staleWaiting := delivered("evt_stale_waiting")
		staleWaiting.Stale = true
		bad := delivered("evt_dup")
		bad.Fault, bad.Attempts = fakeprovider.ModeBadSignature, 2
		ds := []fakeprovider.WebhookDelivery{delivered("evt_applied"), bad, stale, staleWaiting, delivered("evt_evidence"),
			{EventID: "evt_dropped", GaveUp: true, Fault: fakeprovider.ModeDropWebhook}}
		require.Empty(t, e.checkWebhooks(t, ds))
	})

	cases := []struct {
		name      string
		rows      func(e env, step uuid.UUID)
		ds        []fakeprovider.WebhookDelivery
		invariant string
		detail    string
	}{
		{
			name:      "acknowledged event missing from the inbox",
			rows:      func(env, uuid.UUID) {},
			ds:        []fakeprovider.WebhookDelivery{delivered("evt_lost")},
			invariant: "I11", detail: "evt_lost was acknowledged but is not in the inbox",
		},
		{
			name: "wrongly signed first attempt accepted",
			rows: func(e env, step uuid.UUID) { e.inboxRow(t, "evt_forged", "PROCESSED", "duplicate", &step) },
			ds: []fakeprovider.WebhookDelivery{{EventID: "evt_forged", Delivered: true, Attempts: 1,
				Fault: fakeprovider.ModeBadSignature}},
			invariant: "I11", detail: "wrongly signed",
		},
		{
			name:      "event the provider never sent",
			rows:      func(e env, step uuid.UUID) { e.inboxRow(t, "evt_unknown", "PROCESSED", "applied", &step) },
			ds:        nil,
			invariant: "I11", detail: "evt_unknown",
		},
		{
			name:      "marked applied, but the step records no webhook attempt",
			rows:      func(e env, step uuid.UUID) { e.inboxRow(t, "evt_fake", "PROCESSED", "applied", &step) },
			ds:        []fakeprovider.WebhookDelivery{delivered("evt_fake")},
			invariant: "I11", detail: "evt_fake was marked applied",
		},
		{
			name:      "orphan",
			rows:      func(e env, _ uuid.UUID) { e.inboxRow(t, "evt_o", "IGNORED", "orphan", nil) },
			ds:        []fakeprovider.WebhookDelivery{delivered("evt_o")},
			invariant: "I10", detail: "orphan",
		},
		{
			name:      "a true event dismissed as stale",
			rows:      func(e env, step uuid.UUID) { e.inboxRow(t, "evt_t", "IGNORED", "stale", &step) },
			ds:        []fakeprovider.WebhookDelivery{delivered("evt_t")},
			invariant: "I10", detail: "evt_t",
		},
		{
			name:      "a stale event ignored for another reason",
			rows:      func(e env, step uuid.UUID) { e.inboxRow(t, "evt_s", "IGNORED", "orphan", &step) },
			ds:        []fakeprovider.WebhookDelivery{{EventID: "evt_s", Delivered: true, Attempts: 1, Stale: true}},
			invariant: "I10", detail: "evt_s",
		},
		{
			name:      "evidence on a workflow that is not parked",
			rows:      func(e env, step uuid.UUID) { e.inboxRow(t, "evt_ev", "EVIDENCE", "provider says paid", &step) },
			ds:        []fakeprovider.WebhookDelivery{delivered("evt_ev")},
			invariant: "I10", detail: "evt_ev",
		},
		{
			name:      "mismatch",
			rows:      func(e env, step uuid.UUID) { e.inboxRow(t, "evt_m", "IGNORED", "amount 1 != 2", &step) },
			ds:        []fakeprovider.WebhookDelivery{delivered("evt_m")},
			invariant: "I10", detail: "amount 1 != 2",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := setup(t)
			wf, step := newWorkflow(t, e.pool)
			e.setStatus(t, wf, "SUCCEEDED")
			c.rows(e, step)
			v := e.checkWebhooks(t, c.ds)
			require.Len(t, v, 1, "violations: %v", v)
			require.Equal(t, c.invariant, v[0].Invariant)
			require.Contains(t, v[0].Detail, c.detail)
		})
	}
}

// Final review, minor 6: `idem check` runs after normal operations too.
// Evidence about a workflow an operator then resolved, and events old enough
// to have been pruned by inbox retention, are not violations.
func TestCheckWebhooks_AfterNormalOperations(t *testing.T) {
	ctx := context.Background()
	e := setup(t)
	wf, step := newWorkflow(t, e.pool)
	e.setStatus(t, wf, "SUCCEEDED") // an operator confirmed the parked payout
	_, err := e.pool.Exec(ctx, `INSERT INTO admin_actions (id, actor, action, workflow_id, step_id, reason, details)
		VALUES (gen_random_uuid(), 'ops', 'confirm_step', $1, $2, 'provider shows it paid', '{}')`, wf, step)
	require.NoError(t, err)
	e.inboxRow(t, "evt_evidence", "EVIDENCE", "provider says paid", &step)

	old := time.Now().Add(-48 * time.Hour)
	pruned := fakeprovider.WebhookDelivery{EventID: "evt_pruned", Delivered: true, Attempts: 1, Created: old.Unix()}
	ds := []fakeprovider.WebhookDelivery{{EventID: "evt_evidence", Delivered: true, Attempts: 1, Created: time.Now().Unix()}, pruned}

	v, err := invariants.CheckWebhooks(ctx, e.pool, ds, time.Now().Add(-24*time.Hour))
	require.NoError(t, err)
	require.Empty(t, v)

	// Without a retention cutoff (chaos), a missing event is still a violation.
	v, err = invariants.CheckWebhooks(ctx, e.pool, ds, time.Time{})
	require.NoError(t, err)
	require.Len(t, v, 1)
	require.Contains(t, v[0].Detail, "evt_pruned")
}
