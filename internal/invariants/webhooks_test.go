package invariants_test

import (
	"context"
	"testing"

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

func (e env) checkWebhooks(t *testing.T, ds []fakeprovider.WebhookDelivery) []invariants.Violation {
	t.Helper()
	v, err := invariants.CheckWebhooks(context.Background(), e.pool, ds)
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
		e.inboxRow(t, "evt_dup", "PROCESSED", "duplicate", &step)
		e.inboxRow(t, "evt_stale", "IGNORED", "stale", &step)
		e.inboxRow(t, "evt_evidence", "EVIDENCE", "provider says paid po_1", &step)
		stale := delivered("evt_stale")
		stale.Stale = true
		bad := delivered("evt_dup")
		bad.Fault, bad.Attempts = fakeprovider.ModeBadSignature, 2
		ds := []fakeprovider.WebhookDelivery{delivered("evt_applied"), bad, stale, delivered("evt_evidence"),
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
			name: "acknowledged event missing from the inbox",
			rows: func(env, uuid.UUID) {},
			ds:   []fakeprovider.WebhookDelivery{delivered("evt_lost")},
			invariant: "I11", detail: "evt_lost was acknowledged but is not in the inbox",
		},
		{
			name: "wrongly signed first attempt accepted",
			rows: func(e env, step uuid.UUID) { e.inboxRow(t, "evt_forged", "PROCESSED", "applied", &step) },
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
			name: "a stale event applied",
			rows: func(e env, step uuid.UUID) { e.inboxRow(t, "evt_s", "PROCESSED", "applied", &step) },
			ds: []fakeprovider.WebhookDelivery{{EventID: "evt_s", Delivered: true, Attempts: 1, Stale: true}},
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
