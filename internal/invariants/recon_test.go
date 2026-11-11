package invariants_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/invariants"
	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/testutil"
)

// reconRunWith records a successful reconciliation run with the given
// discrepancies (workflow, type, parked).
func reconRunWith(t *testing.T, e env, ds ...struct {
	wf     uuid.UUID
	typ    string
	parked bool
}) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	run := uuid.Must(uuid.NewV7())
	_, err := e.pool.Exec(ctx, `INSERT INTO recon_runs (id, window_from, window_to) VALUES ($1, now() - interval '1 hour', now())`, run)
	require.NoError(t, err)
	_, err = e.pool.Exec(ctx, `UPDATE recon_runs SET status = 'SUCCEEDED', finished_at = now() WHERE id = $1`, run)
	require.NoError(t, err)
	for _, d := range ds {
		_, err := e.pool.Exec(ctx, `INSERT INTO recon_discrepancies (id, run_id, workflow_id, kind, effect_type, parked, detail)
			VALUES (gen_random_uuid(), $1, $2, 'missing_in_ledger', $3, $4, 'x')`, run, d.wf, d.typ, d.parked)
		require.NoError(t, err)
	}
	return run
}

type disc = struct {
	wf     uuid.UUID
	typ    string
	parked bool
}

func recon(t *testing.T, e env, run uuid.UUID) []invariants.Violation {
	t.Helper()
	v, err := invariants.CheckReconciliation(context.Background(), e.pool, run, e.fake.Effects())
	require.NoError(t, err)
	return v
}

// parkedWithCharge parks a workflow whose charge moved and is posted; its
// payout is reported moved by the oracle when payout is true.
func parkedWithCharge(t *testing.T, e env, payout bool) uuid.UUID {
	t.Helper()
	wf, step := newWorkflow(t, e.pool)
	e.charge(t, wf, "key-"+wf.String())
	e.post(t, wf, step)
	testutil.SetWorkflowStatus(t, e.pool, wf, "NEEDS_ATTENTION")
	if payout {
		out := e.client.CreatePayout(context.Background(), "po-"+wf.String(),
			provider.PayoutRequest{AmountCents: 9825, Currency: "EUR", MerchantID: "mer_1", WorkflowID: wf})
		require.Equal(t, provider.Accepted, out.Class)
		e.fake.SettleNow()
	}
	return wf
}

// Slice 2.6, the RECON invariant.
func TestRECON_Clean(t *testing.T) {
	e := setup(t)
	parkedWithCharge(t, e, false) // parked, but the ledger matches the provider
	require.Empty(t, recon(t, e, reconRunWith(t, e)))
}

func TestRECON_NonParkedDiscrepancy(t *testing.T) {
	e := setup(t)
	wf, _ := newWorkflow(t, e.pool)
	v := recon(t, e, reconRunWith(t, e, disc{wf, "charge", false}))
	require.Len(t, v, 1)
	require.Equal(t, "RECON", v[0].Invariant)
}

// A parked workflow's differences must be reported exactly: none missing,
// none extra.
func TestRECON_ParkedMustBeReportedExactly(t *testing.T) {
	e := setup(t)
	wf := parkedWithCharge(t, e, true) // the payout moved, the ledger has none

	require.Empty(t, recon(t, e, reconRunWith(t, e, disc{wf, "payout", true})))

	missing := recon(t, e, reconRunWith(t, e))
	require.Len(t, missing, 1)
	require.Contains(t, missing[0].Detail, "not reported")

	extra := recon(t, e, reconRunWith(t, e, disc{wf, "payout", true}, disc{wf, "charge", true}))
	require.Len(t, extra, 1)
	require.Contains(t, extra[0].Detail, "charge")
}
