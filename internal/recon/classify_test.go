package recon_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/recon"
)

var (
	wf     = uuid.MustParse("01936f3e-8c2a-7b1e-9f00-000000000042")
	t0     = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	window = recon.Window{From: t0, To: t0.Add(time.Hour)}
	in     = t0.Add(30 * time.Minute)  // inside the window
	before = t0.Add(-30 * time.Minute) // before it, within the margin
	after  = t0.Add(90 * time.Minute)  // after it
)

func rec(typ string, amount int64, ref string, at time.Time) provider.BalanceTransaction {
	return provider.BalanceTransaction{ID: "txn_" + ref, Type: typ, Amount: amount, Currency: "EUR", Source: ref, WorkflowID: wf.String(), Created: at}
}

type stepSpec struct {
	name, status, ref string
	posted            *int64 // the posting's amount, if the step posted
	at                time.Time
}

func posted(v int64) *int64 { return &v }

func state(status string, steps ...stepSpec) *recon.Workflow {
	w := &recon.Workflow{ID: wf, Status: status}
	for _, s := range steps {
		id := uuid.New()
		w.Steps = append(w.Steps, recon.Step{ID: id, Name: s.name, Status: s.status, ProviderRef: s.ref})
		if s.posted != nil {
			w.Postings = append(w.Postings, recon.Posting{StepID: id, Kind: s.name, Amount: *s.posted, Currency: "EUR", At: s.at})
		}
	}
	return w
}

func kinds(ds []recon.Discrepancy) map[string]string {
	out := map[string]string{}
	for _, d := range ds {
		out[d.EffectType] = d.Kind
	}
	return out
}

// Slice 2.6, after the final review (C1): each provider movement in the window
// must have its posting, and each posting in the window its movement;
// history outside the window is never judged.
func TestClassify(t *testing.T) {
	charge := stepSpec{"charge", "SUCCEEDED", "ch_1", posted(10000), in}
	payout := stepSpec{"payout", "SUCCEEDED", "po_1", posted(-9825), in}
	cases := []struct {
		name     string
		w        *recon.Workflow
		records  []provider.BalanceTransaction
		want     map[string]string
		inFlight int
	}{
		{"clean succeeded", state("SUCCEEDED", charge, payout),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1", in), rec("payout", -9825, "po_1", in)}, map[string]string{}, 0},
		{"payout settles days after its charge: the old charge is not judged", state("SUCCEEDED",
			stepSpec{"charge", "SUCCEEDED", "ch_1", posted(10000), t0.Add(-72 * time.Hour)}, payout),
			[]provider.BalanceTransaction{rec("payout", -9825, "po_1", in)}, map[string]string{}, 0},
		{"posting in the window, its movement before it", state("SUCCEEDED", charge, stepSpec{"payout", "SUCCEEDED", "po_1", posted(-9825), in}),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1", before), rec("payout", -9825, "po_1", before)}, map[string]string{}, 0},
		{"movement in the window, posted after it", state("SUCCEEDED", stepSpec{"payout", "SUCCEEDED", "po_1", posted(-9825), after}),
			[]provider.BalanceTransaction{rec("payout", -9825, "po_1", in)}, map[string]string{}, 0},
		{"moved, step still waiting: in flight", state("RUNNING", charge, stepSpec{"payout", "AWAITING_CALLBACK", "po_1", nil, in}),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1", in), rec("payout", -9825, "po_1", in)}, map[string]string{}, 1},
		{"provider moved, never posted", state("NEEDS_ATTENTION", charge, stepSpec{"payout", "DEAD", "po_1", nil, in}),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1", in), rec("payout", -9825, "po_1", in)},
			map[string]string{"payout": "missing_in_ledger"}, 0},
		{"posted, no movement", state("SUCCEEDED", charge, payout),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1", in)}, map[string]string{"payout": "missing_at_provider"}, 0},
		{"amounts differ", state("SUCCEEDED", charge),
			[]provider.BalanceTransaction{rec("charge", 9000, "ch_1", in)}, map[string]string{"charge": "amount_mismatch"}, 0},
		{"currencies differ", state("SUCCEEDED", charge),
			[]provider.BalanceTransaction{{ID: "txn_ch_1", Type: "charge", Amount: 10000, Currency: "USD", Source: "ch_1", WorkflowID: wf.String(), Created: in}},
			map[string]string{"charge": "amount_mismatch"}, 0},
		{"two movements beat everything", state("SUCCEEDED", charge),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1", in), rec("charge", 10000, "ch_2", in)}, map[string]string{"charge": "duplicate_effect"}, 0},
		{"a second charge across the window edge", state("SUCCEEDED", stepSpec{"charge", "SUCCEEDED", "ch_1", posted(10000), before}),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1", before), rec("charge", 10000, "ch_2", in)}, map[string]string{"charge": "duplicate_effect"}, 0},
		{"moved although recorded refused", state("FAILED", stepSpec{"charge", "FAILED_PERMANENT", "", nil, in}),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_9", in)}, map[string]string{"charge": "status_mismatch"}, 0},
		{"a charge made outside idem for a workflow with a charge in flight", state("RUNNING", stepSpec{"charge", "RUNNING", "", nil, in}),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1", in)}, map[string]string{}, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ds, inFlight := recon.Classify(c.w, c.records, window)
			require.Equal(t, c.want, kinds(ds))
			require.Equal(t, c.inFlight, inFlight)
		})
	}
}

func TestClassify_ParkedIsFlaggedAndKeyed(t *testing.T) {
	ds, _ := recon.Classify(state("NEEDS_ATTENTION", stepSpec{"charge", "SUCCEEDED", "ch_1", posted(10000), in}, stepSpec{"payout", "DEAD", "po_1", nil, in}),
		[]provider.BalanceTransaction{rec("charge", 10000, "ch_1", in), rec("payout", -9825, "po_1", in)}, window)
	require.Len(t, ds, 1)
	d := ds[0]
	require.True(t, d.Parked)
	require.Equal(t, wf, *d.WorkflowID)
	require.Equal(t, []string{"po_1"}, d.ProviderRefs)
	require.Equal(t, int64(-9825), *d.ProviderAmount)
	require.Nil(t, d.LedgerAmount)
	require.Equal(t, wf.String()+"|payout", d.ItemKey)
	require.Equal(t, in, d.ItemAt)
}

// The provider names a workflow idem does not know.
func TestClassify_UnknownWorkflow(t *testing.T) {
	ds, _ := recon.Classify(nil, []provider.BalanceTransaction{rec("charge", 10000, "ch_x", in)}, window)
	require.Len(t, ds, 1)
	require.Equal(t, "missing_in_ledger", ds[0].Kind)
	require.Equal(t, wf, *ds[0].WorkflowID)
	require.Equal(t, []string{"ch_x"}, ds[0].ProviderRefs)

	none, _ := recon.Classify(nil, []provider.BalanceTransaction{rec("charge", 10000, "ch_old", before)}, window)
	require.Empty(t, none, "a movement outside the window is not judged")
}
