package recon_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/provider"
	"github.com/AdamDiouri/idem/internal/recon"
)

var wf = uuid.MustParse("01936f3e-8c2a-7b1e-9f00-000000000042")

func rec(typ string, amount int64, ref string) provider.BalanceTransaction {
	return provider.BalanceTransaction{ID: "txn_" + ref, Type: typ, Amount: amount, Source: ref, WorkflowID: wf.String()}
}

func wfState(status string, postings map[string][]int64, steps map[string]string) *recon.Workflow {
	return &recon.Workflow{ID: wf, Status: status, Postings: postings, StepStatus: steps}
}

func kinds(ds []recon.Discrepancy) map[string]string {
	out := map[string]string{}
	for _, d := range ds {
		out[d.EffectType] = d.Kind
	}
	return out
}

// Slice 2.6 (plan, decision 2): one kind per (workflow, type), by precedence.
func TestClassify(t *testing.T) {
	cases := []struct {
		name    string
		w       *recon.Workflow
		records []provider.BalanceTransaction
		want    map[string]string
	}{
		{"clean succeeded", wfState("SUCCEEDED", map[string][]int64{"charge": {10000}, "payout": {-9825}}, nil),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1"), rec("payout", -9825, "po_1")}, map[string]string{}},
		{"clean compensated", wfState("COMPENSATED", map[string][]int64{"charge": {10000}, "refund": {-10000}}, nil),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1"), rec("refund", -10000, "re_1")}, map[string]string{}},
		{"clean failed", wfState("FAILED", nil, map[string]string{"charge": "FAILED_PERMANENT"}), nil, map[string]string{}},
		{"provider moved, ledger did not", wfState("SUCCEEDED", map[string][]int64{"charge": {10000}}, nil),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1"), rec("payout", -9825, "po_1")},
			map[string]string{"payout": "missing_in_ledger"}},
		{"posted, provider did not", wfState("SUCCEEDED", map[string][]int64{"charge": {10000}, "payout": {-9825}}, nil),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1")}, map[string]string{"payout": "missing_at_provider"}},
		{"amounts differ", wfState("SUCCEEDED", map[string][]int64{"charge": {10000}}, nil),
			[]provider.BalanceTransaction{rec("charge", 9000, "ch_1")}, map[string]string{"charge": "amount_mismatch"}},
		{"two provider records beat amounts", wfState("SUCCEEDED", map[string][]int64{"payout": {-9825}}, nil),
			[]provider.BalanceTransaction{rec("payout", -9825, "po_1"), rec("payout", -1, "po_2")}, map[string]string{"payout": "duplicate_effect"}},
		{"two postings", wfState("SUCCEEDED", map[string][]int64{"charge": {10000, 10000}}, nil),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1")}, map[string]string{"charge": "duplicate_effect"}},
		{"moved although recorded refused", wfState("COMPENSATED", map[string][]int64{"charge": {10000}, "refund": {-10000}},
			map[string]string{"payout": "FAILED_PERMANENT"}),
			[]provider.BalanceTransaction{rec("charge", 10000, "ch_1"), rec("payout", -9825, "po_1"), rec("refund", -10000, "re_1")},
			map[string]string{"payout": "status_mismatch"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, kinds(recon.Classify(c.w, c.records)))
		})
	}
}

func TestClassify_ParkedIsFlaggedAndCarriesAmounts(t *testing.T) {
	ds := recon.Classify(wfState("NEEDS_ATTENTION", map[string][]int64{"charge": {10000}}, nil),
		[]provider.BalanceTransaction{rec("charge", 10000, "ch_1"), rec("payout", -9825, "po_1")})
	require.Len(t, ds, 1)
	d := ds[0]
	require.True(t, d.Parked)
	require.Equal(t, wf, *d.WorkflowID)
	require.Equal(t, []string{"po_1"}, d.ProviderRefs)
	require.Equal(t, int64(-9825), *d.ProviderAmount)
	require.Nil(t, d.LedgerAmount)
	require.NotEmpty(t, d.Detail)
}

// The provider names a workflow idem does not know: money moved that idem
// never asked for.
func TestClassify_UnknownWorkflow(t *testing.T) {
	ds := recon.Classify(nil, []provider.BalanceTransaction{rec("charge", 10000, "ch_x")})
	require.Len(t, ds, 1)
	require.Equal(t, "missing_in_ledger", ds[0].Kind)
	require.False(t, ds[0].Parked)
	require.Equal(t, wf, *ds[0].WorkflowID, "the provider's workflow ID is kept, though idem has no such workflow")
}
