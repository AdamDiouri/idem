package invariants

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/store/db"
)

// CheckReconciliation is the RECON invariant (slice 2.6, M2 design §9):
// judged against the oracle, the run of `idem reconcile` reported no
// difference on a workflow that is not parked, and, for each parked
// workflow, exactly the effect types on which the provider's movements and
// the ledger's postings differ: none missing, none extra. It proves I3
// through the production path (the provider's report), not only the oracle.
func CheckReconciliation(ctx context.Context, pool *pgxpool.Pool, runID uuid.UUID, effects []fakeprovider.Effect) ([]Violation, error) {
	q := db.New(pool)
	rows, err := q.ReconDiscrepanciesOfRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("query discrepancies: %w", err)
	}
	var out []Violation
	add := func(wf, format string, args ...any) {
		out = append(out, Violation{Invariant: "RECON", WorkflowID: wf, Detail: fmt.Sprintf(format, args...)})
	}
	reported := map[string]map[string]bool{} // parked workflow → effect types reported
	for _, r := range rows {
		wf := ""
		if r.WorkflowID != nil {
			wf = r.WorkflowID.String()
		}
		if !r.Parked {
			add(wf, "reconciliation found %s on %s (not parked): %s", r.Kind, r.EffectType, r.Detail)
			continue
		}
		if reported[wf] == nil {
			reported[wf] = map[string]bool{}
		}
		reported[wf][r.EffectType] = true
	}

	parked, err := NeedsAttention(ctx, pool)
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, len(parked))
	for _, p := range parked {
		if id, err := uuid.Parse(p); err == nil {
			ids = append(ids, id)
		}
	}
	postings, err := q.ReconPostings(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("query postings: %w", err)
	}
	ledger := map[string]map[string][]int64{}
	for _, p := range postings {
		wf := p.WorkflowID.String()
		if ledger[wf] == nil {
			ledger[wf] = map[string][]int64{}
		}
		ledger[wf][p.Kind] = append(ledger[wf][p.Kind], p.AmountCents)
	}
	oracle := map[string]map[string][]int64{}
	for _, e := range effects {
		amount := -e.AmountCents
		if e.Type == "charge" {
			amount = e.AmountCents
		}
		if oracle[e.WorkflowID] == nil {
			oracle[e.WorkflowID] = map[string][]int64{}
		}
		oracle[e.WorkflowID][e.Type] = append(oracle[e.WorkflowID][e.Type], amount)
	}
	for _, wf := range parked {
		for _, typ := range []string{"charge", "payout", "refund"} {
			moved, posted := slices.Sorted(slices.Values(oracle[wf][typ])), slices.Sorted(slices.Values(ledger[wf][typ]))
			differs := !slices.Equal(moved, posted)
			switch {
			case differs && !reported[wf][typ]:
				add(wf, "parked workflow: %s differs (provider %v, ledger %v) but was not reported", typ, moved, posted)
			case !differs && reported[wf][typ]:
				add(wf, "parked workflow: %s reported, but the provider and the ledger agree (%v)", typ, moved)
			}
		}
	}
	return out, nil
}
