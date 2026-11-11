// Package recon reconciles idem with its payment provider (M2 design §6, D15):
// it compares the provider's own record of money movements with the ledger
// and the steps, records every difference, and changes nothing else.
package recon

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/AdamDiouri/idem/internal/provider"
)

// Kinds of discrepancy.
const (
	MissingInLedger   = "missing_in_ledger"   // the provider moved money that idem never recorded
	MissingAtProvider = "missing_at_provider" // idem posted a movement the provider has no record of
	AmountMismatch    = "amount_mismatch"
	DuplicateEffect   = "duplicate_effect" // more than one charge, payout or refund (either side)
	StatusMismatch    = "status_mismatch"  // money moved for a step idem recorded as definitely refused
)

// EffectTypes are the movements compared, in report order.
var EffectTypes = []string{"charge", "payout", "refund"}

// Workflow is what idem knows about one workflow.
type Workflow struct {
	ID         uuid.UUID
	Status     string
	StepStatus map[string]string  // effect type → status of its latest step
	Postings   map[string][]int64 // effect type → each posting's provider_receivable amount (signed like the provider's)
}

// Discrepancy is one difference, for one workflow and effect type.
type Discrepancy struct {
	WorkflowID     *uuid.UUID
	Kind           string
	EffectType     string
	ProviderRefs   []string
	ProviderAmount *int64
	LedgerAmount   *int64
	Parked         bool
	Detail         string
}

// Classify compares one workflow's postings with the provider's records about
// it. w is nil when the provider names a workflow idem does not know. Each
// (workflow, type) yields at most one discrepancy, by precedence:
// duplicate_effect > amount_mismatch > status_mismatch > missing_in_ledger >
// missing_at_provider.
func Classify(w *Workflow, records []provider.BalanceTransaction) []Discrepancy {
	byType := map[string][]provider.BalanceTransaction{}
	for _, r := range records {
		byType[r.Type] = append(byType[r.Type], r)
	}
	var id *uuid.UUID
	parked := false
	postings, steps := map[string][]int64{}, map[string]string{}
	if w != nil {
		id, parked, postings, steps = &w.ID, w.Status == "NEEDS_ATTENTION", w.Postings, w.StepStatus
	} else if len(records) > 0 {
		if parsed, err := uuid.Parse(records[0].WorkflowID); err == nil {
			id = &parsed
		}
	}

	var out []Discrepancy
	for _, typ := range EffectTypes {
		recs, posts := byType[typ], postings[typ]
		d := Discrepancy{WorkflowID: id, EffectType: typ, Parked: parked}
		for _, r := range recs {
			d.ProviderRefs = append(d.ProviderRefs, r.Source)
		}
		if len(recs) > 0 {
			d.ProviderAmount = ptr(recs[0].Amount)
		}
		if len(posts) > 0 {
			d.LedgerAmount = ptr(posts[0])
		}
		switch {
		case len(recs) > 1 || len(posts) > 1:
			d.Kind = DuplicateEffect
			d.Detail = fmt.Sprintf("%d %s records at the provider, %d postings in the ledger", len(recs), typ, len(posts))
		case len(recs) == 1 && len(posts) == 1:
			if recs[0].Amount == posts[0] {
				continue
			}
			d.Kind = AmountMismatch
			d.Detail = fmt.Sprintf("%s: the provider moved %d, the ledger posted %d", typ, recs[0].Amount, posts[0])
		case len(recs) == 1 && steps[typ] == "FAILED_PERMANENT":
			d.Kind = StatusMismatch
			d.Detail = fmt.Sprintf("%s %s moved %d, but idem recorded the %s as refused", typ, recs[0].Source, recs[0].Amount, typ)
		case len(recs) == 1:
			d.Kind = MissingInLedger
			d.Detail = fmt.Sprintf("the provider moved %d (%s %s) with no ledger posting", recs[0].Amount, typ, recs[0].Source)
		case len(posts) == 1:
			d.Kind = MissingAtProvider
			d.Detail = fmt.Sprintf("the ledger posted %d for a %s the provider has no record of", posts[0], typ)
		default:
			continue
		}
		out = append(out, d)
	}
	return out
}

func ptr(v int64) *int64 { return &v }
