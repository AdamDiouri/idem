// Package recon reconciles idem with its payment provider (M2 design §6, D15):
// it compares the provider's own record of money movements with the ledger
// and the steps, records every difference, and changes nothing else.
package recon

import (
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/AdamDiouri/idem/internal/provider"
)

// Kinds of discrepancy, by precedence (one per workflow and effect type).
const (
	DuplicateEffect   = "duplicate_effect" // more than one charge, payout or refund (either side)
	AmountMismatch    = "amount_mismatch"  // amount or currency
	StatusMismatch    = "status_mismatch"  // money moved for a step idem recorded as definitely refused
	MissingInLedger   = "missing_in_ledger"
	MissingAtProvider = "missing_at_provider"
)

var precedence = []string{DuplicateEffect, AmountMismatch, StatusMismatch, MissingInLedger, MissingAtProvider}

// EffectTypes are the movements compared.
var EffectTypes = []string{"charge", "payout", "refund"}

// Window is the period a run judges, [From, To).
type Window struct{ From, To time.Time }

func (w Window) contains(t time.Time) bool { return !t.Before(w.From) && t.Before(w.To) }

// Step is a step as idem knows it.
type Step struct {
	ID          uuid.UUID
	Name        string
	Status      string
	ProviderRef string // the provider's object (ch_, po_, re_) once known
}

// Posting is a ledger posting, by the step that made it.
type Posting struct {
	StepID   uuid.UUID
	Kind     string
	Amount   int64 // its provider_receivable entry: signed like the provider's amount
	Currency string
	At       time.Time
}

// Workflow is what idem knows about one workflow.
type Workflow struct {
	ID       uuid.UUID
	Status   string
	Steps    []Step
	Postings []Posting
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
	// ItemKey and ItemAt identify what is wrong and when it happened, so a
	// later run covering ItemAt can resolve it.
	ItemKey string
	ItemAt  time.Time
}

// notFinal are step statuses whose outcome is not known yet.
var notFinal = []string{"PENDING", "RUNNING", "IN_DOUBT", "AWAITING_CALLBACK"}

// Classify judges the items of one workflow that fall in the window: each
// provider movement created in it must have its posting (matched through the
// step's provider reference), and each posting created in it its movement
// (looked for among all records read, which reach back a margin before the
// window). History outside the window is never judged: a payout settling days
// after its charge is not a difference. A movement whose step is not final in
// idem's snapshot is in flight, not a difference. w is nil when the provider
// names a workflow idem does not know. It returns the discrepancies (at most
// one per effect type, by precedence) and how many items were in flight.
func Classify(w *Workflow, records []provider.BalanceTransaction, win Window) (out []Discrepancy, inFlight int) {
	var id *uuid.UUID
	parked := false
	key := ""
	if w != nil {
		id, parked, key = &w.ID, w.Status == "NEEDS_ATTENTION", w.ID.String()
	} else if len(records) > 0 {
		key = records[0].WorkflowID
		if parsed, err := uuid.Parse(records[0].WorkflowID); err == nil {
			id, key = &parsed, parsed.String()
		}
	}

	for _, typ := range EffectTypes {
		var recs []provider.BalanceTransaction
		for _, r := range records {
			if r.Type == typ {
				recs = append(recs, r)
			}
		}
		var steps []Step
		var posts []Posting
		if w != nil {
			for _, s := range w.Steps {
				if s.Name == typ {
					steps = append(steps, s)
				}
			}
			for _, p := range w.Postings {
				if p.Kind == typ {
					posts = append(posts, p)
				}
			}
		}

		found := map[string]Discrepancy{}
		note := func(kind string, at time.Time, refs []string, provAmt, ledgerAmt *int64, format string, args ...any) {
			if _, ok := found[kind]; ok {
				return
			}
			found[kind] = Discrepancy{WorkflowID: id, Kind: kind, EffectType: typ, ProviderRefs: refs, ProviderAmount: provAmt,
				LedgerAmount: ledgerAmt, Parked: parked, Detail: fmt.Sprintf(format, args...), ItemKey: key + "|" + typ, ItemAt: at}
		}
		touched := false

		// Duplicates: two distinct movements, or two postings, of one type.
		sources := map[string]bool{}
		for _, r := range recs {
			sources[r.Source] = true
		}
		for _, r := range recs {
			if win.contains(r.Created) {
				touched = true
				if len(sources) > 1 {
					note(DuplicateEffect, r.Created, sortedKeys(sources), &r.Amount, nil, "%d %s movements at the provider for one workflow", len(sources), typ)
				}
			}
		}
		for _, p := range posts {
			if win.contains(p.At) {
				touched = true
				if len(posts) > 1 {
					note(DuplicateEffect, p.At, nil, nil, &p.Amount, "%d %s postings in the ledger for one workflow", len(posts), typ)
				}
			}
		}
		if !touched {
			continue
		}

		// Each movement in the window has its posting.
		for _, r := range recs {
			if !win.contains(r.Created) {
				continue
			}
			step := stepByRef(steps, r.Source)
			if step == nil {
				switch {
				case slices.ContainsFunc(steps, func(s Step) bool { return slices.Contains(notFinal, s.Status) }):
					inFlight++ // the step has not learned its reference yet
				case len(steps) > 0 && steps[len(steps)-1].Status == "FAILED_PERMANENT":
					note(StatusMismatch, r.Created, []string{r.Source}, &r.Amount, nil,
						"%s %s moved %d, but idem recorded the %s as refused", typ, r.Source, r.Amount, typ)
				default:
					note(MissingInLedger, r.Created, []string{r.Source}, &r.Amount, nil,
						"the provider moved %d (%s %s) with no ledger posting", r.Amount, typ, r.Source)
				}
				continue
			}
			p := postingOf(posts, step.ID)
			switch {
			case p != nil && (p.Amount != r.Amount || p.Currency != r.Currency):
				note(AmountMismatch, r.Created, []string{r.Source}, &r.Amount, &p.Amount,
					"%s %s: the provider moved %d %s, the ledger posted %d %s", typ, r.Source, r.Amount, r.Currency, p.Amount, p.Currency)
			case p != nil:
			case slices.Contains(notFinal, step.Status):
				inFlight++
			case step.Status == "FAILED_PERMANENT":
				note(StatusMismatch, r.Created, []string{r.Source}, &r.Amount, nil,
					"%s %s moved %d, but idem recorded it as refused", typ, r.Source, r.Amount)
			default:
				note(MissingInLedger, r.Created, []string{r.Source}, &r.Amount, nil,
					"the provider moved %d (%s %s) with no ledger posting", r.Amount, typ, r.Source)
			}
		}

		// Each posting in the window has its movement.
		for _, p := range posts {
			if !win.contains(p.At) {
				continue
			}
			ref := ""
			if s := stepByID(steps, p.StepID); s != nil {
				ref = s.ProviderRef
			}
			i := slices.IndexFunc(recs, func(r provider.BalanceTransaction) bool { return ref != "" && r.Source == ref })
			if i < 0 {
				note(MissingAtProvider, p.At, nil, nil, &p.Amount, "the ledger posted %d for %s %s, which the provider has no record of", p.Amount, typ, ref)
				continue
			}
			if r := recs[i]; r.Amount != p.Amount || r.Currency != p.Currency {
				note(AmountMismatch, p.At, []string{r.Source}, &r.Amount, &p.Amount,
					"%s %s: the provider moved %d %s, the ledger posted %d %s", typ, r.Source, r.Amount, r.Currency, p.Amount, p.Currency)
			}
		}

		for _, kind := range precedence {
			if d, ok := found[kind]; ok {
				out = append(out, d)
				break
			}
		}
	}
	return out, inFlight
}

func stepByRef(steps []Step, ref string) *Step {
	for i := range steps {
		if steps[i].ProviderRef != "" && steps[i].ProviderRef == ref {
			return &steps[i]
		}
	}
	return nil
}

func stepByID(steps []Step, id uuid.UUID) *Step {
	for i := range steps {
		if steps[i].ID == id {
			return &steps[i]
		}
	}
	return nil
}

func postingOf(posts []Posting, stepID uuid.UUID) *Posting {
	for i := range posts {
		if posts[i].StepID == stepID {
			return &posts[i]
		}
	}
	return nil
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
