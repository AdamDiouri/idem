// Package invariants checks the system's correctness properties (design §13)
// against the database and the payment provider's effects oracle.
package invariants

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/store/db"
)

// Violation is one broken invariant.
type Violation struct {
	Invariant  string // "I1"–"I7"
	WorkflowID string // empty for global violations
	Detail     string
}

func (v Violation) String() string {
	wf := v.WorkflowID
	if wf == "" {
		wf = "(global)"
	}
	return fmt.Sprintf("%s %s: %s", v.Invariant, wf, v.Detail)
}

// Check evaluates the invariants of design §13 and returns violations sorted
// by invariant, then workflow.
//
//   - I1: per workflow, the provider performed at most one effect of each type.
//     Counted per workflow, not per key, so a bug that mints a new key is caught.
//   - I2: every ledger transaction sums to zero, and so does the whole ledger.
//   - I3: the ledger agrees with the provider: per workflow and kind, as many
//     postings as effects, with the same amounts; and every refund reverses a
//     charge of its own workflow. A workflow in NEEDS_ATTENTION is exempt
//     only while its outcome is genuinely unknown (money may have moved
//     without a posting) until a human reconciles it: a DEAD step whose reason
//     leaves the provider's action unknown, or a parked step with no DEAD
//     reason (sticky doubt). Parked only for reasons where the provider
//     definitively did nothing (permanent_after_in_doubt,
//     attempts_exhausted_no_effect), the ledger must match exactly.
//   - I5: no payout without a charge.
//   - I7: terminal workflows match what moved: SUCCEEDED = charge + payout,
//     COMPENSATED = charge + a refund of the same amount and no payout,
//     FAILED = nothing. A workflow parked because its payout was refused
//     after an ambiguous attempt shows a charge and nothing else.
//   - I6: every inbound idempotency key maps to exactly one workflow.
//   - I8: every terminal workflow has exactly one outbox event, of its type,
//     carrying its view; no other workflow has one (checkOutbox).
//
// I4 (liveness) is CheckLiveness: it only holds after quiescence.
func Check(ctx context.Context, pool *pgxpool.Pool, effects []fakeprovider.Effect) ([]Violation, error) {
	violations := checkDuplicateEffects(effects)
	violations = append(violations, checkPayoutHasCharge(effects)...)

	q := db.New(pool)
	postings, err := q.LedgerTransactionsSummary(ctx)
	if err != nil {
		return nil, fmt.Errorf("query ledger summary: %w", err)
	}
	attention, err := NeedsAttention(ctx, pool)
	if err != nil {
		return nil, err
	}
	parkedSteps, err := q.ParkedStepReasons(ctx)
	if err != nil {
		return nil, fmt.Errorf("query parked steps: %w", err)
	}
	violations = append(violations, checkLedgerMatchesProvider(effects, postings, unresolved(attention, parkedSteps))...)
	violations = append(violations, checkParkedShapes(effects, parkedSteps)...)
	violations = append(violations, checkRefundOwnership(effects)...)

	terminal, err := q.TerminalWorkflows(ctx)
	if err != nil {
		return nil, fmt.Errorf("query terminal workflows: %w", err)
	}
	violations = append(violations, checkTerminalStates(effects, terminal)...)

	conflicts, err := q.ConflictEvents(ctx)
	if err != nil {
		return nil, fmt.Errorf("query conflicts: %w", err)
	}
	for _, c := range conflicts {
		// I10: the fake provider never truly contradicts itself, so a
		// conflict means idem misread its news.
		violations = append(violations, Violation{Invariant: "I10", WorkflowID: c.WorkflowID.String(),
			Detail: fmt.Sprintf("provider event %s: %s", c.ProviderEventID, c.Note)})
	}

	outboxViolations, err := checkOutbox(ctx, q)
	if err != nil {
		return nil, err
	}
	violations = append(violations, outboxViolations...)

	keyViolations, err := checkKeys(ctx, q)
	if err != nil {
		return nil, err
	}
	violations = append(violations, keyViolations...)

	unbalanced, err := q.UnbalancedLedgerTransactions(ctx)
	if err != nil {
		return nil, fmt.Errorf("query unbalanced ledger transactions: %w", err)
	}
	for _, u := range unbalanced {
		violations = append(violations, Violation{
			Invariant:  "I2",
			WorkflowID: u.WorkflowID.String(),
			Detail:     fmt.Sprintf("ledger transaction %s sums to %d", u.ID, u.Total),
		})
	}

	total, err := q.LedgerGlobalSum(ctx)
	if err != nil {
		return nil, fmt.Errorf("query ledger total: %w", err)
	}
	if total != 0 {
		violations = append(violations, Violation{Invariant: "I2", Detail: fmt.Sprintf("ledger sums to %d", total)})
	}

	sort.SliceStable(violations, func(i, j int) bool {
		if violations[i].Invariant != violations[j].Invariant {
			return violations[i].Invariant < violations[j].Invariant
		}
		return violations[i].WorkflowID < violations[j].WorkflowID
	})
	return violations, nil
}

func checkDuplicateEffects(effects []fakeprovider.Effect) []Violation {
	type group struct{ workflowID, effectType string }
	keys := map[group][]string{}
	var order []group
	for _, e := range effects {
		g := group{e.WorkflowID, e.Type}
		if _, seen := keys[g]; !seen {
			order = append(order, g)
		}
		keys[g] = append(keys[g], e.IdempotencyKey)
	}

	var violations []Violation
	for _, g := range order {
		if ks := keys[g]; len(ks) > 1 {
			violations = append(violations, Violation{
				Invariant:  "I1",
				WorkflowID: g.workflowID,
				Detail:     fmt.Sprintf("%d %s effects (idempotency keys: %s)", len(ks), g.effectType, strings.Join(ks, ", ")),
			})
		}
	}
	return violations
}

type kindKey struct{ workflowID, kind string }

// NeedsAttention returns the IDs of workflows waiting for a human.
func NeedsAttention(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	ids, err := db.New(pool).NeedsAttentionWorkflows(ctx)
	if err != nil {
		return nil, fmt.Errorf("query workflows needing attention: %w", err)
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out, nil
}

func checkRefundOwnership(effects []fakeprovider.Effect) []Violation {
	chargeOwner := map[string]string{}
	for _, e := range effects {
		if e.Type == "charge" {
			chargeOwner[e.ID] = e.WorkflowID
		}
	}
	var violations []Violation
	for _, e := range effects {
		if e.Type != "refund" {
			continue
		}
		owner, ok := chargeOwner[e.RefundOf]
		switch {
		case !ok:
			violations = append(violations, Violation{Invariant: "I3", WorkflowID: e.WorkflowID,
				Detail: fmt.Sprintf("refund %s reverses unknown charge %s", e.ID, e.RefundOf)})
		case owner != e.WorkflowID:
			violations = append(violations, Violation{Invariant: "I3", WorkflowID: e.WorkflowID,
				Detail: fmt.Sprintf("refund %s reverses charge %s of another workflow (%s)", e.ID, e.RefundOf, owner)})
		}
	}
	return violations
}

func checkLedgerMatchesProvider(effects []fakeprovider.Effect, postings []db.LedgerTransactionsSummaryRow, attention []string) []Violation {
	exempt := map[string]bool{}
	for _, id := range attention {
		exempt[id] = true
	}
	provider := map[kindKey][]int64{}
	ledger := map[kindKey][]int64{}
	var keys []kindKey
	add := func(m map[kindKey][]int64, k kindKey, amount int64) {
		if _, seen := provider[k]; !seen {
			if _, seen := ledger[k]; !seen {
				keys = append(keys, k)
			}
		}
		m[k] = append(m[k], amount)
	}
	for _, e := range effects {
		add(provider, kindKey{e.WorkflowID, e.Type}, e.AmountCents)
	}
	for _, p := range postings {
		add(ledger, kindKey{p.WorkflowID.String(), p.Kind}, p.Debits)
	}

	var violations []Violation
	for _, k := range keys {
		if exempt[k.workflowID] {
			continue
		}
		pa, la := sortedCopy(provider[k]), sortedCopy(ledger[k])
		if !slices.Equal(pa, la) {
			violations = append(violations, Violation{
				Invariant:  "I3",
				WorkflowID: k.workflowID,
				Detail:     fmt.Sprintf("%s: provider moved %v, ledger posted %v", k.kind, pa, la),
			})
		}
	}
	return violations
}

func sortedCopy(xs []int64) []int64 {
	out := slices.Clone(xs)
	slices.Sort(out)
	if out == nil {
		out = []int64{}
	}
	return out
}

// effectCounts returns, per workflow, how many effects of each type happened.
func effectCounts(effects []fakeprovider.Effect) map[string]map[string]int {
	counts := map[string]map[string]int{}
	for _, e := range effects {
		if counts[e.WorkflowID] == nil {
			counts[e.WorkflowID] = map[string]int{}
		}
		counts[e.WorkflowID][e.Type]++
	}
	return counts
}

func checkPayoutHasCharge(effects []fakeprovider.Effect) []Violation {
	var violations []Violation
	for wf, c := range effectCounts(effects) {
		if c["payout"] > 0 && c["charge"] == 0 {
			violations = append(violations, Violation{Invariant: "I5", WorkflowID: wf, Detail: "payout without a charge"})
		}
	}
	return violations
}

func checkTerminalStates(effects []fakeprovider.Effect, terminal []db.TerminalWorkflowsRow) []Violation {
	counts := effectCounts(effects)
	amounts := effectAmounts(effects)
	var violations []Violation
	for _, w := range terminal {
		c := counts[w.ID.String()]
		var ok bool
		switch w.Status {
		case "SUCCEEDED":
			ok = c["charge"] > 0 && c["payout"] > 0 && c["refund"] == 0
		case "COMPENSATED":
			ok = c["charge"] > 0 && c["refund"] > 0 && c["payout"] == 0
			if a := amounts[w.ID.String()]; ok && a["refund"] != a["charge"] {
				violations = append(violations, Violation{
					Invariant:  "I7",
					WorkflowID: w.ID.String(),
					Detail:     fmt.Sprintf("COMPENSATED but refund %d ≠ charge %d", a["refund"], a["charge"]),
				})
			}
		case "FAILED":
			ok = len(c) == 0
		}
		if !ok {
			violations = append(violations, Violation{
				Invariant:  "I7",
				WorkflowID: w.ID.String(),
				Detail:     fmt.Sprintf("%s but provider effects are charge=%d payout=%d refund=%d", w.Status, c["charge"], c["payout"], c["refund"]),
			})
		}
	}
	return violations
}

// effectAmounts returns, per workflow, the total amount of each effect type.
func effectAmounts(effects []fakeprovider.Effect) map[string]map[string]int64 {
	out := map[string]map[string]int64{}
	for _, e := range effects {
		if out[e.WorkflowID] == nil {
			out[e.WorkflowID] = map[string]int64{}
		}
		out[e.WorkflowID][e.Type] += e.AmountCents
	}
	return out
}

// definitiveReasons are DEAD reasons after which the provider is known to
// have done nothing for that step.
var definitiveReasons = map[string]bool{
	"permanent_after_in_doubt":     true,
	"attempts_exhausted_no_effect": true,
	"hook_failed_no_effect":        true,
}

// unresolved returns the parked workflows whose outcome is unknown: those
// with a DEAD step for an uncertain reason, or with no DEAD step at all
// (sticky doubt on a refused step). They are exempt from I3.
func unresolved(attention []string, parked []db.ParkedStepReasonsRow) []string {
	definitive := map[string]bool{} // every DEAD step definitive
	for _, p := range parked {
		id := p.WorkflowID.String()
		if _, seen := definitive[id]; !seen {
			definitive[id] = true
		}
		if !definitiveReasons[p.DeadReason] {
			definitive[id] = false
		}
	}
	var out []string
	for _, id := range attention {
		if !definitive[id] {
			out = append(out, id)
		}
	}
	return out
}

// checkParkedShapes checks I7 for a payout parked under the pivot rule: it was
// refused after an ambiguous attempt, so the provider shows the charge only.
func checkParkedShapes(effects []fakeprovider.Effect, parked []db.ParkedStepReasonsRow) []Violation {
	counts := effectCounts(effects)
	var violations []Violation
	for _, p := range parked {
		if p.Name != "payout" || p.DeadReason != "permanent_after_in_doubt" {
			continue
		}
		c := counts[p.WorkflowID.String()]
		if c["charge"] != 1 || c["payout"] != 0 || c["refund"] != 0 {
			violations = append(violations, Violation{
				Invariant:  "I7",
				WorkflowID: p.WorkflowID.String(),
				Detail: fmt.Sprintf("NEEDS_ATTENTION (payout permanent_after_in_doubt) but provider effects are charge=%d payout=%d refund=%d",
					c["charge"], c["payout"], c["refund"]),
			})
		}
	}
	return violations
}
