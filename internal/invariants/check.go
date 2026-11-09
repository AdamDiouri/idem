// Package invariants checks the system's correctness properties (design §13)
// against the database and the payment provider's effects oracle.
package invariants

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/store/db"
)

// Violation is one broken invariant.
type Violation struct {
	Invariant  string // "I1", "I2"
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

// Check evaluates I1 and I2 and returns violations sorted by invariant, then workflow.
//
//   - I1: per workflow, the provider performed at most one effect of each type.
//     Counted per workflow, not per key, so a bug that mints a new key is caught.
//   - I2: every ledger transaction sums to zero, and so does the whole ledger.
func Check(ctx context.Context, pool *pgxpool.Pool, effects []fakeprovider.Effect) ([]Violation, error) {
	violations := checkDuplicateEffects(effects)

	q := db.New(pool)
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
