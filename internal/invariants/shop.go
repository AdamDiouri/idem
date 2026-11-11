package invariants

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/fakeshop"
	"github.com/AdamDiouri/idem/internal/store/db"
)

// CheckShop is I9 (slice 2.5): the shop's deduplicated view equals idem's
// outcomes for its clients. Every outbox event of those clients' workflows
// reached the shop; the shop saw no event idem never wrote; each event it saw
// carries the workflow's outcome and type; and every delivery was correctly
// signed (idem always signs, so a refused signature means a bug or a forger).
// Workflows with no event at all are I8's business.
func CheckShop(ctx context.Context, pool *pgxpool.Pool, seen fakeshop.Seen, clients []string) ([]Violation, error) {
	rows, err := db.New(pool).OutboxEventsForShopCheck(ctx, clients)
	if err != nil {
		return nil, fmt.Errorf("query outbox events: %w", err)
	}
	var out []Violation
	add := func(wf, format string, args ...any) {
		out = append(out, Violation{Invariant: "I9", WorkflowID: wf, Detail: fmt.Sprintf(format, args...)})
	}
	written := make(map[string]db.OutboxEventsForShopCheckRow, len(rows))
	for _, r := range rows {
		written[r.ID.String()] = r
	}
	shop := make(map[string]fakeshop.SeenEvent, len(seen.Events))
	for _, e := range seen.Events {
		shop[e.EventID] = e
	}
	for _, r := range rows {
		if _, ok := shop[r.ID.String()]; !ok {
			add(r.WorkflowID.String(), "the shop never learned this outcome (event %s, %s, %s)", r.ID, r.Type, r.Status)
		}
	}
	for _, e := range seen.Events {
		r, ok := written[e.EventID]
		switch {
		case !ok:
			add(e.WorkflowID, "the shop saw event %s, which idem never wrote", e.EventID)
		case e.WorkflowID != r.WorkflowID.String():
			add(r.WorkflowID.String(), "event %s reached the shop about workflow %s", e.EventID, e.WorkflowID)
		case e.Status != r.ViewStatus:
			add(r.WorkflowID.String(), "the shop was told %q, but the workflow is %q", e.Status, r.ViewStatus)
		case e.Type != r.Type:
			add(r.WorkflowID.String(), "the shop saw event %s as %s, but it is %s", e.EventID, e.Type, r.Type)
		}
	}
	if seen.BadSignatures > 0 {
		add("", "the shop refused %d deliveries for their signature", seen.BadSignatures)
	}
	return out, nil
}
