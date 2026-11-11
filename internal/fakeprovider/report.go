package fakeprovider

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// maxReportPage bounds one page of the balance-transactions report.
const maxReportPage = 100

// balanceTransaction is one money movement, as a real provider reports it
// (M2 design §6): a succeeded charge (+), a paid payout (−, at settlement), a
// succeeded refund (−). A failed or pending payout moved no money and has no
// row. Unlike /_effects (the test oracle), this is what a production
// reconciliation job could read from a real provider.
type balanceTransaction struct {
	ID       string            `json:"id"`
	Type     string            `json:"type"`
	Amount   int64             `json:"amount"`
	Currency string            `json:"currency"`
	Created  int64             `json:"created"`
	Source   string            `json:"source"`
	Metadata map[string]string `json:"metadata"`
}

// getBalanceTransactions serves GET /reports/balance_transactions:
// created_gte and created_lt (unix seconds) bound the window, limit (1..100,
// default 100) sizes a page, starting_after continues after a row's id.
// Rows are ordered by created, then id.
func (p *Provider) getBalanceTransactions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from, okFrom := unixParam(q.Get("created_gte"), 0)
	to, okTo := unixParam(q.Get("created_lt"), 1<<62)
	limit, err := strconv.Atoi(cmpOr(q.Get("limit"), "100"))
	if !okFrom || !okTo || err != nil || limit < 1 || limit > maxReportPage {
		writeRaw(w, http.StatusBadRequest, errorBody("invalid_parameters"))
		return
	}

	type timed struct {
		at  time.Time
		row balanceTransaction
	}
	var all []timed
	for _, e := range p.Effects() {
		if e.At.Before(time.Unix(from, 0)) || !e.At.Before(time.Unix(to, 0)) {
			continue
		}
		amount := -e.AmountCents
		if e.Type == "charge" {
			amount = e.AmountCents
		}
		all = append(all, timed{e.At, balanceTransaction{
			ID: "txn_" + e.ID, Type: e.Type, Amount: amount, Currency: e.Currency, Created: e.At.Unix(), Source: e.ID,
			Metadata: map[string]string{"workflow_id": e.WorkflowID, "idempotency_key": e.IdempotencyKey},
		}})
	}
	slices.SortStableFunc(all, func(a, b timed) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return strings.Compare(a.row.ID, b.row.ID)
	})
	rows := make([]balanceTransaction, 0, len(all))
	for _, t := range all {
		rows = append(rows, t.row)
	}
	if after := q.Get("starting_after"); after != "" {
		i := slices.IndexFunc(rows, func(t balanceTransaction) bool { return t.ID == after })
		if i < 0 {
			writeRaw(w, http.StatusBadRequest, errorBody("unknown_starting_after"))
			return
		}
		rows = rows[i+1:]
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	writeRaw(w, http.StatusOK, mustJSON(map[string]any{"data": rows, "has_more": hasMore}))
}

func unixParam(v string, def int64) (int64, bool) {
	if v == "" {
		return def, true
	}
	n, err := strconv.ParseInt(v, 10, 64)
	return n, err == nil && n >= 0
}

func cmpOr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
