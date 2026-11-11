package provider_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
	"github.com/AdamDiouri/idem/internal/provider"
)

// Slice 2.6 (M2 design §6): one signed row per money movement, from the
// provider's production-style report, never from the test oracle.
func TestListBalanceTransactions_OneRowPerMovement(t *testing.T) {
	ctx := context.Background()
	p := fakeprovider.New(fakeprovider.Options{Hang: 5 * time.Second, PayoutSettle: time.Hour})
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)
	c := provider.NewClient(srv.URL, time.Second)
	from := time.Now().Add(-time.Minute)

	charge := c.CreateCharge(ctx, "ch-key", chargeReq("cus_1"))
	require.Equal(t, provider.Success, charge.Class)
	require.Equal(t, provider.Accepted, c.CreatePayout(ctx, "po-key", payoutReq("mer_1")).Class)
	require.Equal(t, provider.Success, c.CreateRefund(ctx, "re-key", refundReq(charge.ProviderRef)).Class)

	rows, err := c.ListBalanceTransactions(ctx, from, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, rows, 2, "a pending payout moved no money yet")
	p.SettleNow()
	rows, err = c.ListBalanceTransactions(ctx, from, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, rows, 3)
	amounts := map[string]int64{}
	for _, r := range rows {
		amounts[r.Type] = r.Amount
		require.Equal(t, wfID.String(), r.WorkflowID)
		require.Equal(t, "EUR", r.Currency)
		require.NotEmpty(t, r.Source)
		require.False(t, r.Created.IsZero())
	}
	require.Equal(t, map[string]int64{"charge": 10000, "payout": -9825, "refund": -10000}, amounts)
}

func TestListBalanceTransactions_ReadsEveryPage(t *testing.T) {
	ctx := context.Background()
	_, url := fake(t)
	c := provider.NewClient(url, time.Second)
	from := time.Now().Add(-time.Minute)
	for i := range 250 { // more than two pages of 100
		require.Equal(t, provider.Success, c.CreateCharge(ctx, fmt.Sprintf("k-%d", i), chargeReq("cus_1")).Class)
	}
	rows, err := c.ListBalanceTransactions(ctx, from, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, rows, 250)
	seen := map[string]bool{}
	for _, r := range rows {
		require.False(t, seen[r.ID], "no row twice")
		seen[r.ID] = true
	}
}

// Review focus 3: a broken page fails the read; nothing is reconciled on a
// partial report.
func TestListBalanceTransactions_FailsOnABadPage(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			_, _ = w.Write([]byte(`{"data":[{"id":"txn_1","type":"charge","amount":100,"currency":"EUR","created":1}],"has_more":true}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	_, err := provider.NewClient(srv.URL, time.Second).ListBalanceTransactions(context.Background(), time.Unix(0, 0), time.Now())
	require.Error(t, err)
	require.Equal(t, 2, calls)
}
