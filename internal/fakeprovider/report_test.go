package fakeprovider_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type reportPage struct {
	Data []struct {
		ID      string `json:"id"`
		Created int64  `json:"created"`
	} `json:"data"`
	HasMore bool `json:"has_more"`
}

func getReport(t *testing.T, url, query string) (int, reportPage) {
	t.Helper()
	resp, err := http.Get(url + "/reports/balance_transactions?" + query)
	require.NoError(t, err)
	defer resp.Body.Close()
	var p reportPage
	if resp.StatusCode == http.StatusOK {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&p))
	}
	return resp.StatusCode, p
}

// Slice 2.6: pages in created order, bounded by the window.
func TestReport_WindowAndPaging(t *testing.T) {
	_, srv := newServer(t)
	from := time.Now().Add(-time.Second).Unix()
	for i := range 7 {
		mustCharge(t, srv.URL, fmt.Sprintf("k-%d", i), body("cus_1", 1000))
	}
	to := time.Now().Add(2 * time.Second).Unix()
	window := fmt.Sprintf("created_gte=%d&created_lt=%d", from, to)

	var ids []string
	after := ""
	for _, want := range []int{3, 3, 1} {
		q := window + "&limit=3"
		if after != "" {
			q += "&starting_after=" + after
		}
		code, page := getReport(t, srv.URL, q)
		require.Equal(t, http.StatusOK, code)
		require.Len(t, page.Data, want)
		require.Equal(t, want == 3, page.HasMore)
		for _, r := range page.Data {
			ids = append(ids, r.ID)
		}
		after = page.Data[len(page.Data)-1].ID
	}
	require.Len(t, ids, 7)

	_, empty := getReport(t, srv.URL, fmt.Sprintf("created_gte=%d&created_lt=%d", to, to+60))
	require.Empty(t, empty.Data)
	require.False(t, empty.HasMore)
}

func TestReport_RejectsBadParameters(t *testing.T) {
	_, srv := newServer(t)
	for _, q := range []string{"created_gte=x", "created_lt=-", "limit=0", "limit=101", "starting_after=txn_unknown"} {
		code, _ := getReport(t, srv.URL, q)
		require.Equal(t, http.StatusBadRequest, code, q)
	}
}
