package chaos

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/fakeprovider"
)

func TestUndeliveredWebhooks(t *testing.T) {
	ds := []fakeprovider.WebhookDelivery{
		{EventID: "delivered", Delivered: true},
		{EventID: "gave-up", GaveUp: true},
		{EventID: "in-flight"},
		{EventID: "both", Delivered: true, GaveUp: true},
	}
	require.Equal(t, 1, undeliveredWebhooks(ds))
	require.Equal(t, 0, undeliveredWebhooks(nil))
}

func TestReport_WebhooksInFlightFails(t *testing.T) {
	r := healthy()
	r.WebhooksInFlight = 2
	r.Evaluate()
	require.False(t, r.Passed)
	require.Contains(t, strings.Join(r.Reasons, "\n"), "2 webhook deliveries still in flight at the verdict")
}

func TestReport_WebhookPathMustDoWork(t *testing.T) {
	r := healthy() // 30s
	r.Webhooks.Applied = 0
	r.Evaluate()
	require.Contains(t, strings.Join(r.Reasons, "\n"), "vacuous: no webhook completed a payout")

	r = healthy()
	r.Evaluate()
	require.NotContains(t, strings.Join(r.Reasons, "\n"), "no reordered webhook was delivered",
		"short runs have no reserved reorder window")

	r = healthy()
	r.Config.Duration = time.Minute
	r.Evaluate()
	require.Contains(t, strings.Join(r.Reasons, "\n"), "vacuous: no reordered webhook was delivered")

	// Final review, minor 8: a stale event that reached a waiting payout is
	// applied with the re-checked state, not ignored; it still counts as the
	// reorder fault doing its work (I10 judges each outcome).
	r = healthy()
	r.Config.Duration = time.Minute
	r.Webhooks.StaleIgnored, r.Webhooks.StaleDelivered = 0, 3
	r.Evaluate()
	require.NotContains(t, strings.Join(r.Reasons, "\n"), "reordered")
}

// Slice 2.3b: chaos runs idem's processes as the restricted application user,
// so a missing grant fails a campaign rather than production.
func TestPostgres_AppDSN(t *testing.T) {
	pg := &Postgres{Port: 5555}
	require.Equal(t, "postgres://"+appDBUser+":"+appDBPassword+"@127.0.0.1:5555/idem?sslmode=disable", pg.AppDSN())
	require.NotContains(t, pg.AppDSN(), "idem:idem@", "not the owner")
}
