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
	r.Webhooks.Applied = 3
	r.Evaluate()
	require.NotContains(t, strings.Join(r.Reasons, "\n"), "no reordered webhook was recognised as stale",
		"short runs have no reserved reorder window")

	r = healthy()
	r.Config.Duration = time.Minute
	r.Webhooks.Applied = 3
	r.Evaluate()
	require.Contains(t, strings.Join(r.Reasons, "\n"), "vacuous: no reordered webhook was recognised as stale")
}
