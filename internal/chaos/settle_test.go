package chaos

import (
	"strings"
	"testing"

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
