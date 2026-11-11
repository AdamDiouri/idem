package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/testutil"
)

// Slice 2.5, review focus 3: a misconfigured client stops the relay before it
// touches the database, naming the client.
func TestRelayCmd_ConfigErrorIsUsage(t *testing.T) {
	t.Setenv("IDEM_DATABASE_URL", "postgres://idem:idem@127.0.0.1:1/idem?sslmode=disable&connect_timeout=1")
	t.Setenv("IDEM_CLIENT_WEBHOOKS", "demo=http://shop:8000/hooks")
	t.Setenv("IDEM_CLIENT_WEBHOOK_SECRETS", "")
	code, out := run(t, "relay")
	require.Equal(t, 2, code, out)
	require.Contains(t, out, `client "demo"`)
}

// Final review, Important 2: a configured client that has no workflows is
// probably a typo, while the real client's events become NO_ENDPOINT.
func TestRelayCmd_ChecksClientsAgainstWorkflows(t *testing.T) {
	pool := testutil.NewPool(t, dsn)
	parkedCharge(t, pool) // client "c" has a workflow
	withoutURL, unknown, err := checkClients(context.Background(), pool, []string{"cc"})
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, withoutURL)
	require.Equal(t, []string{"cc"}, unknown)
}
