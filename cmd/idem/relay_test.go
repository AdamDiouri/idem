package main

import (
	"testing"

	"github.com/stretchr/testify/require"
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
