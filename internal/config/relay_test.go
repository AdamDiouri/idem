package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/config"
)

func setRelayRequired(t *testing.T) {
	t.Helper()
	t.Setenv("IDEM_DATABASE_URL", "postgres://idem:idem@localhost:5432/idem")
	for _, k := range []string{
		"IDEM_CLIENT_WEBHOOKS", "IDEM_CLIENT_WEBHOOK_SECRETS", "IDEM_RELAY_LEASE_TTL", "IDEM_RELAY_TIMEOUT",
		"IDEM_RELAY_MAX_ATTEMPTS", "IDEM_RELAY_RETRY_BASE", "IDEM_RELAY_RETRY_CAP", "IDEM_POLL_INTERVAL",
		"IDEM_SHUTDOWN_GRACE", "IDEM_WORKER_ID", "IDEM_FAILPOINTS", "IDEM_LOG_LEVEL", "IDEM_METRICS_ADDR",
		"IDEM_RELAY_CONCURRENCY", "IDEM_RELAY_PER_CLIENT",
	} {
		t.Setenv(k, "")
	}
}

// Slice 2.5 (M2 design §5.3): the relay's defaults.
func TestLoadRelay_Defaults(t *testing.T) {
	setRelayRequired(t)
	cfg, err := config.LoadRelay()
	require.NoError(t, err)
	require.Empty(t, cfg.ClientWebhooks)
	require.Equal(t, 30*time.Second, cfg.LeaseTTL)
	require.Equal(t, 10*time.Second, cfg.Timeout)
	require.Equal(t, int32(15), cfg.MaxAttempts)
	require.Equal(t, 30*time.Second, cfg.RetryBase)
	require.Equal(t, 6*time.Hour, cfg.RetryCap)
	require.Equal(t, 500*time.Millisecond, cfg.PollInterval)
	require.Equal(t, 15*time.Second, cfg.ShutdownGrace)
	require.Equal(t, ":8090", cfg.MetricsAddr)
	require.Equal(t, 4, cfg.Concurrency)
	require.Equal(t, 2, cfg.PerClient)
}

func TestLoadRelay_ParsesClients(t *testing.T) {
	setRelayRequired(t)
	t.Setenv("IDEM_CLIENT_WEBHOOKS", "demo=http://shop:8000/hooks,acme=https://acme.example/idem")
	t.Setenv("IDEM_CLIENT_WEBHOOK_SECRETS", "demo=whsec-demo-0123456789,acme=whsec-acme-0123456789")
	cfg, err := config.LoadRelay()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"demo": "http://shop:8000/hooks", "acme": "https://acme.example/idem"}, cfg.ClientWebhooks)
	require.Equal(t, "whsec-acme-0123456789", cfg.ClientWebhookSecrets["acme"])
}

// Review focus 3: a misconfigured client stops the relay at startup, with a
// message naming what is wrong.
func TestLoadRelay_Rejects(t *testing.T) {
	const okURL, okSecret = "demo=http://shop:8000/hooks", "demo=whsec-demo-0123456789"
	cases := []struct {
		name, hooks, secrets string
		env                  map[string]string
		want                 string
	}{
		{"url without secret", okURL, "", nil, `client "demo"`},
		{"secret without url", "", okSecret, nil, `client "demo"`},
		{"relative url", "demo=/hooks", okSecret, nil, `client "demo"`},
		{"not http", "demo=ftp://shop/hooks", okSecret, nil, `client "demo"`},
		{"no host", "demo=http:///hooks", okSecret, nil, `client "demo"`},
		{"short secret", okURL, "demo=short", nil, `client "demo"`},
		{"duplicate url", okURL + ",demo=http://other/hooks", okSecret, nil, "IDEM_CLIENT_WEBHOOKS"},
		{"duplicate secret", okURL, okSecret + ",demo=whsec-other-0123456789", nil, "IDEM_CLIENT_WEBHOOK_SECRETS"},
		{"timeout too close to lease", okURL, okSecret, map[string]string{"IDEM_RELAY_TIMEOUT": "29s"}, "IDEM_RELAY_TIMEOUT"},
		{"grace below timeout", okURL, okSecret, map[string]string{"IDEM_SHUTDOWN_GRACE": "5s"}, "IDEM_SHUTDOWN_GRACE"},
		{"no attempts", okURL, okSecret, map[string]string{"IDEM_RELAY_MAX_ATTEMPTS": "0"}, "IDEM_RELAY_MAX_ATTEMPTS"},
		{"zero base", okURL, okSecret, map[string]string{"IDEM_RELAY_RETRY_BASE": "0s"}, "IDEM_RELAY_RETRY_BASE"},
		{"cap below base", okURL, okSecret, map[string]string{"IDEM_RELAY_RETRY_CAP": "10s"}, "IDEM_RELAY_RETRY_CAP"},
		{"no concurrency", okURL, okSecret, map[string]string{"IDEM_RELAY_CONCURRENCY": "0"}, "IDEM_RELAY_CONCURRENCY"},
		{"per client above concurrency", okURL, okSecret, map[string]string{"IDEM_RELAY_PER_CLIENT": "5"}, "IDEM_RELAY_PER_CLIENT"},
		{"unknown failpoint", okURL, okSecret, map[string]string{"IDEM_FAILPOINTS": "before_commit=exit"}, "before_commit"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setRelayRequired(t)
			t.Setenv("IDEM_CLIENT_WEBHOOKS", c.hooks)
			t.Setenv("IDEM_CLIENT_WEBHOOK_SECRETS", c.secrets)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			_, err := config.LoadRelay()
			require.ErrorContains(t, err, c.want)
		})
	}
}

// Final review, Important 2: a stray space must not create a client " b" whose
// events silently become NO_ENDPOINT.
func TestLoadRelay_TrimsClientIDs(t *testing.T) {
	setRelayRequired(t)
	t.Setenv("IDEM_CLIENT_WEBHOOKS", "a=http://x/hooks, b = http://y/hooks")
	t.Setenv("IDEM_CLIENT_WEBHOOK_SECRETS", " a=whsec-a-0123456789012,b=whsec-b-0123456789012 ")
	cfg, err := config.LoadRelay()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"a": "http://x/hooks", "b": "http://y/hooks"}, cfg.ClientWebhooks)
	require.Equal(t, "whsec-b-0123456789012", cfg.ClientWebhookSecrets["b"])
}

func TestLoadRelay_RejectsBadClientIDs(t *testing.T) {
	for name, hooks := range map[string]string{
		"empty id":     "=http://x/hooks",
		"inner space":  "a b=http://x/hooks",
		"no separator": "a",
	} {
		t.Run(name, func(t *testing.T) {
			setRelayRequired(t)
			t.Setenv("IDEM_CLIENT_WEBHOOKS", hooks)
			_, err := config.LoadRelay()
			require.ErrorContains(t, err, "IDEM_CLIENT_WEBHOOKS")
		})
	}
}

// Final review, minor 5: a malformed secrets list never echoes a secret.
func TestLoadRelay_ErrorsNeverShowSecrets(t *testing.T) {
	setRelayRequired(t)
	t.Setenv("IDEM_CLIENT_WEBHOOKS", "a=http://x/hooks")
	t.Setenv("IDEM_CLIENT_WEBHOOK_SECRETS", "a=whsec-top,secret-part-0123456789")
	_, err := config.LoadRelay()
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret-part")
	require.NotContains(t, err.Error(), "whsec-top")
}
