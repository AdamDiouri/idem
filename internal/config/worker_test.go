package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/config"
)

func setWorkerRequired(t *testing.T) {
	t.Helper()
	t.Setenv("IDEM_DATABASE_URL", "postgres://idem:idem@localhost:5432/idem")
	t.Setenv("IDEM_PROVIDER_URL", "http://localhost:9090")
	for _, k := range []string{
		"IDEM_API_TOKENS", "IDEM_PROVIDER_TIMEOUT", "IDEM_LEASE_TTL", "IDEM_POLL_INTERVAL",
		"IDEM_RETRY_DELAY", "IDEM_SWEEP_INTERVAL", "IDEM_WORKER_ID", "IDEM_FAILPOINTS", "IDEM_LOG_LEVEL",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadWorker_Defaults(t *testing.T) {
	setWorkerRequired(t)
	cfg, err := config.LoadWorker()
	require.NoError(t, err)
	require.Equal(t, "http://localhost:9090", cfg.ProviderURL)
	require.Equal(t, 10*time.Second, cfg.ProviderTimeout)
	require.Equal(t, 30*time.Second, cfg.LeaseTTL)
	require.Equal(t, 500*time.Millisecond, cfg.PollInterval)
	require.Equal(t, 2*time.Second, cfg.RetryDelay)
	require.Equal(t, 5*time.Second, cfg.SweepInterval)
	require.Equal(t, "", cfg.WorkerID)
	require.Equal(t, "", cfg.Failpoints)
	require.Equal(t, "info", cfg.LogLevel)
}

func TestLoadWorker_Invalid(t *testing.T) {
	cases := []struct {
		name, key, value, wantVar string
	}{
		{"missing database url", "IDEM_DATABASE_URL", "", "IDEM_DATABASE_URL"},
		{"missing provider url", "IDEM_PROVIDER_URL", "", "IDEM_PROVIDER_URL"},
		{"provider url not a url", "IDEM_PROVIDER_URL", "not a url", "IDEM_PROVIDER_URL"},
		{"provider url wrong scheme", "IDEM_PROVIDER_URL", "ftp://x", "IDEM_PROVIDER_URL"},
		{"provider timeout too close to lease", "IDEM_PROVIDER_TIMEOUT", "29s", "IDEM_PROVIDER_TIMEOUT"},
		{"zero provider timeout", "IDEM_PROVIDER_TIMEOUT", "0s", "IDEM_PROVIDER_TIMEOUT"},
		{"zero poll interval", "IDEM_POLL_INTERVAL", "0s", "IDEM_POLL_INTERVAL"},
		{"zero sweep interval", "IDEM_SWEEP_INTERVAL", "0s", "IDEM_SWEEP_INTERVAL"},
		{"negative retry delay", "IDEM_RETRY_DELAY", "-1s", "IDEM_RETRY_DELAY"},
		{"bad failpoints", "IDEM_FAILPOINTS", "x=fly", "IDEM_FAILPOINTS"},
		{"unknown log level", "IDEM_LOG_LEVEL", "loud", "IDEM_LOG_LEVEL"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setWorkerRequired(t)
			t.Setenv(c.key, c.value)
			_, err := config.LoadWorker()
			require.Error(t, err)
			require.Contains(t, err.Error(), c.wantVar)
		})
	}
}

func TestLoad_APIConfigUnaffected(t *testing.T) {
	setWorkerRequired(t)
	_, err := config.LoadWorker()
	require.NoError(t, err, "workers must not need API tokens")

	_, err = config.Load()
	require.Error(t, err)
	require.Contains(t, err.Error(), "IDEM_API_TOKENS")
}
