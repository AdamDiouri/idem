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
		"IDEM_RETRY_BASE", "IDEM_RETRY_CAP", "IDEM_PROVIDER_KEY_RETENTION", "IDEM_KEY_RETENTION_MARGIN",
		"IDEM_SHUTDOWN_GRACE", "IDEM_STUCK_AFTER", "IDEM_METRICS_ADDR", "IDEM_CALLBACK_DEADLINE", "IDEM_CALLBACK_MAX_POLLS",
		"IDEM_CALLBACK_MAX_WAIT", "IDEM_CALLBACK_POLL_CAP",
		"IDEM_SWEEP_INTERVAL", "IDEM_WORKER_ID", "IDEM_FAILPOINTS", "IDEM_LOG_LEVEL",
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
	require.Equal(t, 2*time.Second, cfg.RetryBase)
	require.Equal(t, 5*time.Minute, cfg.RetryCap)
	require.Equal(t, 24*time.Hour, cfg.ProviderKeyRetention)
	require.Equal(t, time.Hour, cfg.KeyRetentionMargin)
	require.Equal(t, 15*time.Second, cfg.ShutdownGrace)
	require.Equal(t, 15*time.Minute, cfg.StuckAfter)
	require.Equal(t, 5*time.Second, cfg.SweepInterval)
	require.Equal(t, "", cfg.WorkerID)
	require.Equal(t, "", cfg.Failpoints)
	require.Equal(t, "info", cfg.LogLevel)
	require.Equal(t, 10*time.Minute, cfg.CallbackDeadline)
	require.Equal(t, int32(0), cfg.CallbackMaxPolls, "no poll count limit by default")
	require.Equal(t, 72*time.Hour, cfg.CallbackMaxWait)
	require.Equal(t, time.Hour, cfg.CallbackPollCap)
	require.Equal(t, ":8090", cfg.MetricsAddr, "outside 9100-9999, the exporter range (node_exporter is 9100)")
}

func TestLoadWorker_MetricsCanBeDisabled(t *testing.T) {
	setWorkerRequired(t)
	t.Setenv("IDEM_METRICS_ADDR", "off")
	cfg, err := config.LoadWorker()
	require.NoError(t, err)
	require.Equal(t, "off", cfg.MetricsAddr)
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
		{"zero retry base", "IDEM_RETRY_BASE", "0s", "IDEM_RETRY_BASE"},
		{"retry cap below base", "IDEM_RETRY_CAP", "1s", "IDEM_RETRY_CAP"},
		{"margin not below retention", "IDEM_KEY_RETENTION_MARGIN", "24h", "IDEM_KEY_RETENTION_MARGIN"},
		{"zero margin", "IDEM_KEY_RETENTION_MARGIN", "0s", "IDEM_KEY_RETENTION_MARGIN"},
		{"shutdown grace below provider timeout", "IDEM_SHUTDOWN_GRACE", "5s", "IDEM_SHUTDOWN_GRACE"},
		{"zero stuck after", "IDEM_STUCK_AFTER", "0s", "IDEM_STUCK_AFTER"},
		{"bad failpoints", "IDEM_FAILPOINTS", "x=fly", "IDEM_FAILPOINTS"},
		{"unknown failpoint name", "IDEM_FAILPOINTS", "after_provider_cal=exit", "IDEM_FAILPOINTS"},
		{"unknown log level", "IDEM_LOG_LEVEL", "loud", "IDEM_LOG_LEVEL"},
		{"bad metrics address", "IDEM_METRICS_ADDR", "nope", "IDEM_METRICS_ADDR"},
		{"zero callback deadline", "IDEM_CALLBACK_DEADLINE", "0s", "IDEM_CALLBACK_DEADLINE"},
		{"negative callback polls", "IDEM_CALLBACK_MAX_POLLS", "-1", "IDEM_CALLBACK_MAX_POLLS"},
		{"zero callback max wait", "IDEM_CALLBACK_MAX_WAIT", "0s", "IDEM_CALLBACK_MAX_WAIT"},
		{"max wait not above deadline", "IDEM_CALLBACK_MAX_WAIT", "10m", "IDEM_CALLBACK_MAX_WAIT"},
		{"poll cap below retry base", "IDEM_CALLBACK_POLL_CAP", "1s", "IDEM_CALLBACK_POLL_CAP"},
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
