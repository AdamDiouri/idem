package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/config"
)

func setReconRequired(t *testing.T) {
	t.Helper()
	t.Setenv("IDEM_DATABASE_URL", "postgres://idem:idem@localhost:5432/idem")
	t.Setenv("IDEM_PROVIDER_URL", "http://localhost:9090")
	for _, k := range []string{"IDEM_PROVIDER_TIMEOUT", "IDEM_RECON_LAG", "IDEM_RECON_MARGIN", "IDEM_RECON_TIMEOUT", "IDEM_LOG_LEVEL"} {
		t.Setenv(k, "")
	}
}

// Slice 2.6.
func TestLoadRecon_DefaultsAndRejects(t *testing.T) {
	setReconRequired(t)
	cfg, err := config.LoadRecon()
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, cfg.Lag)
	require.Equal(t, 24*time.Hour, cfg.Margin)
	require.Equal(t, 10*time.Minute, cfg.Timeout)
	require.Equal(t, 10*time.Second, cfg.ProviderTimeout)

	for k, v := range map[string]string{"IDEM_PROVIDER_URL": "not a url", "IDEM_RECON_LAG": "-1s", "IDEM_RECON_MARGIN": "0s", "IDEM_PROVIDER_TIMEOUT": "0s", "IDEM_RECON_TIMEOUT": "0s"} {
		t.Run(k, func(t *testing.T) {
			setReconRequired(t)
			t.Setenv(k, v)
			_, err := config.LoadRecon()
			require.ErrorContains(t, err, k)
		})
	}
}
