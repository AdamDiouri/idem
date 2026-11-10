package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/config"
)

const (
	tokA = "tok-aaaaaaaaaaaaaaaa"
	tokB = "tok-bbbbbbbbbbbbbbbb"
)

// setRequired sets the required variables and clears every optional one so
// the host environment cannot leak into a test.
func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("IDEM_DATABASE_URL", "postgres://idem:idem@localhost:5432/idem")
	t.Setenv("IDEM_API_TOKENS", "a="+tokA)
	for _, k := range []string{
		"IDEM_HTTP_ADDR", "IDEM_IDEMPOTENCY_LOCK_TIMEOUT", "IDEM_REQUEST_TIMEOUT", "IDEM_HTTP_WRITE_TIMEOUT",
		"IDEM_FEE_RATE_BPS", "IDEM_FEE_FIXED_CENTS", "IDEM_LOG_LEVEL",
		"IDEM_PROVIDER_WEBHOOK_SECRETS", "IDEM_FAILPOINTS",
	} {
		t.Setenv(k, "")
	}
}

func TestLoad_Defaults(t *testing.T) {
	setRequired(t)
	cfg, err := config.Load()
	require.NoError(t, err)
	require.Equal(t, ":8080", cfg.HTTPAddr)
	require.Equal(t, 5*time.Second, cfg.IdempotencyLockTimeout)
	require.Equal(t, 15*time.Second, cfg.HTTPWriteTimeout)
	require.Equal(t, 10*time.Second, cfg.RequestTimeout)
	require.Equal(t, int64(150), cfg.FeeRateBps)
	require.Equal(t, int64(25), cfg.FeeFixedCents)
	require.Equal(t, "info", cfg.LogLevel)
}

func TestLoad_ParsesTokens(t *testing.T) {
	setRequired(t)
	t.Setenv("IDEM_API_TOKENS", "a="+tokA+",b="+tokB)
	cfg, err := config.Load()
	require.NoError(t, err)
	require.Equal(t, map[string]string{tokA: "a", tokB: "b"}, cfg.TokenToClient())
}

func TestLoad_Invalid(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		value   string
		wantVar string
	}{
		{"missing database url", "IDEM_DATABASE_URL", "", "IDEM_DATABASE_URL"},
		{"missing tokens", "IDEM_API_TOKENS", "", "IDEM_API_TOKENS"},
		{"short token", "IDEM_API_TOKENS", "a=short", "IDEM_API_TOKENS"},
		{"duplicate token", "IDEM_API_TOKENS", "a=" + tokA + ",b=" + tokA, "IDEM_API_TOKENS"},
		{"lock timeout not below write timeout", "IDEM_IDEMPOTENCY_LOCK_TIMEOUT", "20s", "IDEM_IDEMPOTENCY_LOCK_TIMEOUT"},
		{"zero lock timeout", "IDEM_IDEMPOTENCY_LOCK_TIMEOUT", "0s", "IDEM_IDEMPOTENCY_LOCK_TIMEOUT"},
		{"lock timeout not below request timeout", "IDEM_IDEMPOTENCY_LOCK_TIMEOUT", "10s", "IDEM_IDEMPOTENCY_LOCK_TIMEOUT"},
		{"request timeout not below write timeout", "IDEM_REQUEST_TIMEOUT", "15s", "IDEM_REQUEST_TIMEOUT"},
		{"fee rate too high", "IDEM_FEE_RATE_BPS", "10001", "IDEM_FEE_RATE_BPS"},
		{"negative fee rate", "IDEM_FEE_RATE_BPS", "-1", "IDEM_FEE_RATE_BPS"},
		{"negative fixed fee", "IDEM_FEE_FIXED_CENTS", "-1", "IDEM_FEE_FIXED_CENTS"},
		{"fixed fee above max amount", "IDEM_FEE_FIXED_CENTS", "100000001", "IDEM_FEE_FIXED_CENTS"},
		{"fixed fee near int64 max", "IDEM_FEE_FIXED_CENTS", "9223372036854775807", "IDEM_FEE_FIXED_CENTS"},
		{"unknown log level", "IDEM_LOG_LEVEL", "loud", "IDEM_LOG_LEVEL"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setRequired(t)
			t.Setenv(c.key, c.value)
			_, err := config.Load()
			require.Error(t, err)
			require.Contains(t, err.Error(), c.wantVar)
		})
	}
}

// Slice 2.2: webhook secrets (current first, then previous) and API failpoints.
func TestLoad_WebhookSecretsAndFailpoints(t *testing.T) {
	setRequired(t)
	cfg, err := config.Load()
	require.NoError(t, err)
	require.Empty(t, cfg.ProviderWebhookSecrets, "off unless configured")

	t.Setenv("IDEM_PROVIDER_WEBHOOK_SECRETS", "whsec_new_0123456789,whsec_old_0123456789")
	cfg, err = config.Load()
	require.NoError(t, err)
	require.Equal(t, []string{"whsec_new_0123456789", "whsec_old_0123456789"}, cfg.ProviderWebhookSecrets)

	t.Setenv("IDEM_FAILPOINTS", "webhook_after_insert=exit")
	_, err = config.Load()
	require.NoError(t, err)
	t.Setenv("IDEM_FAILPOINTS", "nope=exit")
	_, err = config.Load()
	require.ErrorContains(t, err, "IDEM_FAILPOINTS")
}

// Minor fix: secrets are trimmed, and an empty entry is refused, so
// "a, b" does not silently keep a secret that can never match.
func TestLoad_WebhookSecretsTrimmed(t *testing.T) {
	setRequired(t)
	t.Setenv("IDEM_PROVIDER_WEBHOOK_SECRETS", " whsec_new_0123456789 , whsec_old_0123456789")
	cfg, err := config.Load()
	require.NoError(t, err)
	require.Equal(t, []string{"whsec_new_0123456789", "whsec_old_0123456789"}, cfg.ProviderWebhookSecrets)
	t.Setenv("IDEM_PROVIDER_WEBHOOK_SECRETS", "whsec_new_0123456789,,")
	_, err = config.Load()
	require.ErrorContains(t, err, "IDEM_PROVIDER_WEBHOOK_SECRETS")
}
