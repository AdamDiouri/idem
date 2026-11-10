// Package config loads idem's configuration from IDEM_* environment variables.
package config

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/AdamDiouri/idem/internal/api"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/workflow"
)

const minTokenLength = 16

// Config is the runtime configuration for idem.
type Config struct {
	DatabaseURL            string            `env:"IDEM_DATABASE_URL,required,notEmpty"`
	HTTPAddr               string            `env:"IDEM_HTTP_ADDR" envDefault:":8080"`
	APITokens              map[string]string `env:"IDEM_API_TOKENS,required,notEmpty" envKeyValSeparator:"="` // client=token,...
	IdempotencyLockTimeout time.Duration     `env:"IDEM_IDEMPOTENCY_LOCK_TIMEOUT" envDefault:"5s"`
	RequestTimeout         time.Duration     `env:"IDEM_REQUEST_TIMEOUT" envDefault:"10s"`
	HTTPWriteTimeout       time.Duration     `env:"IDEM_HTTP_WRITE_TIMEOUT" envDefault:"15s"`
	FeeRateBps             int64             `env:"IDEM_FEE_RATE_BPS" envDefault:"150"`
	FeeFixedCents          int64             `env:"IDEM_FEE_FIXED_CENTS" envDefault:"25"`
	LogLevel               string            `env:"IDEM_LOG_LEVEL" envDefault:"info"`
	// ProviderWebhookSecrets verify provider webhooks: the current secret
	// first, then the previous one during a rotation. Empty: endpoint off.
	ProviderWebhookSecrets []string `env:"IDEM_PROVIDER_WEBHOOK_SECRETS"`
	Failpoints             string   `env:"IDEM_FAILPOINTS"` // see failpoint.EnableFromEnv
}

// Load parses and validates the environment. Errors name the offending variable.
func Load() (Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	return cfg, nil
}

func (c Config) validate() error {
	seen := make(map[string]bool, len(c.APITokens))
	for client, token := range c.APITokens {
		if client == "" {
			return fmt.Errorf("IDEM_API_TOKENS: empty client id")
		}
		if len(token) < minTokenLength {
			return fmt.Errorf("IDEM_API_TOKENS: token for client %q is shorter than %d characters", client, minTokenLength)
		}
		if seen[token] {
			return fmt.Errorf("IDEM_API_TOKENS: token for client %q is shared with another client", client)
		}
		seen[token] = true
	}

	if c.IdempotencyLockTimeout <= 0 {
		return fmt.Errorf("IDEM_IDEMPOTENCY_LOCK_TIMEOUT must be positive")
	}
	// lock wait < whole-request deadline < server write deadline, so a client
	// always gets a status (409 or 503) instead of a dropped connection.
	if c.IdempotencyLockTimeout >= c.RequestTimeout {
		return fmt.Errorf("IDEM_IDEMPOTENCY_LOCK_TIMEOUT (%s) must be less than IDEM_REQUEST_TIMEOUT (%s)",
			c.IdempotencyLockTimeout, c.RequestTimeout)
	}
	if c.RequestTimeout >= c.HTTPWriteTimeout {
		return fmt.Errorf("IDEM_REQUEST_TIMEOUT (%s) must be less than IDEM_HTTP_WRITE_TIMEOUT (%s)",
			c.RequestTimeout, c.HTTPWriteTimeout)
	}

	if c.FeeRateBps < 0 || c.FeeRateBps > workflow.MaxFeeRateBps {
		return fmt.Errorf("IDEM_FEE_RATE_BPS must be between 0 and %d", workflow.MaxFeeRateBps)
	}
	if c.FeeFixedCents < 0 || c.FeeFixedCents > workflow.MaxAmountCents {
		return fmt.Errorf("IDEM_FEE_FIXED_CENTS must be between 0 and %d", workflow.MaxAmountCents)
	}

	for _, s := range c.ProviderWebhookSecrets {
		if len(s) < minTokenLength {
			return fmt.Errorf("IDEM_PROVIDER_WEBHOOK_SECRETS: a secret is shorter than %d characters", minTokenLength)
		}
	}
	if err := failpoint.ValidateSpec(c.Failpoints, api.FailpointNames...); err != nil {
		return fmt.Errorf("IDEM_FAILPOINTS: %w", err)
	}
	if _, err := c.SlogLevel(); err != nil {
		return err
	}
	return nil
}

// TokenToClient inverts APITokens into bearer token → client ID.
func (c Config) TokenToClient() map[string]string {
	out := make(map[string]string, len(c.APITokens))
	for client, token := range c.APITokens {
		out[token] = client
	}
	return out
}

// SlogLevel converts LogLevel to a slog.Level.
func (c Config) SlogLevel() (slog.Level, error) {
	return parseLevel(c.LogLevel)
}

func parseLevel(level string) (slog.Level, error) {
	switch level {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("IDEM_LOG_LEVEL must be one of debug, info, warn, error")
}
