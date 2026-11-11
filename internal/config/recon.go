package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/caarlos0/env/v11"
)

// ReconConfig configures `idem reconcile` (M2 design §6).
type ReconConfig struct {
	DatabaseURL     string        `env:"IDEM_DATABASE_URL,required,notEmpty"`
	ProviderURL     string        `env:"IDEM_PROVIDER_URL,required,notEmpty"`
	ProviderTimeout time.Duration `env:"IDEM_PROVIDER_TIMEOUT" envDefault:"10s"`
	// Lag: the default window is the last complete hour ending this long ago,
	// leaving the provider's report time to catch up.
	Lag time.Duration `env:"IDEM_RECON_LAG" envDefault:"5m"`
	// Margin is how far before the window the provider's report is read: a
	// posting in the window may follow its movement by up to this long (a
	// payout settles before the webhook or a poll lets idem post it).
	Margin time.Duration `env:"IDEM_RECON_MARGIN" envDefault:"24h"`
	// Timeout bounds one run, so a provider that hangs cannot stall it.
	Timeout  time.Duration `env:"IDEM_RECON_TIMEOUT" envDefault:"10m"`
	LogLevel string        `env:"IDEM_LOG_LEVEL" envDefault:"info"`
}

// LoadRecon reads and validates the reconciliation job's configuration.
func LoadRecon() (ReconConfig, error) {
	cfg, err := env.ParseAs[ReconConfig]()
	if err != nil {
		return ReconConfig{}, fmt.Errorf("load reconcile config: %w", err)
	}
	u, err := url.Parse(cfg.ProviderURL)
	switch {
	case err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
		return ReconConfig{}, fmt.Errorf("load reconcile config: IDEM_PROVIDER_URL must be an absolute http or https URL")
	case cfg.ProviderTimeout <= 0:
		return ReconConfig{}, fmt.Errorf("load reconcile config: IDEM_PROVIDER_TIMEOUT must be positive")
	case cfg.Lag < 0:
		return ReconConfig{}, fmt.Errorf("load reconcile config: IDEM_RECON_LAG must not be negative")
	case cfg.Margin <= 0:
		return ReconConfig{}, fmt.Errorf("load reconcile config: IDEM_RECON_MARGIN must be positive")
	case cfg.Timeout <= 0:
		return ReconConfig{}, fmt.Errorf("load reconcile config: IDEM_RECON_TIMEOUT must be positive")
	}
	if _, err := parseLevel(cfg.LogLevel); err != nil {
		return ReconConfig{}, err
	}
	return cfg, nil
}

// SlogLevel returns the configured log level.
func (c ReconConfig) SlogLevel() (slog.Level, error) { return parseLevel(c.LogLevel) }
