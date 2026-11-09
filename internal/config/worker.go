package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/AdamDiouri/idem/internal/failpoint"
)

// CommitBudget is the time reserved after a provider call to commit its result.
// A healthy worker must finish call + commit inside its lease; pauses beyond
// that are handled by fencing, not by this margin.
const CommitBudget = 2 * time.Second

// WorkerConfig configures `idem worker`, `idem sweeper` and `idem check`.
type WorkerConfig struct {
	DatabaseURL     string        `env:"IDEM_DATABASE_URL,required,notEmpty"`
	ProviderURL     string        `env:"IDEM_PROVIDER_URL,required,notEmpty"`
	ProviderTimeout time.Duration `env:"IDEM_PROVIDER_TIMEOUT" envDefault:"10s"`
	LeaseTTL        time.Duration `env:"IDEM_LEASE_TTL" envDefault:"30s"`
	PollInterval    time.Duration `env:"IDEM_POLL_INTERVAL" envDefault:"500ms"`
	RetryDelay      time.Duration `env:"IDEM_RETRY_DELAY" envDefault:"2s"`
	SweepInterval   time.Duration `env:"IDEM_SWEEP_INTERVAL" envDefault:"5s"`
	WorkerID        string        `env:"IDEM_WORKER_ID"`  // empty → hostname-pid
	Failpoints      string        `env:"IDEM_FAILPOINTS"` // see failpoint.EnableFromEnv
	LogLevel        string        `env:"IDEM_LOG_LEVEL" envDefault:"info"`
}

// LoadWorker parses and validates the worker environment. It does not need API tokens.
func LoadWorker() (WorkerConfig, error) {
	cfg, err := env.ParseAs[WorkerConfig]()
	if err != nil {
		return WorkerConfig{}, fmt.Errorf("load worker config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return WorkerConfig{}, fmt.Errorf("load worker config: %w", err)
	}
	return cfg, nil
}

func (c WorkerConfig) validate() error {
	u, err := url.Parse(c.ProviderURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("IDEM_PROVIDER_URL must be an absolute http or https URL")
	}
	if c.ProviderTimeout <= 0 {
		return fmt.Errorf("IDEM_PROVIDER_TIMEOUT must be positive")
	}
	if c.ProviderTimeout+CommitBudget >= c.LeaseTTL {
		return fmt.Errorf("IDEM_PROVIDER_TIMEOUT (%s) + %s commit budget must be less than IDEM_LEASE_TTL (%s)",
			c.ProviderTimeout, CommitBudget, c.LeaseTTL)
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("IDEM_POLL_INTERVAL must be positive")
	}
	if c.SweepInterval <= 0 {
		return fmt.Errorf("IDEM_SWEEP_INTERVAL must be positive")
	}
	if c.RetryDelay < 0 {
		return fmt.Errorf("IDEM_RETRY_DELAY must not be negative")
	}
	if err := failpoint.ValidateSpec(c.Failpoints); err != nil {
		return fmt.Errorf("IDEM_FAILPOINTS: %w", err)
	}
	if _, err := c.SlogLevel(); err != nil {
		return err
	}
	return nil
}

// SlogLevel converts LogLevel to a slog.Level.
func (c WorkerConfig) SlogLevel() (slog.Level, error) {
	return parseLevel(c.LogLevel)
}
