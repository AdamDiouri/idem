package config

import (
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/AdamDiouri/idem/internal/failpoint"
)

// minWebhookSecret is the shortest client webhook secret accepted.
const minWebhookSecret = 16

// relayFailpoints are the failpoints `idem relay` may arm.
var relayFailpoints = []string{"relay_before_send", "relay_after_send"}

// RelayConfig configures `idem relay` (M2 design §5.3). Client URLs and
// secrets are set by the operator only, never through the API, so no request
// can make idem call an arbitrary address.
type RelayConfig struct {
	DatabaseURL          string            `env:"IDEM_DATABASE_URL,required,notEmpty"`
	ClientWebhooks       map[string]string `env:"IDEM_CLIENT_WEBHOOKS" envKeyValSeparator:"="`        // client=url,...
	ClientWebhookSecrets map[string]string `env:"IDEM_CLIENT_WEBHOOK_SECRETS" envKeyValSeparator:"="` // client=secret,...
	LeaseTTL             time.Duration     `env:"IDEM_RELAY_LEASE_TTL" envDefault:"30s"`
	Timeout              time.Duration     `env:"IDEM_RELAY_TIMEOUT" envDefault:"10s"`
	// 15 attempts with full jitter from 30s, capped at 6h: about 16 hours on
	// average before DEAD, at most about 32 ("roughly a day").
	MaxAttempts   int32         `env:"IDEM_RELAY_MAX_ATTEMPTS" envDefault:"15"`
	RetryBase     time.Duration `env:"IDEM_RELAY_RETRY_BASE" envDefault:"30s"`
	RetryCap      time.Duration `env:"IDEM_RELAY_RETRY_CAP" envDefault:"6h"`
	PollInterval  time.Duration `env:"IDEM_POLL_INTERVAL" envDefault:"500ms"`
	ShutdownGrace time.Duration `env:"IDEM_SHUTDOWN_GRACE" envDefault:"15s"`
	WorkerID      string        `env:"IDEM_WORKER_ID"`  // empty → hostname-pid
	Failpoints    string        `env:"IDEM_FAILPOINTS"` // see failpoint.EnableFromEnv
	LogLevel      string        `env:"IDEM_LOG_LEVEL" envDefault:"info"`
	MetricsAddr   string        `env:"IDEM_METRICS_ADDR" envDefault:":8090"` // "off" disables
}

// LoadRelay reads and validates the relay's configuration.
func LoadRelay() (RelayConfig, error) {
	cfg, err := env.ParseAs[RelayConfig]()
	if err != nil {
		return RelayConfig{}, fmt.Errorf("load relay config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return RelayConfig{}, fmt.Errorf("load relay config: %w", err)
	}
	return cfg, nil
}

func (c RelayConfig) validate() error {
	for _, v := range []string{"IDEM_CLIENT_WEBHOOKS", "IDEM_CLIENT_WEBHOOK_SECRETS"} {
		if dups := duplicateClientIDs(os.Getenv(v)); len(dups) > 0 {
			return fmt.Errorf("%s lists client %q more than once", v, dups[0])
		}
	}
	var errs []error
	for _, client := range slices.Sorted(keys(c.ClientWebhooks, c.ClientWebhookSecrets)) {
		raw, hasURL := c.ClientWebhooks[client]
		secret, hasSecret := c.ClientWebhookSecrets[client]
		switch {
		case !hasSecret:
			errs = append(errs, fmt.Errorf("client %q has a webhook URL but no secret in IDEM_CLIENT_WEBHOOK_SECRETS", client))
		case !hasURL:
			errs = append(errs, fmt.Errorf("client %q has a webhook secret but no URL in IDEM_CLIENT_WEBHOOKS", client))
		case len(secret) < minWebhookSecret:
			errs = append(errs, fmt.Errorf("client %q: webhook secret must be at least %d characters", client, minWebhookSecret))
		default:
			u, err := url.Parse(raw)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				errs = append(errs, fmt.Errorf("client %q: webhook URL must be an absolute http or https URL", client))
			}
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	switch {
	case c.Timeout <= 0:
		return fmt.Errorf("IDEM_RELAY_TIMEOUT must be positive")
	case c.Timeout+CommitBudget >= c.LeaseTTL:
		return fmt.Errorf("IDEM_RELAY_TIMEOUT (%s) + %s commit budget must be less than IDEM_RELAY_LEASE_TTL (%s)",
			c.Timeout, CommitBudget, c.LeaseTTL)
	case c.ShutdownGrace < c.Timeout+CommitBudget:
		return fmt.Errorf("IDEM_SHUTDOWN_GRACE (%s) must be at least IDEM_RELAY_TIMEOUT + %s (%s)",
			c.ShutdownGrace, CommitBudget, c.Timeout+CommitBudget)
	case c.MaxAttempts < 1:
		return fmt.Errorf("IDEM_RELAY_MAX_ATTEMPTS must be at least 1")
	case c.RetryBase <= 0:
		return fmt.Errorf("IDEM_RELAY_RETRY_BASE must be positive")
	case c.RetryCap < c.RetryBase:
		return fmt.Errorf("IDEM_RELAY_RETRY_CAP (%s) must be at least IDEM_RELAY_RETRY_BASE (%s)", c.RetryCap, c.RetryBase)
	case c.PollInterval <= 0:
		return fmt.Errorf("IDEM_POLL_INTERVAL must be positive")
	}
	if _, err := parseLevel(c.LogLevel); err != nil {
		return err
	}
	return failpoint.ValidateSpec(c.Failpoints, relayFailpoints...)
}

// SlogLevel returns the configured log level.
func (c RelayConfig) SlogLevel() (slog.Level, error) { return parseLevel(c.LogLevel) }

// keys returns the union of the maps' keys.
func keys(maps ...map[string]string) iter.Seq[string] {
	return func(yield func(string) bool) {
		seen := map[string]bool{}
		for _, m := range maps {
			for k := range m {
				if seen[k] {
					continue
				}
				seen[k] = true
				if !yield(k) {
					return
				}
			}
		}
	}
}
