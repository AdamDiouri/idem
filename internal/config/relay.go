package config

import (
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/relay"
)

// minWebhookSecret is the shortest client webhook secret accepted.
const minWebhookSecret = 16

// RelayConfig configures `idem relay` (M2 design §5.3). Client URLs and
// secrets are set by the operator only, never through the API, so no request
// can make idem call an arbitrary address.
type RelayConfig struct {
	DatabaseURL string `env:"IDEM_DATABASE_URL,required,notEmpty"`
	// IDEM_CLIENT_WEBHOOKS ("client=url,...") and IDEM_CLIENT_WEBHOOK_SECRETS
	// ("client=secret,..."), parsed by parseClientList: trimmed, and with
	// errors that never quote a value (a secret must not reach the logs).
	ClientWebhooks       map[string]string `env:"-"`
	ClientWebhookSecrets map[string]string `env:"-"`
	LeaseTTL             time.Duration     `env:"IDEM_RELAY_LEASE_TTL" envDefault:"30s"`
	Timeout              time.Duration     `env:"IDEM_RELAY_TIMEOUT" envDefault:"10s"`
	// 15 attempts with full jitter from 30s, capped at 6h: about 16 hours on
	// average before DEAD, at most about 32 ("roughly a day").
	MaxAttempts  int32         `env:"IDEM_RELAY_MAX_ATTEMPTS" envDefault:"15"`
	RetryBase    time.Duration `env:"IDEM_RELAY_RETRY_BASE" envDefault:"30s"`
	RetryCap     time.Duration `env:"IDEM_RELAY_RETRY_CAP" envDefault:"6h"`
	PollInterval time.Duration `env:"IDEM_POLL_INTERVAL" envDefault:"500ms"`
	// Deliveries at once per relay, and at most PerClient of them for one
	// client: a shop that hangs to the timeout cannot delay the others.
	Concurrency   int           `env:"IDEM_RELAY_CONCURRENCY" envDefault:"4"`
	PerClient     int           `env:"IDEM_RELAY_PER_CLIENT" envDefault:"2"`
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
	if cfg.ClientWebhooks, err = parseClientList("IDEM_CLIENT_WEBHOOKS"); err != nil {
		return RelayConfig{}, fmt.Errorf("load relay config: %w", err)
	}
	if cfg.ClientWebhookSecrets, err = parseClientList("IDEM_CLIENT_WEBHOOK_SECRETS"); err != nil {
		return RelayConfig{}, fmt.Errorf("load relay config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return RelayConfig{}, fmt.Errorf("load relay config: %w", err)
	}
	return cfg, nil
}

// parseClientList reads "client=value,..." from env: spaces around IDs and
// values are trimmed (a stray space must not create a client " b" whose
// events silently become NO_ENDPOINT), IDs may not be empty, contain spaces
// or repeat, and values may not contain commas. Errors name the variable,
// the entry's position and the client, never the value.
func parseClientList(env string) (map[string]string, error) {
	out := map[string]string{}
	raw := strings.TrimSpace(os.Getenv(env))
	if raw == "" {
		return out, nil
	}
	for i, entry := range strings.Split(raw, ",") {
		client, value, ok := strings.Cut(entry, "=")
		client, value = strings.TrimSpace(client), strings.TrimSpace(value)
		switch {
		case !ok:
			return nil, fmt.Errorf("%s entry %d is not client=value (values may not contain commas)", env, i+1)
		case client == "" || strings.ContainsAny(client, " \t"):
			return nil, fmt.Errorf("%s entry %d: client IDs may not be empty or contain spaces", env, i+1)
		case value == "":
			return nil, fmt.Errorf("%s: client %q has an empty value", env, client)
		}
		if _, dup := out[client]; dup {
			return nil, fmt.Errorf("%s lists client %q more than once", env, client)
		}
		out[client] = value
	}
	return out, nil
}

func (c RelayConfig) validate() error {
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
	case c.Concurrency < 1 || c.Concurrency > 64:
		return fmt.Errorf("IDEM_RELAY_CONCURRENCY must be between 1 and 64")
	case c.PerClient < 1 || c.PerClient > c.Concurrency:
		return fmt.Errorf("IDEM_RELAY_PER_CLIENT must be between 1 and IDEM_RELAY_CONCURRENCY (%d)", c.Concurrency)
	}
	if _, err := parseLevel(c.LogLevel); err != nil {
		return err
	}
	return failpoint.ValidateSpec(c.Failpoints, relay.FailpointNames...)
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
