package config

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/AdamDiouri/idem/internal/engine"
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
	RetryBase       time.Duration `env:"IDEM_RETRY_BASE" envDefault:"2s"`
	RetryCap        time.Duration `env:"IDEM_RETRY_CAP" envDefault:"5m"`
	// ProviderKeyRetention is how long the provider remembers idempotency keys;
	// in-doubt steps older than retention − margin are resolved by lookup.
	ProviderKeyRetention time.Duration `env:"IDEM_PROVIDER_KEY_RETENTION" envDefault:"24h"`
	KeyRetentionMargin   time.Duration `env:"IDEM_KEY_RETENTION_MARGIN" envDefault:"1h"`
	ShutdownGrace        time.Duration `env:"IDEM_SHUTDOWN_GRACE" envDefault:"15s"`
	StuckAfter           time.Duration `env:"IDEM_STUCK_AFTER" envDefault:"15m"`
	// An accepted payout waits CallbackDeadline before its first poll; polls
	// then back off (RetryBase doubling, up to CallbackPollCap). After
	// CallbackMaxWait since acceptance (or CallbackMaxPolls polls, if set) it
	// is callback_unresolved. Real bank payouts take hours or days.
	CallbackDeadline time.Duration `env:"IDEM_CALLBACK_DEADLINE" envDefault:"10m"`
	CallbackPollCap  time.Duration `env:"IDEM_CALLBACK_POLL_CAP" envDefault:"1h"`
	CallbackMaxWait  time.Duration `env:"IDEM_CALLBACK_MAX_WAIT" envDefault:"72h"`
	CallbackMaxPolls int32         `env:"IDEM_CALLBACK_MAX_POLLS" envDefault:"0"`
	// InboxOrphanAfter: a provider webhook that still matches no step this
	// long after it arrived is ignored as an orphan (and counted).
	InboxOrphanAfter time.Duration `env:"IDEM_INBOX_ORPHAN_AFTER" envDefault:"1h"`
	// InboxRetention: processed and ignored provider events older than this
	// are pruned by the sweeper; evidence and conflicts are kept.
	InboxRetention time.Duration `env:"IDEM_INBOX_RETENTION" envDefault:"720h"`
	SweepInterval  time.Duration `env:"IDEM_SWEEP_INTERVAL" envDefault:"5s"`
	WorkerID       string        `env:"IDEM_WORKER_ID"`  // empty → hostname-pid
	Failpoints     string        `env:"IDEM_FAILPOINTS"` // see failpoint.EnableFromEnv
	LogLevel       string        `env:"IDEM_LOG_LEVEL" envDefault:"info"`
	// MetricsAddr is where worker and sweeper serve /metrics ("off" disables). The
	// default stays outside 9100-9999, which Prometheus allocates to exporters
	// (node_exporter is 9100 and runs on most monitored hosts).
	MetricsAddr string `env:"IDEM_METRICS_ADDR" envDefault:":8090"`
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
	if c.RetryBase <= 0 {
		return fmt.Errorf("IDEM_RETRY_BASE must be positive")
	}
	if c.RetryCap < c.RetryBase {
		return fmt.Errorf("IDEM_RETRY_CAP (%s) must be at least IDEM_RETRY_BASE (%s)", c.RetryCap, c.RetryBase)
	}
	if c.KeyRetentionMargin <= 0 {
		return fmt.Errorf("IDEM_KEY_RETENTION_MARGIN must be positive")
	}
	if c.KeyRetentionMargin >= c.ProviderKeyRetention {
		return fmt.Errorf("IDEM_KEY_RETENTION_MARGIN (%s) must be less than IDEM_PROVIDER_KEY_RETENTION (%s)",
			c.KeyRetentionMargin, c.ProviderKeyRetention)
	}
	if c.ShutdownGrace < c.ProviderTimeout+CommitBudget {
		return fmt.Errorf("IDEM_SHUTDOWN_GRACE (%s) must be at least IDEM_PROVIDER_TIMEOUT + %s (%s)",
			c.ShutdownGrace, CommitBudget, c.ProviderTimeout+CommitBudget)
	}
	if c.StuckAfter <= 0 {
		return fmt.Errorf("IDEM_STUCK_AFTER must be positive")
	}
	if c.CallbackDeadline <= 0 {
		return fmt.Errorf("IDEM_CALLBACK_DEADLINE must be positive")
	}
	if c.CallbackMaxPolls < 0 {
		return fmt.Errorf("IDEM_CALLBACK_MAX_POLLS must not be negative (0 = no limit)")
	}
	if c.CallbackMaxWait <= c.CallbackDeadline {
		return fmt.Errorf("IDEM_CALLBACK_MAX_WAIT (%s) must be longer than IDEM_CALLBACK_DEADLINE (%s)",
			c.CallbackMaxWait, c.CallbackDeadline)
	}
	if c.InboxOrphanAfter <= 0 {
		return fmt.Errorf("IDEM_INBOX_ORPHAN_AFTER must be positive")
	}
	if c.InboxRetention <= 0 {
		return fmt.Errorf("IDEM_INBOX_RETENTION must be positive")
	}
	if c.CallbackPollCap < c.RetryBase {
		return fmt.Errorf("IDEM_CALLBACK_POLL_CAP (%s) must be at least IDEM_RETRY_BASE (%s)", c.CallbackPollCap, c.RetryBase)
	}
	if err := failpoint.ValidateSpec(c.Failpoints, engine.FailpointNames...); err != nil {
		return fmt.Errorf("IDEM_FAILPOINTS: %w", err)
	}
	if c.MetricsAddr != "off" {
		if _, _, err := net.SplitHostPort(c.MetricsAddr); err != nil {
			return fmt.Errorf("IDEM_METRICS_ADDR must be host:port or \"off\": %w", err)
		}
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
