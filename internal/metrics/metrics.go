// Package metrics defines idem's Prometheus metrics (design §15). Labels are
// bounded by construction: step names, outcome classes, statuses and reasons —
// never workflow, step or idempotency-key IDs, and never error text.
//
// All methods are safe on a nil *Metrics, which disables metrics.
package metrics

import (
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every idem metric in its own registry.
type Metrics struct {
	reg *prometheus.Registry

	StepAttempts        *prometheus.CounterVec   // step, outcome
	ProviderDuration    *prometheus.HistogramVec // step, class
	KeyLookups          *prometheus.CounterVec   // step, found
	LeasesReclaimed     prometheus.Counter
	Workflows           *prometheus.GaugeVec // status
	Steps               *prometheus.GaugeVec // step, status
	UnsettledIdle       prometheus.Gauge
	StepsOverLimit      prometheus.Gauge
	Parked              *prometheus.GaugeVec   // reason
	LastSnapshot        prometheus.Gauge       // unix time of the last successful sweeper snapshot
	Submissions         *prometheus.CounterVec // result
	StatusRequests      *prometheus.CounterVec // result
	RelayDeliveries     *prometheus.CounterVec // result
	RelayDuration       prometheus.Histogram
	CallbackPolls       *prometheus.CounterVec // step, result
	WebhooksReceived    *prometheus.CounterVec // result
	WebhooksApplied     *prometheus.CounterVec // outcome
	InboxPending        prometheus.Gauge
	InboxOldestPending  prometheus.Gauge
	OutboxPending       prometheus.Gauge
	OutboxOldestPending prometheus.Gauge
	OutboxDead          prometheus.Gauge
	ReconDiscrepancies  *prometheus.GaugeVec // kind, parked
	ReconLastSuccess    prometheus.Gauge
	ConflictsOpen       prometheus.Gauge
	CallbacksOverdue    prometheus.Gauge
}

// New registers every metric, plus the Go runtime and process collectors.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	m := &Metrics{
		reg: reg,
		StepAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "idem_step_attempts_total",
			Help: "Step attempts by outcome: succeeded, failed_permanent, in_doubt, retry_no_effect, dead, fenced, error.",
		}, []string{"step", "outcome"}),
		ProviderDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "idem_provider_request_duration_seconds",
			Help:    "Provider call latency by step and outcome class.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"step", "class"}),
		KeyLookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "idem_key_retention_lookups_total",
			Help: "In-doubt steps resolved by lookup near the provider's key retention, by whether the key was found.",
		}, []string{"step", "found"}),
		LeasesReclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "idem_leases_reclaimed_total",
			Help: "Steps whose lease expired (crashed or paused worker) and were moved to IN_DOUBT.",
		}),
		Workflows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "idem_workflows", Help: "Workflows by status.",
		}, []string{"status"}),
		Steps: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "idem_steps", Help: "Steps by name and status.",
		}, []string{"step", "status"}),
		UnsettledIdle: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "idem_unsettled_workflow_max_idle_seconds",
			Help: "Longest time any PENDING, RUNNING or COMPENSATING workflow has gone without progress (a status change or a step attempt); 0 if none.",
		}),
		StepsOverLimit: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "idem_steps_over_attempt_limit",
			Help: "Steps with more attempts than their limit (should always be 0).",
		}),
		Parked: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "idem_parked_workflows",
			Help: "Workflows in NEEDS_ATTENTION by the reason their step was parked.",
		}, []string{"reason"}),
		LastSnapshot: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "idem_sweeper_last_snapshot_timestamp_seconds",
			Help: "When the sweeper last refreshed the state gauges (stale gauges must not look healthy).",
		}),
		Submissions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "idem_workflow_submissions_total",
			Help: "POST /workflows results: created, replayed, validation_failed, key_reused, key_in_use, timeout, error.",
		}, []string{"result"}),
		StatusRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "idem_workflow_status_requests_total",
			Help: "GET /workflows/{id} results: found, not_found, invalid_id, timeout, error.",
		}, []string{"result"}),
		RelayDeliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "idem_relay_deliveries_total",
			Help: "Outbox events handled by the relay: delivered, retry, dead, no_endpoint, fenced.",
		}, []string{"result"}),
		RelayDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "idem_relay_delivery_duration_seconds",
			Help:    "How long shops took to answer a delivery (including failures).",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}),
		WebhooksReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "idem_webhooks_received_total",
			Help: "POST /webhooks/provider results: accepted, duplicate, bad_signature, stale, malformed, too_large, store_error.",
		}, []string{"result"}),
		InboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "idem_inbox_pending", Help: "Provider webhook events not processed yet.",
		}),
		InboxOldestPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "idem_inbox_oldest_pending_age_seconds", Help: "Age of the oldest unprocessed provider webhook event (0 if none).",
		}),
		OutboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "idem_outbox_pending", Help: "Workflow outcome events not delivered to their shop yet.",
		}),
		OutboxOldestPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "idem_outbox_oldest_pending_age_seconds", Help: "Age of the oldest undelivered workflow outcome event (0 if none).",
		}),
		OutboxDead: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "idem_outbox_dead", Help: "Workflow outcome events the relay gave up delivering (see idem admin outbox list --dead).",
		}),
		ReconDiscrepancies: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "idem_reconciliation_discrepancies",
			Help: "Open differences between the provider's records and the ledger (not resolved by a later reconciliation covering them), by kind and whether the workflow is parked.",
		}, []string{"kind", "parked"}),
		ReconLastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "idem_reconciliation_last_success_timestamp_seconds",
			Help: "When the latest successful reconciliation finished (0 if none ever did).",
		}),
		ConflictsOpen: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "idem_webhook_conflicts_open", Help: "Provider news that truly contradicts idem, not yet resolved by a human.",
		}),
		CallbacksOverdue: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "idem_callbacks_overdue", Help: "Waiting payouts whose poll is more than 5 minutes overdue.",
		}),
		WebhooksApplied: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "idem_webhooks_applied_total",
			Help: "Inbox events processed by outcome: applied, noop, deferred, orphaned, stale, evidence, conflict, malformed, fenced.",
		}, []string{"outcome"}),
		CallbackPolls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "idem_callback_polls_total",
			Help: "Status polls of accepted async requests (payouts) by result: paid, failed, pending, error.",
		}, []string{"step", "result"}),
	}
	for _, c := range []prometheus.Collector{
		m.StepAttempts, m.ProviderDuration, m.KeyLookups, m.LeasesReclaimed, m.Workflows, m.Steps,
		m.UnsettledIdle, m.StepsOverLimit, m.Parked, m.LastSnapshot, m.Submissions, m.StatusRequests, m.RelayDeliveries, m.RelayDuration, m.CallbackPolls, m.WebhooksReceived, m.WebhooksApplied,
		m.InboxPending, m.InboxOldestPending, m.OutboxPending, m.OutboxOldestPending, m.OutboxDead, m.ReconDiscrepancies, m.ReconLastSuccess, m.ConflictsOpen, m.CallbacksOverdue,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	} {
		f(c)
	}
	return m
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// InternalHandler serves /metrics (when m is set) and Go's profiling endpoints
// under /debug/pprof/ (CPU, heap, goroutines, mutex, block, execution trace).
// It belongs on an internal-only listener: profiles reveal internals, and a
// CPU profile costs the process while it runs.
func InternalHandler(m *Metrics) http.Handler {
	mux := http.NewServeMux()
	if m != nil {
		mux.Handle("GET /metrics", m.Handler())
	}
	mux.HandleFunc("GET /debug/pprof/", pprof.Index) // also serves heap, goroutine, block, mutex, allocs
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return mux
}

// Gatherer exposes the registry (for tests and checks).
func (m *Metrics) Gatherer() prometheus.Gatherer { return m.reg }

// ObserveAttempt counts one step attempt's outcome.
func (m *Metrics) ObserveAttempt(step, outcome string) {
	if m == nil {
		return
	}
	m.StepAttempts.WithLabelValues(step, outcome).Inc()
}

// ObserveProvider records a provider call's latency.
func (m *Metrics) ObserveProvider(step, class string, d time.Duration) {
	if m == nil {
		return
	}
	m.ProviderDuration.WithLabelValues(step, class).Observe(d.Seconds())
}

// ObserveLookup counts a near-retention lookup.
func (m *Metrics) ObserveLookup(step string, found bool) {
	if m == nil {
		return
	}
	v := "false"
	if found {
		v = "true"
	}
	m.KeyLookups.WithLabelValues(step, v).Inc()
}

// AddReclaimed counts reclaimed leases.
func (m *Metrics) AddReclaimed(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.LeasesReclaimed.Add(float64(n))
}

// Snapshot is the database state the sweeper exports as gauges.
type Snapshot struct {
	Workflows      map[string]int    // status → count
	Steps          map[[2]string]int // {step, status} → count
	UnsettledIdle  time.Duration
	StepsOverLimit int
	Parked         map[string]int // reason → count

	InboxPending       int
	InboxOldestPending time.Duration
	ConflictsOpen      int
	CallbacksOverdue   int

	OutboxPending       int
	OutboxOldestPending time.Duration
	OutboxDead          int

	ReconDiscrepancies map[[2]string]int // {kind, parked} → count of open discrepancies
	ReconLastSuccess   time.Time         // zero if none
}

// SetSnapshot replaces every state gauge, so label values that disappeared
// (e.g. a resolved parking reason) stop being exported.
func (m *Metrics) SetSnapshot(s Snapshot) {
	if m == nil {
		return
	}
	m.Workflows.Reset()
	for status, n := range s.Workflows {
		m.Workflows.WithLabelValues(status).Set(float64(n))
	}
	m.Steps.Reset()
	for k, n := range s.Steps {
		m.Steps.WithLabelValues(k[0], k[1]).Set(float64(n))
	}
	m.UnsettledIdle.Set(s.UnsettledIdle.Seconds())
	m.StepsOverLimit.Set(float64(s.StepsOverLimit))
	m.Parked.Reset()
	for reason, n := range s.Parked {
		m.Parked.WithLabelValues(reason).Set(float64(n))
	}
	m.InboxPending.Set(float64(s.InboxPending))
	m.InboxOldestPending.Set(s.InboxOldestPending.Seconds())
	m.OutboxPending.Set(float64(s.OutboxPending))
	m.OutboxOldestPending.Set(s.OutboxOldestPending.Seconds())
	m.OutboxDead.Set(float64(s.OutboxDead))
	m.ReconDiscrepancies.Reset()
	for k, n := range s.ReconDiscrepancies {
		m.ReconDiscrepancies.WithLabelValues(k[0], k[1]).Set(float64(n))
	}
	m.ReconLastSuccess.Set(0)
	if !s.ReconLastSuccess.IsZero() {
		m.ReconLastSuccess.Set(float64(s.ReconLastSuccess.Unix()))
	}
	m.ConflictsOpen.Set(float64(s.ConflictsOpen))
	m.CallbacksOverdue.Set(float64(s.CallbacksOverdue))
	m.LastSnapshot.SetToCurrentTime()
}

// ObservePoll counts one status poll of an accepted async request.
func (m *Metrics) ObservePoll(step, result string) {
	if m == nil {
		return
	}
	m.CallbackPolls.WithLabelValues(step, result).Inc()
}

// ObserveInbox counts one processed inbox event.
func (m *Metrics) ObserveInbox(outcome string) {
	if m == nil || outcome == "" {
		return
	}
	m.WebhooksApplied.WithLabelValues(outcome).Inc()
}

// ObserveWebhook counts one POST /webhooks/provider result.
func (m *Metrics) ObserveWebhook(result string) {
	if m == nil {
		return
	}
	m.WebhooksReceived.WithLabelValues(result).Inc()
}

// ObserveRelay counts one relay outcome and, for an attempted delivery, how
// long the shop took (d < 0: no request was made).
func (m *Metrics) ObserveRelay(result string, d time.Duration) {
	if m == nil {
		return
	}
	m.RelayDeliveries.WithLabelValues(result).Inc()
	if d >= 0 {
		m.RelayDuration.Observe(d.Seconds())
	}
}

// ObserveStatusRequest counts one GET /workflows/{id} by result.
func (m *Metrics) ObserveStatusRequest(result string) {
	if m == nil {
		return
	}
	m.StatusRequests.WithLabelValues(result).Inc()
}

// ObserveSubmission counts a POST /workflows result.
func (m *Metrics) ObserveSubmission(result string) {
	if m == nil {
		return
	}
	m.Submissions.WithLabelValues(result).Inc()
}
