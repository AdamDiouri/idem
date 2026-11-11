// Package relay delivers outbox events to shops (M2 design §5.3, D14): each
// workflow outcome, signed with the shop's secret, at least once. A relay
// that crashes after the shop answered but before recording it sends the
// event again; the shop deduplicates on Idem-Event-Id.
package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/engine"
	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/metrics"
	"github.com/AdamDiouri/idem/internal/store/db"
	"github.com/AdamDiouri/idem/internal/webhook"
)

// Headers of a delivery.
const (
	EventIDHeader   = "Idem-Event-Id"
	EventTypeHeader = "Idem-Event-Type"
	SignatureHeader = "Idem-Signature"
)

// Failpoints the relay hits.
const (
	FPBeforeSend = "relay_before_send" // claimed, nothing sent yet
	FPAfterSend  = "relay_after_send"  // the shop answered; the result is not recorded yet (F32)
)

// FailpointNames lists every failpoint the relay hits, for config validation.
var FailpointNames = []string{FPBeforeSend, FPAfterSend}

// Outcomes of one RunOnce (also idem_relay_deliveries_total{result}).
const (
	OutcomeDelivered  = "delivered"
	OutcomeRetry      = "retry"
	OutcomeDead       = "dead"
	OutcomeNoEndpoint = "no_endpoint"
	OutcomeFenced     = "fenced"
)

// maxResponseRead bounds how much of a shop's answer is read: only its status
// matters, and a shop must not be able to make idem read without end.
const maxResponseRead = 4 << 10

// maxErrorLen bounds the stored last_error.
const maxErrorLen = 500

// Endpoint is where, and with which secret, a client receives its events.
type Endpoint struct {
	URL    string
	Secret []byte
}

// Options configures a Relay.
type Options struct {
	ID            string
	Clients       map[string]Endpoint // client ID → endpoint; a client without one gets NO_ENDPOINT (D19)
	LeaseTTL      time.Duration
	Timeout       time.Duration // per delivery; must leave room for the commit inside the lease
	PollInterval  time.Duration
	ShutdownGrace time.Duration
	MaxAttempts   int32
	Retry         engine.Backoff
	Metrics       *metrics.Metrics
	Logger        *slog.Logger
}

// Result is what one RunOnce did.
type Result struct {
	Claimed bool
	EventID uuid.UUID
	Outcome string
}

// Relay claims due outbox events and delivers them.
type Relay struct {
	pool   *pgxpool.Pool
	opts   Options
	client *http.Client
	logger *slog.Logger
}

// New returns a relay for opts.
func New(pool *pgxpool.Pool, opts Options) *Relay {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Relay{
		pool: pool,
		opts: opts,
		client: &http.Client{
			Timeout: opts.Timeout,
			// A redirect is a failed attempt: a shop must not be able to point
			// idem at another host (the URL is operator-set for a reason).
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		logger: logger.With("relay_id", opts.ID),
	}
}

// Run delivers events until ctx is cancelled. A delivery in flight when ctx
// ends may finish within ShutdownGrace; an interrupted one is retried after
// its lease expires.
func (r *Relay) Run(ctx context.Context) error {
	r.logger.Info("relay started", "clients", len(r.opts.Clients))
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	stop := context.AfterFunc(ctx, func() { time.AfterFunc(r.opts.ShutdownGrace, cancelWork) })
	defer stop()

	for ctx.Err() == nil {
		res, err := r.RunOnce(workCtx)
		if err != nil {
			r.logger.Error("relay event", "event_id", res.EventID, "error", err)
		}
		if err != nil || !res.Claimed {
			select {
			case <-ctx.Done():
			case <-time.After(r.opts.PollInterval):
			}
		}
	}
	r.logger.Info("relay stopped")
	return nil
}

// RunOnce claims one due event and delivers it.
func (r *Relay) RunOnce(ctx context.Context) (Result, error) {
	q := db.New(r.pool)
	ev, err := q.ClaimOutboxEvent(ctx, db.ClaimOutboxEventParams{Owner: r.opts.ID, LeaseMs: r.opts.LeaseTTL.Milliseconds()})
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{}, nil
	}
	if err != nil {
		return Result{}, fmt.Errorf("claim outbox event: %w", err)
	}
	res := Result{Claimed: true, EventID: ev.ID}
	log := r.logger.With("event_id", ev.ID, "workflow_id", ev.WorkflowID, "client_id", ev.ClientID, "attempt", ev.Attempts)

	ep, ok := r.opts.Clients[ev.ClientID]
	if !ok {
		res.Outcome, err = r.finish(ctx, q, ev, "NO_ENDPOINT", "", 0, log)
		r.opts.Metrics.ObserveRelay(res.Outcome, -1)
		return res, err
	}

	failpoint.Hit(FPBeforeSend)
	start := time.Now()
	sendErr := r.send(ctx, ep, ev)
	took := time.Since(start)
	failpoint.Hit(FPAfterSend)

	switch {
	case sendErr == nil:
		res.Outcome, err = r.finish(ctx, q, ev, "DELIVERED", "", 0, log)
	case ev.Attempts >= r.opts.MaxAttempts:
		log.Error("event delivery gave up; the shop never got it", "error", sendErr)
		res.Outcome, err = r.finish(ctx, q, ev, "DEAD", sendErr.Error(), 0, log)
	default:
		delay := r.opts.Retry.Delay(ev.Attempts)
		log.Warn("event delivery failed; will retry", "error", sendErr, "retry_in", delay.String())
		res.Outcome, err = r.finish(ctx, q, ev, "PENDING", sendErr.Error(), delay, log)
	}
	r.opts.Metrics.ObserveRelay(res.Outcome, took)
	return res, err
}

// send POSTs the event; nil means the shop answered 2xx in time.
func (r *Relay) send(ctx context.Context, ep Endpoint, ev db.ClaimOutboxEventRow) error {
	ctx, cancel := context.WithTimeout(ctx, r.opts.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.URL, bytes.NewReader(ev.Payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(EventIDHeader, ev.ID.String())
	req.Header.Set(EventTypeHeader, ev.Type)
	req.Header.Set(SignatureHeader, webhook.Sign(ep.Secret, time.Now(), ev.Payload))
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.CopyN(io.Discard, resp.Body, maxResponseRead)
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("shop answered %d", resp.StatusCode)
	}
	return nil
}

// finish records the result if this relay still holds the lease; otherwise
// the outcome is "fenced" and nothing changes (D7).
func (r *Relay) finish(ctx context.Context, q *db.Queries, ev db.ClaimOutboxEventRow, status, lastError string,
	retry time.Duration, log *slog.Logger) (string, error) {
	var errText *string
	if lastError != "" {
		if len(lastError) > maxErrorLen {
			lastError = lastError[:maxErrorLen]
		}
		errText = &lastError
	}
	n, err := q.FinishOutboxEvent(ctx, db.FinishOutboxEventParams{
		Status: status, LastError: errText, RetryMs: retry.Milliseconds(), ID: ev.ID, LeaseEpoch: ev.LeaseEpoch,
	})
	if err != nil {
		return "", fmt.Errorf("record outbox event %s: %w", ev.ID, err)
	}
	if n == 0 {
		log.Warn("lease lost before recording the delivery; the new owner decides", "status", status)
		return OutcomeFenced, nil
	}
	switch status {
	case "DELIVERED":
		log.Info("event delivered")
		return OutcomeDelivered, nil
	case "NO_ENDPOINT":
		log.Info("client has no webhook URL; event kept as NO_ENDPOINT")
		return OutcomeNoEndpoint, nil
	case "DEAD":
		return OutcomeDead, nil
	}
	return OutcomeRetry, nil
}
