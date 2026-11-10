package fakeprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/AdamDiouri/idem/internal/webhook"
)

const webhookTimeout = 2 * time.Second

// delivery is one webhook event on its way to WebhookURL (and its log entry).
type delivery struct {
	event     webhook.ProviderEvent
	body      []byte
	fault     string
	stale     bool // a reorder fault's contradictory copy
	attempts  int
	nextAt    time.Time
	sending   bool
	delivered bool
	gaveUp    bool
	fast      bool // fast_webhook: delivered inline by POST /payouts
	sentTwice bool // dup_webhook: the second copy went out
}

// WebhookDelivery is the oracle's view of one webhook event.
type WebhookDelivery struct {
	EventID   string `json:"event_id"`
	Type      string `json:"type"`
	PayoutID  string `json:"payout_id"`
	Attempts  int    `json:"attempts"`
	Delivered bool   `json:"delivered"`
	Stale     bool   `json:"stale"`
	Fault     string `json:"fault,omitempty"`
}

// enqueueWebhookLocked queues the event for a just-settled payout. Callers hold p.mu.
func (p *Provider) enqueueWebhookLocked(po *payout, now time.Time) {
	if p.opts.WebhookURL == "" {
		return
	}
	typ := webhook.EventPayoutPaid
	if po.status == "failed" {
		typ = webhook.EventPayoutFailed
	}
	d := p.newDeliveryLocked(po, typ, po.settleAt.Unix(), now)
	d.fault = po.webhookFault
	switch d.fault {
	case ModeDropWebhook:
		d.gaveUp = true // never sent
	case ModeDelayWebhook:
		d.nextAt = now.Add(4 * p.opts.WebhookRetryBase)
	case ModeFastWebhook:
		d.fast = true
	}
}

func (p *Provider) newDeliveryLocked(po *payout, typ string, created int64, now time.Time) *delivery {
	e := webhook.ProviderEvent{ID: "evt_" + uuid.Must(uuid.NewV7()).String(), Type: typ, Created: created}
	e.Data.PayoutID, e.Data.AmountCents, e.Data.Currency = po.id, po.req.AmountCents, po.req.Currency
	e.Data.Status, e.Data.IdempotencyKey = "paid", po.key
	if typ == webhook.EventPayoutFailed {
		e.Data.Status, e.Data.FailureCode = "failed", failureAccountClosed
	}
	e.Data.Metadata.WorkflowID = po.req.WorkflowID
	d := &delivery{event: e, body: mustJSON(e), nextAt: now}
	p.webhooks = append(p.webhooks, d)
	return d
}

// Run settles due payouts and delivers webhooks every Tick until ctx ends.
func (p *Provider) Run(ctx context.Context) {
	t := time.NewTicker(p.opts.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		p.mu.Lock()
		now := time.Now()
		p.settleDueLocked(now)
		due := p.takeDueLocked(func(d *delivery) bool { return !now.Before(d.nextAt) })
		p.mu.Unlock()
		for _, d := range due {
			p.send(ctx, d)
		}
	}
}

// deliverFast sends fast_webhook events inline (before the 202 is written).
func (p *Provider) deliverFast(ctx context.Context) {
	if p.opts.WebhookURL == "" {
		return
	}
	p.mu.Lock()
	p.settleDueLocked(time.Now())
	due := p.takeDueLocked(func(d *delivery) bool { return d.fast && d.attempts == 0 })
	p.mu.Unlock()
	for _, d := range due {
		p.send(context.WithoutCancel(ctx), d)
	}
}

// takeDueLocked marks matching unsent deliveries as being sent. Callers hold p.mu.
func (p *Provider) takeDueLocked(match func(*delivery) bool) []*delivery {
	var due []*delivery
	for _, d := range p.webhooks {
		if !d.delivered && !d.gaveUp && !d.sending && match(d) {
			d.sending = true
			due = append(due, d)
		}
	}
	return due
}

// send makes one delivery attempt (no lock held during HTTP) and records it.
func (p *Provider) send(ctx context.Context, d *delivery) {
	secret := p.opts.WebhookSecret
	if d.fault == ModeBadSignature && d.attempts == 0 {
		secret = []byte("not-the-secret")
	}
	ok := p.post(ctx, d.body, secret)
	if ok && d.fault == ModeDupWebhook && !d.sentTwice {
		p.post(ctx, d.body, p.opts.WebhookSecret) // the same event again
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	d.sending = false
	d.attempts++
	switch {
	case ok:
		d.delivered = true
		if d.fault == ModeDupWebhook {
			d.sentTwice = true
		}
		if d.fault == ModeReorder && !d.stale {
			p.enqueueStaleLocked(d)
		}
	case d.attempts >= p.opts.WebhookMaxAttempts:
		d.gaveUp = true
	default:
		d.nextAt = time.Now().Add(p.opts.WebhookRetryBase << (d.attempts - 1))
	}
}

// enqueueStaleLocked queues a contradictory copy of d's event, older than it.
func (p *Provider) enqueueStaleLocked(d *delivery) {
	po := p.payouts[d.event.Data.PayoutID]
	if po == nil {
		return
	}
	opposite := webhook.EventPayoutFailed
	if d.event.Type == webhook.EventPayoutFailed {
		opposite = webhook.EventPayoutPaid
	}
	s := p.newDeliveryLocked(po, opposite, d.event.Created-1, time.Now())
	s.stale, s.fault = true, ModeReorder
}

func (p *Provider) post(ctx context.Context, body, secret []byte) bool {
	ctx, cancel := context.WithTimeout(ctx, webhookTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.opts.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(webhook.ProviderSignatureHeader, webhook.Sign(secret, time.Now(), body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// Webhooks returns every webhook event and its delivery state, in order.
func (p *Provider) Webhooks() []WebhookDelivery {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]WebhookDelivery, 0, len(p.webhooks))
	for _, d := range p.webhooks {
		out = append(out, WebhookDelivery{
			EventID: d.event.ID, Type: d.event.Type, PayoutID: d.event.Data.PayoutID,
			Attempts: d.attempts, Delivered: d.delivered, Stale: d.stale, Fault: d.fault,
		})
	}
	return out
}

func (p *Provider) getWebhooks(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p.Webhooks())
}
