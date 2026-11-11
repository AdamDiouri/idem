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

// deliveryState is a delivery's lifecycle (slice 2.5b: one field instead of
// three booleans, so delivered-and-given-up cannot be represented):
//
//	queued ──take──▶ sending ──2xx──▶ delivered
//	   ▲                │
//	   └──── retry ─────┤  (backing off = queued with nextAt in the future)
//	                    └──last attempt──▶ gaveUp
//
// A drop_webhook fault starts it as gaveUp: never sent.
type deliveryState int

const (
	queued deliveryState = iota
	sending
	delivered
	gaveUp
)

// delivery is one webhook event on its way to WebhookURLs (and its log entry).
type delivery struct {
	index     int      // position in the queue: rotates the first target
	targets   []string // the URL of each attempt
	event     webhook.ProviderEvent
	body      []byte
	fault     string
	stale     bool // a reorder fault's contradictory copy
	attempts  int
	nextAt    time.Time
	state     deliveryState
	fast      bool // fast_webhook: delivered inline by POST /payouts
	sentTwice bool // dup_webhook: the second copy went out
}

// WebhookDelivery is the oracle's view of one webhook event.
type WebhookDelivery struct {
	EventID   string   `json:"event_id"`
	Type      string   `json:"type"`
	PayoutID  string   `json:"payout_id"`
	Attempts  int      `json:"attempts"`
	Delivered bool     `json:"delivered"`
	GaveUp    bool     `json:"gave_up"` // finished without a 2xx (or a drop fault: never sent)
	Stale     bool     `json:"stale"`
	Fault     string   `json:"fault,omitempty"`
	Created   int64    `json:"created"` // the event's created time (Unix seconds)
	Targets   []string `json:"targets"` // the URL of each attempt, in order
}

// enqueueWebhookLocked queues the event for a just-settled payout. Callers hold p.mu.
func (p *Provider) enqueueWebhookLocked(po *payout, now time.Time) {
	if len(p.opts.WebhookURLs) == 0 {
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
		d.state = gaveUp // never sent
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
	d := &delivery{index: len(p.webhooks), event: e, body: mustJSON(e), nextAt: now}
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
			go p.send(ctx, d) // a slow receiver must not hold up the others
		}
	}
}

// deliverFast sends the fast_webhook event of the payout accepted under slot
// inline, before the 202 is written. The payout was held back from Run, so
// nothing else can settle or send it first.
func (p *Provider) deliverFast(ctx context.Context, slot string) {
	if len(p.opts.WebhookURLs) == 0 {
		return
	}
	p.mu.Lock()
	ks := p.keys[slot]
	var po *payout
	if ks != nil {
		po = p.payouts[ks.payoutID]
	}
	if po == nil || !po.fastHold {
		p.mu.Unlock()
		return
	}
	po.fastHold = false
	p.settleDueLocked(time.Now())
	due := p.takeDueLocked(func(d *delivery) bool { return d.fast && d.attempts == 0 && d.event.Data.PayoutID == po.id })
	p.mu.Unlock()
	for _, d := range due {
		p.send(context.WithoutCancel(ctx), d)
	}
}

// takeDueLocked marks matching unsent deliveries as being sent. Callers hold p.mu.
func (p *Provider) takeDueLocked(match func(*delivery) bool) []*delivery {
	var due []*delivery
	for _, d := range p.webhooks {
		if d.state == queued && match(d) {
			d.state = sending
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
	p.mu.Lock()
	n := len(d.targets) // every send so far, including dup copies
	p.mu.Unlock()
	url := p.opts.WebhookURLs[(d.index+n)%len(p.opts.WebhookURLs)]
	ok := p.post(ctx, url, d.body, secret)
	sent := []string{url}
	if ok && d.fault == ModeDupWebhook && !d.sentTwice {
		// The same event again, to the next replica: dedup across replicas.
		dupURL := p.opts.WebhookURLs[(d.index+n+1)%len(p.opts.WebhookURLs)]
		p.post(ctx, dupURL, d.body, p.opts.WebhookSecret)
		sent = append(sent, dupURL)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	d.targets = append(d.targets, sent...)
	d.attempts++
	switch {
	case ok:
		d.state = delivered
		if d.fault == ModeDupWebhook {
			d.sentTwice = true
		}
		if d.fault == ModeReorder && !d.stale {
			p.enqueueStaleLocked(d)
		}
	case d.attempts >= p.opts.WebhookMaxAttempts:
		d.state = gaveUp
	default:
		d.state = queued
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

func (p *Provider) post(ctx context.Context, url string, body, secret []byte) bool {
	ctx, cancel := context.WithTimeout(ctx, webhookTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
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
			Attempts: d.attempts, Delivered: d.state == delivered, GaveUp: d.state == gaveUp, Stale: d.stale, Fault: d.fault,
			Created: d.event.Created,
			Targets: append([]string(nil), d.targets...),
		})
	}
	return out
}

func (p *Provider) getWebhooks(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p.Webhooks())
}
