// Package fakeprovider is a Stripe-like payment provider for tests and local
// runs. It honours idempotency keys, injects faults on demand and records every
// real effect in an oracle that the invariant checker treats as ground truth.
//
// Three resources share one idempotency engine: charges (take money from a
// customer), payouts (pay a merchant) and refunds (return a charge).
package fakeprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	defaultHang   = 30 * time.Second
	maxBodyBytes  = 64 << 10
	declinePrefix = "cus_decline"
	invalidPrefix = "mer_invalid"
)

// Options configures a Provider.
type Options struct {
	// Hang is how long timeout faults hang before giving up (default 30s).
	// A hang also ends when the client goes away.
	Hang time.Duration
	// KeyRetention is how long completed idempotency keys are remembered
	// (0 = forever). Like a real provider, an expired key is unknown: lookups
	// return 404 and a new request with it performs a NEW effect.
	KeyRetention time.Duration
}

// Effect is one real movement of money performed by the provider.
type Effect struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"` // charge | payout | refund
	WorkflowID     string    `json:"workflow_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	AmountCents    int64     `json:"amount_cents"`
	RefundOf       string    `json:"refund_of,omitempty"` // refunds: the charge ID
	At             time.Time `json:"at"`
}

// request is the common view of a decoded resource request.
type request struct {
	AmountCents int64
	Currency    string
	WorkflowID  string
	CustomerID  string // charges
	MerchantID  string // payouts
	ChargeID    string // refunds
	fingerprint string
}

// resource describes one idempotent money-moving endpoint.
type resource struct {
	name          string // URL path segment and fault endpoint: charges | payouts | refunds
	effectType    string // charge | payout | refund
	idPrefix      string // ch_ | po_ | re_
	successStatus string // succeeded | paid
	decode        func(*json.Decoder) (request, bool)
	// rejection is the definitive refusal for this resource (stored and
	// replayed like a success), used by the decline/payout_fail modes.
	rejectStatus int
	rejectCode   string
	// rejects reports whether req is refused on its merits. Called with p.mu held.
	rejects func(p *Provider, req request) bool
}

var resources = []resource{
	{
		name: "charges", effectType: "charge", idPrefix: "ch_", successStatus: "succeeded",
		decode: decodeInto(func(r struct {
			AmountCents int64  `json:"amount_cents"`
			Currency    string `json:"currency"`
			CustomerID  string `json:"customer_id"`
			WorkflowID  string `json:"workflow_id"`
		}) (request, bool) {
			return request{AmountCents: r.AmountCents, Currency: r.Currency, WorkflowID: r.WorkflowID, CustomerID: r.CustomerID},
				r.CustomerID != ""
		}),
		rejectStatus: http.StatusPaymentRequired, rejectCode: "card_declined",
		rejects: func(_ *Provider, req request) bool { return strings.HasPrefix(req.CustomerID, declinePrefix) },
	},
	{
		name: "payouts", effectType: "payout", idPrefix: "po_", successStatus: "paid",
		decode: decodeInto(func(r struct {
			AmountCents int64  `json:"amount_cents"`
			Currency    string `json:"currency"`
			MerchantID  string `json:"merchant_id"`
			WorkflowID  string `json:"workflow_id"`
		}) (request, bool) {
			return request{AmountCents: r.AmountCents, Currency: r.Currency, WorkflowID: r.WorkflowID, MerchantID: r.MerchantID},
				r.MerchantID != ""
		}),
		rejectStatus: http.StatusUnprocessableEntity, rejectCode: "invalid_account",
		rejects: func(_ *Provider, req request) bool { return strings.HasPrefix(req.MerchantID, invalidPrefix) },
	},
	{
		name: "refunds", effectType: "refund", idPrefix: "re_", successStatus: "succeeded",
		decode: decodeInto(func(r struct {
			AmountCents int64  `json:"amount_cents"`
			Currency    string `json:"currency"`
			ChargeID    string `json:"charge_id"`
			WorkflowID  string `json:"workflow_id"`
		}) (request, bool) {
			return request{AmountCents: r.AmountCents, Currency: r.Currency, WorkflowID: r.WorkflowID, ChargeID: r.ChargeID},
				r.ChargeID != ""
		}),
		rejectStatus: http.StatusUnprocessableEntity, rejectCode: "invalid_refund",
		rejects: func(p *Provider, req request) bool { return !p.refundableLocked(req) },
	},
}

type keyState struct {
	createdAt   time.Time
	inFlight    bool
	fingerprint string
	status      int
	body        []byte
}

// Provider is the fake payment provider. Its zero value is not usable; use New.
type Provider struct {
	opts Options

	mu       sync.Mutex
	keys     map[string]*keyState // "<resource>:<key>"
	effects  []Effect
	calls    map[string]int
	hits     map[string]int // "<endpoint>/<mode>" → faults fired
	faults   Faults
	ruleUses []int
	rng      *rand.Rand
}

// New returns a Provider with no faults.
func New(opts Options) *Provider {
	if opts.Hang <= 0 {
		opts.Hang = defaultHang
	}
	p := &Provider{opts: opts}
	p.Reset()
	return p
}

// Handler returns the provider's HTTP API, including the /_ control endpoints.
func (p *Provider) Handler() http.Handler {
	mux := http.NewServeMux()
	for _, res := range resources {
		mux.HandleFunc("POST /"+res.name, p.create(res))
		mux.HandleFunc("GET /"+res.name, p.lookup(res))
	}
	mux.HandleFunc("PUT /_faults", p.putFaults)
	mux.HandleFunc("GET /_effects", p.getEffects)
	mux.HandleFunc("GET /_fault_hits", p.getFaultHits)
	mux.HandleFunc("POST /_reset", p.postReset)
	return mux
}

// SetFaults replaces the fault configuration and resets rule counters.
// It panics on an invalid configuration (a bug in the calling test).
func (p *Provider) SetFaults(f Faults) {
	if err := f.Validate(); err != nil {
		panic(fmt.Sprintf("fakeprovider: invalid faults: %v", err))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.setFaultsLocked(f)
}

func (p *Provider) setFaultsLocked(f Faults) {
	seed := f.Seed
	if seed == 0 {
		seed = 1
	}
	p.faults = f
	p.ruleUses = make([]int, len(f.Rules))
	p.rng = rand.New(rand.NewPCG(uint64(seed), uint64(seed))) //nolint:gosec // fault injection, not security
}

// Effects returns a copy of every effect, in the order performed.
func (p *Provider) Effects() []Effect {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Effect(nil), p.effects...)
}

// Calls returns how many requests each API endpoint received ("POST /charges",
// "GET /payouts", ...), so tests can prove which path a client took.
func (p *Provider) Calls() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return maps.Clone(p.calls)
}

func (p *Provider) count(endpoint string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[endpoint]++
}

// FaultHits returns how many times each fault fired, keyed "<endpoint>/<mode>".
// Counts survive fault changes; only Reset clears them.
func (p *Provider) FaultHits() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return maps.Clone(p.hits)
}

// Reset forgets all keys, effects, call counts, fault hits and faults.
func (p *Provider) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys = map[string]*keyState{}
	p.effects = nil
	p.calls = map[string]int{}
	p.hits = map[string]int{}
	p.setFaultsLocked(Faults{})
}

func (p *Provider) create(res resource) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p.count("POST /" + res.name)
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			writeError(w, http.StatusBadRequest, "idempotency_key_required")
			return
		}
		req, ok := decodeBody(r, res)
		if !ok {
			writeError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		slot := res.name + ":" + key

		p.mu.Lock()
		p.expireLocked(slot)
		if ks, exists := p.keys[slot]; exists {
			p.mu.Unlock()
			switch {
			case ks.inFlight:
				writeError(w, http.StatusConflict, "idempotency_key_in_flight")
			case ks.fingerprint != req.fingerprint:
				writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused")
			default:
				writeRaw(w, ks.status, ks.body)
			}
			return
		}
		p.keys[slot] = &keyState{createdAt: time.Now(), inFlight: true, fingerprint: req.fingerprint}
		mode := p.pickFault(res.name)
		p.mu.Unlock()

		var status int
		var b []byte
		switch mode {
		case ModeTimeoutBeforeEffect:
			p.hang(r.Context())
			p.release(slot)
			status, b = http.StatusGatewayTimeout, errorBody("timeout")
		case ModeErrorBeforeEffect:
			p.release(slot)
			status, b = http.StatusInternalServerError, errorBody("internal_error")
		case ModeRateLimit:
			p.release(slot)
			w.Header().Set("Retry-After", "1")
			status, b = http.StatusTooManyRequests, errorBody("rate_limited")
		case ModeTimeoutAfterEffect:
			status, b = p.perform(res, slot, key, req)
			p.hang(r.Context())
		case ModeErrorAfterEffect:
			p.perform(res, slot, key, req)
			status, b = http.StatusInternalServerError, errorBody("internal_error")
		case ModeDecline, ModePayoutFail:
			status, b = p.reject(res, slot)
		default:
			status, b = p.perform(res, slot, key, req)
		}
		writeRaw(w, status, b)
	}
}

// perform moves the money (or refuses on the request's merits) and stores the
// final response under slot.
func (p *Provider) perform(res resource, slot, key string, req request) (int, []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if res.rejects(p, req) {
		return p.rejectLocked(res, slot)
	}
	id := res.idPrefix + uuid.Must(uuid.NewV7()).String()
	b := mustJSON(map[string]any{
		"id":           id,
		"status":       res.successStatus,
		"amount_cents": req.AmountCents,
		"currency":     req.Currency,
	})
	p.effects = append(p.effects, Effect{
		ID:             id,
		Type:           res.effectType,
		WorkflowID:     req.WorkflowID,
		IdempotencyKey: key,
		AmountCents:    req.AmountCents,
		RefundOf:       req.ChargeID,
		At:             time.Now().UTC(),
	})
	p.store(slot, http.StatusCreated, b)
	return http.StatusCreated, b
}

func (p *Provider) reject(res resource, slot string) (int, []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rejectLocked(res, slot)
}

func (p *Provider) rejectLocked(res resource, slot string) (int, []byte) {
	b := errorBody(res.rejectCode)
	p.store(slot, res.rejectStatus, b)
	return res.rejectStatus, b
}

// refundableLocked reports whether req refunds a real charge without exceeding
// what is left of it. Callers must hold p.mu.
func (p *Provider) refundableLocked(req request) bool {
	var charged, refunded int64
	found := false
	for _, e := range p.effects {
		switch {
		case e.Type == "charge" && e.ID == req.ChargeID:
			charged, found = e.AmountCents, true
		case e.Type == "refund" && e.RefundOf == req.ChargeID:
			refunded += e.AmountCents
		}
	}
	return found && refunded+req.AmountCents <= charged
}

// store records a final response for slot. Callers must hold p.mu.
func (p *Provider) store(slot string, status int, b []byte) {
	ks := p.keys[slot]
	ks.inFlight, ks.status, ks.body = false, status, b
}

// release forgets an in-flight key that produced no effect, so it can be retried.
func (p *Provider) release(slot string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ks, ok := p.keys[slot]; ok && ks.inFlight {
		delete(p.keys, slot)
	}
}

// expireLocked forgets slot if it completed longer than KeyRetention ago.
// Callers must hold p.mu.
func (p *Provider) expireLocked(slot string) {
	ks, ok := p.keys[slot]
	if ok && !ks.inFlight && p.opts.KeyRetention > 0 && time.Since(ks.createdAt) > p.opts.KeyRetention {
		delete(p.keys, slot)
	}
}

func (p *Provider) hang(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(p.opts.Hang):
	}
}

func (p *Provider) lookup(res resource) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p.count("GET /" + res.name)
		slot := res.name + ":" + r.URL.Query().Get("idempotency_key")
		p.mu.Lock()
		p.expireLocked(slot)
		ks, ok := p.keys[slot]
		var inFlight bool
		var status int
		var b []byte
		if ok {
			inFlight, status, b = ks.inFlight, ks.status, ks.body
		}
		p.mu.Unlock()

		switch {
		case inFlight:
			writeError(w, http.StatusConflict, "idempotency_key_in_flight")
		case b == nil:
			writeError(w, http.StatusNotFound, "not_found")
		case status >= 200 && status < 300:
			writeRaw(w, http.StatusOK, b)
		default:
			writeRaw(w, status, b)
		}
	}
}

func (p *Provider) putFaults(w http.ResponseWriter, r *http.Request) {
	var f Faults
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(&f); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_faults")
		return
	}
	if err := f.Validate(); err != nil {
		writeRaw(w, http.StatusBadRequest, mustJSON(map[string]any{
			"error": map[string]string{"code": "invalid_faults", "message": err.Error()},
		}))
		return
	}
	p.SetFaults(f)
	slog.Info("faults updated", "rules", len(f.Rules))
	w.WriteHeader(http.StatusNoContent)
}

func (p *Provider) getEffects(w http.ResponseWriter, _ *http.Request) {
	effects := p.Effects()
	if effects == nil {
		effects = []Effect{}
	}
	writeRaw(w, http.StatusOK, mustJSON(effects))
}

func (p *Provider) getFaultHits(w http.ResponseWriter, _ *http.Request) {
	writeRaw(w, http.StatusOK, mustJSON(p.FaultHits()))
}

func (p *Provider) postReset(w http.ResponseWriter, _ *http.Request) {
	p.Reset()
	w.WriteHeader(http.StatusNoContent)
}

// decodeInto builds a strict decoder for one resource's JSON body shape.
func decodeInto[T any](convert func(T) (request, bool)) func(*json.Decoder) (request, bool) {
	return func(dec *json.Decoder) (request, bool) {
		var body T
		if err := dec.Decode(&body); err != nil {
			return request{}, false
		}
		if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return request{}, false
		}
		req, ok := convert(body)
		req.fingerprint = string(mustJSON(body))
		return req, ok && req.AmountCents > 0 && req.Currency != "" && req.WorkflowID != ""
	}
}

func decodeBody(r *http.Request, res resource) (request, bool) {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	return res.decode(dec)
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func errorBody(code string) []byte {
	return mustJSON(map[string]any{"error": map[string]string{"code": code}})
}

func writeRaw(w http.ResponseWriter, status int, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeRaw(w, status, errorBody(code))
}
