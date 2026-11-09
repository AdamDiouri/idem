// Package fakeprovider is a Stripe-like payment provider for tests and local
// runs. It honours idempotency keys, injects faults on demand and records every
// real effect in an oracle that the invariant checker treats as ground truth.
package fakeprovider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	defaultHang      = 30 * time.Second
	maxBodyBytes     = 64 << 10
	declinePrefix    = "cus_decline"
	effectTypeCharge = "charge"
)

// Options configures a Provider.
type Options struct {
	// Hang is how long timeout faults hang before giving up (default 30s).
	// A hang also ends when the client goes away.
	Hang time.Duration
}

// Effect is one real movement of money performed by the provider.
type Effect struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"`
	WorkflowID     string    `json:"workflow_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	AmountCents    int64     `json:"amount_cents"`
	At             time.Time `json:"at"`
}

type chargeRequest struct {
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
	CustomerID  string `json:"customer_id"`
	WorkflowID  string `json:"workflow_id"`
}

type keyState struct {
	inFlight    bool
	fingerprint string
	status      int
	body        []byte
}

// Provider is the fake payment provider. Its zero value is not usable; use New.
type Provider struct {
	opts Options

	mu       sync.Mutex
	keys     map[string]*keyState
	effects  []Effect
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
	mux.HandleFunc("POST /charges", p.postCharge)
	mux.HandleFunc("GET /charges", p.getCharge)
	mux.HandleFunc("PUT /_faults", p.putFaults)
	mux.HandleFunc("GET /_effects", p.getEffects)
	mux.HandleFunc("POST /_reset", p.postReset)
	return mux
}

// SetFaults replaces the fault configuration and resets rule counters.
func (p *Provider) SetFaults(f Faults) {
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

// Reset forgets all keys, effects and faults.
func (p *Provider) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys = map[string]*keyState{}
	p.effects = nil
	p.setFaultsLocked(Faults{})
}

func (p *Provider) postCharge(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "idempotency_key_required")
		return
	}
	req, ok := decodeCharge(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	fp := fingerprint(req)

	p.mu.Lock()
	if ks, exists := p.keys[key]; exists {
		p.mu.Unlock()
		switch {
		case ks.inFlight:
			writeError(w, http.StatusConflict, "idempotency_key_in_flight")
		case ks.fingerprint != fp:
			writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused")
		default:
			writeRaw(w, ks.status, ks.body)
		}
		return
	}
	p.keys[key] = &keyState{inFlight: true, fingerprint: fp}
	mode := p.pickFault("charges")
	p.mu.Unlock()

	switch mode {
	case ModeTimeoutBeforeEffect:
		p.hang(r.Context())
		p.release(key)
		writeError(w, http.StatusGatewayTimeout, "timeout")
	case ModeErrorBeforeEffect:
		p.release(key)
		writeError(w, http.StatusInternalServerError, "internal_error")
	case ModeTimeoutAfterEffect:
		status, b := p.perform(key, req)
		p.hang(r.Context())
		writeRaw(w, status, b)
	case ModeErrorAfterEffect:
		p.perform(key, req)
		writeError(w, http.StatusInternalServerError, "internal_error")
	case ModeDecline:
		status, b := p.decline(key)
		writeRaw(w, status, b)
	default:
		if strings.HasPrefix(req.CustomerID, declinePrefix) {
			status, b := p.decline(key)
			writeRaw(w, status, b)
			return
		}
		status, b := p.perform(key, req)
		writeRaw(w, status, b)
	}
}

// perform moves the money: records the effect and stores the success response.
func (p *Provider) perform(key string, req chargeRequest) (int, []byte) {
	id := "ch_" + uuid.Must(uuid.NewV7()).String()
	b := mustJSON(map[string]any{
		"id":           id,
		"status":       "succeeded",
		"amount_cents": req.AmountCents,
		"currency":     req.Currency,
	})

	p.mu.Lock()
	defer p.mu.Unlock()
	p.effects = append(p.effects, Effect{
		ID:             id,
		Type:           effectTypeCharge,
		WorkflowID:     req.WorkflowID,
		IdempotencyKey: key,
		AmountCents:    req.AmountCents,
		At:             time.Now().UTC(),
	})
	p.store(key, http.StatusCreated, b)
	return http.StatusCreated, b
}

func (p *Provider) decline(key string) (int, []byte) {
	b := mustJSON(map[string]any{"error": map[string]string{"code": "card_declined"}})
	p.mu.Lock()
	defer p.mu.Unlock()
	p.store(key, http.StatusPaymentRequired, b)
	return http.StatusPaymentRequired, b
}

// store records a final response for key. Callers must hold p.mu.
func (p *Provider) store(key string, status int, b []byte) {
	ks := p.keys[key]
	ks.inFlight, ks.status, ks.body = false, status, b
}

// release forgets an in-flight key that produced no effect, so it can be retried.
func (p *Provider) release(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ks, ok := p.keys[key]; ok && ks.inFlight {
		delete(p.keys, key)
	}
}

func (p *Provider) hang(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(p.opts.Hang):
	}
}

func (p *Provider) getCharge(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("idempotency_key")
	p.mu.Lock()
	ks, ok := p.keys[key]
	var status int
	var b []byte
	if ok && !ks.inFlight {
		status, b = ks.status, ks.body
	}
	p.mu.Unlock()

	switch {
	case b == nil:
		writeError(w, http.StatusNotFound, "not_found")
	case status >= 200 && status < 300:
		writeRaw(w, http.StatusOK, b)
	default:
		writeRaw(w, status, b)
	}
}

func (p *Provider) putFaults(w http.ResponseWriter, r *http.Request) {
	var f Faults
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(&f); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_faults")
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

func (p *Provider) postReset(w http.ResponseWriter, _ *http.Request) {
	p.Reset()
	w.WriteHeader(http.StatusNoContent)
}

func decodeCharge(r *http.Request) (chargeRequest, bool) {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	var req chargeRequest
	if err := dec.Decode(&req); err != nil {
		return req, false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return req, false
	}
	ok := req.AmountCents > 0 && req.Currency != "" && req.CustomerID != "" && req.WorkflowID != ""
	return req, ok
}

func fingerprint(req chargeRequest) string {
	return string(mustJSON(req))
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func writeRaw(w http.ResponseWriter, status int, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeRaw(w, status, mustJSON(map[string]any{"error": map[string]string{"code": code}}))
}
