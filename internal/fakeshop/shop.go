// Package fakeshop is a shop receiving idem's outbound webhooks, for tests,
// compose and chaos runs (M2 design §5.4). It verifies signatures,
// deduplicates by event ID as a real shop must, misbehaves on request, and
// reports what it learned (GET /_seen), the oracle for invariant I9.
package fakeshop

import (
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/AdamDiouri/idem/internal/webhook"
)

// signatureTolerance is how far a delivery's signature timestamp may be from now.
const signatureTolerance = 5 * time.Minute

// maxBody bounds a delivery's body.
const maxBody = 64 << 10

// Options configures a Shop.
type Options struct {
	Secret  []byte        // verifies Idem-Signature
	Slow    time.Duration // ModeSlow's delay
	LateAck time.Duration // ModeAckAfterTimeout's delay (longer than the relay's timeout)
}

// SeenEvent is one event the shop processed, deduplicated by ID.
type SeenEvent struct {
	EventID    string `json:"event_id"`
	WorkflowID string `json:"workflow_id"`
	Type       string `json:"type"`
	Status     string `json:"status"`
	Deliveries int    `json:"deliveries"`
}

// Seen is what the shop learned: each event once, how many deliveries were
// repeats, and how many were refused for their signature.
type Seen struct {
	Events        []SeenEvent `json:"events"`
	Duplicates    int         `json:"duplicates"`
	BadSignatures int         `json:"bad_signatures"`
}

// Shop is the fake shop.
type Shop struct {
	opts Options

	mu       sync.Mutex
	events   map[string]*SeenEvent
	order    []string
	dups     int
	bad      int
	faults   Faults
	rng      *rand.Rand
	ruleUses map[int]int
	hits     map[string]int
}

// New returns a shop.
func New(opts Options) *Shop {
	return &Shop{opts: opts, events: map[string]*SeenEvent{}, rng: rand.New(rand.NewPCG(1, 1)), //nolint:gosec // fault draws, not security
		ruleUses: map[int]int{}, hits: map[string]int{}}
}

// Handler serves the shop's endpoints.
func (s *Shop) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /hooks", s.receive)
	mux.HandleFunc("GET /_seen", s.getSeen)
	mux.HandleFunc("PUT /_faults", s.putFaults)
	mux.HandleFunc("GET /_faults/hits", s.getHits)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

func (s *Shop) receive(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	if err := webhook.Verify(r.Header.Get("Idem-Signature"), body, [][]byte{s.opts.Secret}, time.Now(), signatureTolerance); err != nil {
		s.mu.Lock()
		s.bad++
		s.mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	mode := s.pickFault()
	s.mu.Unlock()

	switch mode {
	case ModeDown:
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	case Mode5xx:
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ModeSlow:
		time.Sleep(s.opts.Slow)
	}
	s.record(r.Header.Get("Idem-Event-Id"), r.Header.Get("Idem-Event-Type"), body)
	switch mode {
	case ModeAckThen5xx:
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ModeAckAfterTimeout:
		time.Sleep(s.opts.LateAck)
	}
	w.WriteHeader(http.StatusOK)
}

// record processes an event once; a repeat only counts.
func (s *Shop) record(eventID, eventType string, body []byte) {
	var v struct {
		WorkflowID string `json:"workflow_id"`
		Status     string `json:"status"`
	}
	_ = json.Unmarshal(body, &v) // a malformed body is recorded with empty fields, and I9 reports it
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.events[eventID]; ok {
		e.Deliveries++
		s.dups++
		return
	}
	s.events[eventID] = &SeenEvent{EventID: eventID, WorkflowID: v.WorkflowID, Type: eventType, Status: v.Status, Deliveries: 1}
	s.order = append(s.order, eventID)
}

// pickFault returns the mode for one delivery, or "". Callers hold s.mu.
func (s *Shop) pickFault() string {
	for i, r := range s.faults.Rules {
		if r.Times > 0 && s.ruleUses[i] >= r.Times {
			continue
		}
		if r.Probability > 0 && s.rng.Float64() >= r.Probability {
			continue
		}
		s.ruleUses[i]++
		s.hits[r.Mode]++
		return r.Mode
	}
	return ""
}

func (s *Shop) getSeen(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := Seen{Events: make([]SeenEvent, 0, len(s.order)), Duplicates: s.dups, BadSignatures: s.bad}
	for _, id := range s.order {
		out.Events = append(out.Events, *s.events[id])
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

func (s *Shop) putFaults(w http.ResponseWriter, r *http.Request) {
	var f Faults
	if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := f.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.mu.Lock()
	s.faults, s.ruleUses = f, map[int]int{}
	s.rng = rand.New(rand.NewPCG(uint64(f.Seed), uint64(f.Seed))) //nolint:gosec // fault draws, not security
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Shop) getHits(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := make(map[string]int, len(s.hits))
	for k, v := range s.hits {
		out[k] = v
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
