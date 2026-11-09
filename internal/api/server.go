// Package api is idem's HTTP interface.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/AdamDiouri/idem/internal/workflow"
)

const readyTimeout = 2 * time.Second

// Submitter creates workflows idempotently; satisfied by *workflow.Submitter.
type Submitter interface {
	Submit(context.Context, workflow.SubmitRequest) (workflow.SubmitResult, error)
}

// Pinger checks database reachability; satisfied by *pgxpool.Pool.
type Pinger interface {
	Ping(context.Context) error
}

// Options configures the HTTP server.
type Options struct {
	Tokens map[string]string // bearer token → client ID
	Logger *slog.Logger
	// RequestTimeout bounds all work for one POST /workflows, including waiting
	// for a database connection. It must be below the server's WriteTimeout so
	// the client always gets a status. Zero means defaultRequestTimeout.
	RequestTimeout time.Duration
}

const defaultRequestTimeout = 10 * time.Second

type server struct {
	opts   Options
	sub    Submitter
	db     Pinger
	logger *slog.Logger
}

// NewServer returns the HTTP handler for the idem API.
func NewServer(opts Options, sub Submitter, db Pinger) http.Handler {
	s := &server{opts: opts, sub: sub, db: db, logger: opts.Logger}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	if s.opts.RequestTimeout <= 0 {
		s.opts.RequestTimeout = defaultRequestTimeout
	}

	mux := http.NewServeMux()
	mux.Handle("POST /workflows", requireToken(opts.Tokens, http.HandlerFunc(s.postWorkflows)))
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	return mux
}

// healthz reports that the process is alive; it never checks dependencies.
func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz reports whether the database is reachable, so traffic can be withheld when it is not.
func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		s.logger.Warn("readiness check failed", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
