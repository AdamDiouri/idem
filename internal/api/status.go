package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
)

// WorkflowReader returns a client's view of one of its workflows; found is
// false for an unknown workflow or another client's. Satisfied by
// *workflow.Reader.
type WorkflowReader interface {
	View(ctx context.Context, clientID string, id uuid.UUID) (view []byte, found bool, err error)
}

// getWorkflow serves GET /workflows/{id} (spec §5.1). The view is written
// as-is: it is the same JSON the workflow's outbox event carries.
func (s *server) getWorkflow(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.opts.RequestTimeout)
	defer cancel()

	clientID := clientIDFrom(r.Context())
	log := s.logger.With("client_id", clientID)
	result, status := s.handleGet(ctx, w, r.PathValue("id"), clientID, log.Error)
	s.opts.Metrics.ObserveStatusRequest(result)
	log.Info("GET /workflows/{id}", "status", status, "result", result, "workflow_id", r.PathValue("id"))
}

// handleGet writes the response and returns the metric label (found,
// not_found, invalid_id, timeout, error) and the HTTP status.
func (s *server) handleGet(ctx context.Context, w http.ResponseWriter, rawID, clientID string, logError func(string, ...any)) (string, int) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_workflow_id", "the workflow ID must be a UUID", "")
		return "invalid_id", http.StatusBadRequest
	}
	view, found, err := s.opts.Workflows.View(ctx, clientID, id)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusServiceUnavailable, "request_timeout", "the request took too long; retry", "")
		return "timeout", http.StatusServiceUnavailable
	case err != nil:
		logError("read workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error", "")
		return "error", http.StatusInternalServerError
	case !found:
		writeError(w, http.StatusNotFound, "workflow_not_found", "no such workflow", "")
		return "not_found", http.StatusNotFound
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(view) //nolint:gosec // G705: JSON built by Postgres (workflow_view), served as application/json, never rendered as HTML
	return "found", http.StatusOK
}
