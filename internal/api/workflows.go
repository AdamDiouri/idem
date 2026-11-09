package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/AdamDiouri/idem/internal/workflow"
)

const (
	maxBodyBytes       = 64 << 10
	maxIdempotencyKey  = 255
	replayedHeader     = "Idempotent-Replayed"
	idempotencyKeyName = "Idempotency-Key"
)

func (s *server) postWorkflows(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.opts.RequestTimeout)
	defer cancel()
	r = r.WithContext(ctx)

	clientID := clientIDFrom(r.Context())
	key := r.Header.Get(idempotencyKeyName)
	log := s.logger.With("client_id", clientID, "idempotency_key", key)

	res := s.handlePost(w, r, clientID, key, log)
	s.opts.Metrics.ObserveSubmission(res.result)

	attrs := []any{"status", res.status, "result", res.result}
	if res.workflowID != "" {
		attrs = append(attrs, "workflow_id", res.workflowID)
	}
	log.Info("POST /workflows", attrs...)
}

// postResult is what the access log and metrics need. result is a bounded
// label: created, replayed, bad_request, validation_failed, key_reused,
// key_in_use, timeout, error.
type postResult struct {
	status     int
	result     string
	workflowID string
}

// handlePost writes the response and returns what the access log needs.
func (s *server) handlePost(w http.ResponseWriter, r *http.Request, clientID, key string, log *slog.Logger) postResult {
	if key == "" {
		writeError(w, http.StatusBadRequest, "idempotency_key_required", "the Idempotency-Key header is required", "")
		return postResult{status: http.StatusBadRequest, result: "bad_request"}
	}
	if !validIdempotencyKey(key) {
		writeError(w, http.StatusBadRequest, "idempotency_key_invalid",
			"Idempotency-Key must be 1-255 printable ASCII characters without spaces", "")
		return postResult{status: http.StatusBadRequest, result: "bad_request"}
	}

	req, status, err := decodeRequest(w, r)
	if err != nil {
		code := "invalid_json"
		if status == http.StatusRequestEntityTooLarge {
			code = "body_too_large"
		}
		writeError(w, status, code, err.Error(), "")
		return postResult{status: status, result: "bad_request"}
	}

	// Business validation happens inside Submit, after the key lookup, so a
	// retry of an existing workflow replays even if rules or fees changed.
	res, err := s.sub.Submit(r.Context(), workflow.SubmitRequest{
		ClientID:       clientID,
		IdempotencyKey: key,
		Request:        req,
	})
	var ve *workflow.ValidationError
	switch {
	case errors.As(err, &ve):
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", ve.Message, ve.Field)
		return postResult{status: http.StatusUnprocessableEntity, result: "validation_failed"}
	case errors.Is(err, workflow.ErrKeyReuseMismatch):
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused",
			"this Idempotency-Key was already used with a different request body", "")
		return postResult{status: http.StatusUnprocessableEntity, result: "key_reused"}
	case errors.Is(err, context.DeadlineExceeded):
		// The transaction may or may not have committed; a retry with the same
		// key either replays the workflow or creates it, never both.
		log.Warn("submit workflow timed out", "error", err)
		writeError(w, http.StatusServiceUnavailable, "request_timeout",
			"request timed out; retry with the same Idempotency-Key", "")
		return postResult{status: http.StatusServiceUnavailable, result: "timeout"}
	case errors.Is(err, workflow.ErrKeyInUse):
		writeError(w, http.StatusConflict, "idempotency_key_in_use",
			"a request with this Idempotency-Key is still in progress; retry later", "")
		return postResult{status: http.StatusConflict, result: "key_in_use"}
	case err != nil:
		log.Error("submit workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error", "")
		return postResult{status: http.StatusInternalServerError, result: "error"}
	}

	w.Header().Set("Content-Type", "application/json")
	if res.Replayed {
		w.Header().Set(replayedHeader, "true")
	}
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(res.Body)
	result := "created"
	if res.Replayed {
		result = "replayed"
	}
	return postResult{status: http.StatusCreated, result: result, workflowID: extractWorkflowID(res.Body)}
}

// decodeRequest reads exactly one JSON object with known fields from a body of at most maxBodyBytes.
func decodeRequest(w http.ResponseWriter, r *http.Request) (workflow.CreateWorkflowRequest, int, error) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()

	var req workflow.CreateWorkflowRequest
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return req, http.StatusRequestEntityTooLarge, errors.New("request body exceeds 64 KiB")
		}
		return req, http.StatusBadRequest, errors.New("request body must be a single valid JSON object with known fields")
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return req, http.StatusBadRequest, errors.New("request body must contain exactly one JSON object")
	}
	return req, 0, nil
}

func validIdempotencyKey(key string) bool {
	if len(key) == 0 || len(key) > maxIdempotencyKey {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7e {
			return false
		}
	}
	return true
}

func extractWorkflowID(body []byte) string {
	var c struct {
		WorkflowID string `json:"workflow_id"`
	}
	_ = json.Unmarshal(body, &c) // log enrichment only
	return c.WorkflowID
}
