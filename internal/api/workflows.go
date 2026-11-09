package api

import (
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
	clientID := clientIDFrom(r.Context())
	key := r.Header.Get(idempotencyKeyName)
	log := s.logger.With("client_id", clientID, "idempotency_key", key)

	status, replayed, workflowID := s.handlePost(w, r, clientID, key, log)

	attrs := []any{"status", status, "replayed", replayed}
	if workflowID != "" {
		attrs = append(attrs, "workflow_id", workflowID)
	}
	log.Info("POST /workflows", attrs...)
}

// handlePost writes the response and returns what the access log needs.
func (s *server) handlePost(w http.ResponseWriter, r *http.Request, clientID, key string, log *slog.Logger) (status int, replayed bool, workflowID string) {
	if key == "" {
		writeError(w, http.StatusBadRequest, "idempotency_key_required", "the Idempotency-Key header is required", "")
		return http.StatusBadRequest, false, ""
	}
	if !validIdempotencyKey(key) {
		writeError(w, http.StatusBadRequest, "idempotency_key_invalid",
			"Idempotency-Key must be 1-255 printable ASCII characters without spaces", "")
		return http.StatusBadRequest, false, ""
	}

	req, status, err := decodeRequest(w, r)
	if err != nil {
		code := "invalid_json"
		if status == http.StatusRequestEntityTooLarge {
			code = "body_too_large"
		}
		writeError(w, status, code, err.Error(), "")
		return status, false, ""
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
		return http.StatusUnprocessableEntity, false, ""
	case errors.Is(err, workflow.ErrKeyReuseMismatch):
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused",
			"this Idempotency-Key was already used with a different request body", "")
		return http.StatusUnprocessableEntity, false, ""
	case errors.Is(err, workflow.ErrKeyInUse):
		writeError(w, http.StatusConflict, "idempotency_key_in_use",
			"a request with this Idempotency-Key is still in progress; retry later", "")
		return http.StatusConflict, false, ""
	case err != nil:
		log.Error("submit workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error", "")
		return http.StatusInternalServerError, false, ""
	}

	w.Header().Set("Content-Type", "application/json")
	if res.Replayed {
		w.Header().Set(replayedHeader, "true")
	}
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(res.Body)
	return http.StatusCreated, res.Replayed, extractWorkflowID(res.Body)
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
