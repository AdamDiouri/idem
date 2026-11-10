package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/AdamDiouri/idem/internal/failpoint"
	"github.com/AdamDiouri/idem/internal/webhook"
)

// Failpoints the API hits (design §14.2).
const FPWebhookAfterInsert = "webhook_after_insert"

// FailpointNames lists the API's failpoints, for config validation.
var FailpointNames = []string{FPWebhookAfterInsert}

const (
	maxWebhookBytes  = 64 << 10
	webhookTolerance = 5 * time.Minute
)

// postProviderWebhook receives a provider webhook (M2 design §4.3):
// verify the signature → store the raw event → acknowledge. A 2xx is only ever
// sent once the event is committed (durable before acknowledged); duplicates
// get 200 so the provider stops retrying; a store failure gets 503 so it retries.
func (s *server) postProviderWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			s.refuseWebhook(w, http.StatusRequestEntityTooLarge, "too_large", "too_large", err)
			return
		}
		s.refuseWebhook(w, http.StatusBadRequest, "malformed", "malformed", err)
		return
	}
	err = webhook.Verify(r.Header.Get(webhook.ProviderSignatureHeader), body, s.opts.WebhookSecrets, time.Now(), webhookTolerance)
	switch {
	case errors.Is(err, webhook.ErrStaleSignature):
		s.refuseWebhook(w, http.StatusBadRequest, "stale_signature", "stale", err)
		return
	case err != nil:
		s.refuseWebhook(w, http.StatusBadRequest, "bad_signature", "bad_signature", err)
		return
	}
	var ev struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}
	if json.Unmarshal(body, &ev) != nil || ev.ID == "" || ev.Type == "" {
		s.refuseWebhook(w, http.StatusBadRequest, "malformed", "malformed", errors.New("event without id or type"))
		return
	}

	// Bounded like POST /workflows: a saturated pool answers 503 (the
	// provider retries) instead of holding the request open.
	ctx, cancel := context.WithTimeout(r.Context(), s.opts.RequestTimeout)
	defer cancel()
	inserted, err := s.opts.Inbox.Store(ctx, ev.ID, ev.Type, body)
	if err != nil {
		s.logger.Error("store provider webhook", "event_id", ev.ID, "error", err)
		s.opts.Metrics.ObserveWebhook("store_error")
		writeError(w, http.StatusServiceUnavailable, "store_error", "could not store the event; retry", "")
		return
	}
	failpoint.Hit(FPWebhookAfterInsert)
	result := "accepted"
	if !inserted {
		result = "duplicate"
	}
	s.opts.Metrics.ObserveWebhook(result)
	s.logger.Info("provider webhook stored", "event_id", ev.ID, "type", ev.Type, "result", result)
	writeJSON(w, http.StatusOK, map[string]string{"status": result})
}

// refuseWebhook answers a webhook that must not be stored. The body is never logged.
func (s *server) refuseWebhook(w http.ResponseWriter, status int, code, result string, err error) {
	s.logger.Warn("provider webhook refused", "result", result, "error", err)
	s.opts.Metrics.ObserveWebhook(result)
	writeError(w, status, code, "webhook refused", "")
}
