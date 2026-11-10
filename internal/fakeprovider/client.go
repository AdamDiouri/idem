package fakeprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// oracleClient reads the fake provider's oracle endpoints. Every call is
// bounded, so a hung provider cannot stall the chaos harness (its settle phase
// is bounded by its own deadline only if each read is).
var oracleClient = &http.Client{Timeout: 10 * time.Second}

// FetchEffects reads the effects oracle of a provider running at baseURL.
func FetchEffects(ctx context.Context, baseURL string) ([]Effect, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/_effects", nil)
	if err != nil {
		return nil, err
	}
	resp, err := oracleClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch effects: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch effects: status %d", resp.StatusCode)
	}
	var effects []Effect
	if err := json.NewDecoder(resp.Body).Decode(&effects); err != nil {
		return nil, fmt.Errorf("decode effects: %w", err)
	}
	return effects, nil
}

// PutFaults replaces the fault configuration of a provider running at baseURL.
func PutFaults(ctx context.Context, baseURL string, f Faults) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, baseURL+"/_faults", bytes.NewReader(b))
	if err != nil {
		return err
	}
	resp, err := oracleClient.Do(req)
	if err != nil {
		return fmt.Errorf("put faults: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("put faults: status %d", resp.StatusCode)
	}
	return nil
}

// FetchFaultHits reads how many times each fault fired on a provider at baseURL.
func FetchFaultHits(ctx context.Context, baseURL string) (map[string]int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/_fault_hits", nil)
	if err != nil {
		return nil, err
	}
	resp, err := oracleClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch fault hits: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch fault hits: status %d", resp.StatusCode)
	}
	hits := map[string]int{}
	if err := json.NewDecoder(resp.Body).Decode(&hits); err != nil {
		return nil, fmt.Errorf("decode fault hits: %w", err)
	}
	return hits, nil
}

// FetchWebhooks reads the delivery log of a provider at baseURL: every webhook
// event it queued, and whether it was delivered or given up.
func FetchWebhooks(ctx context.Context, baseURL string) ([]WebhookDelivery, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/_webhooks", nil)
	if err != nil {
		return nil, err
	}
	resp, err := oracleClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch webhooks: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch webhooks: status %d", resp.StatusCode)
	}
	var out []WebhookDelivery
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode webhooks: %w", err)
	}
	return out, nil
}
