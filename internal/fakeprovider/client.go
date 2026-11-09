package fakeprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// FetchEffects reads the effects oracle of a provider running at baseURL.
func FetchEffects(ctx context.Context, baseURL string) ([]Effect, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/_effects", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
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
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("put faults: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("put faults: status %d", resp.StatusCode)
	}
	return nil
}
