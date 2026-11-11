package fakeshop

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// oracleClient reads and configures the fake shop. Every call is bounded, so
// a hung shop cannot stall a chaos run's settle phase.
var oracleClient = &http.Client{Timeout: 10 * time.Second}

// PutFaults replaces the shop's fault rules.
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
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		return fmt.Errorf("fake shop refused faults: %d %s", resp.StatusCode, msg)
	}
	return nil
}

// FetchSeen returns what the shop learned.
func FetchSeen(ctx context.Context, baseURL string) (Seen, error) {
	var s Seen
	return s, get(ctx, baseURL+"/_seen", &s)
}

// FetchFaultHits returns how many deliveries each fault mode hit.
func FetchFaultHits(ctx context.Context, baseURL string) (map[string]int, error) {
	hits := map[string]int{}
	return hits, get(ctx, baseURL+"/_faults/hits", &hits)
}

func get(ctx context.Context, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := oracleClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}
