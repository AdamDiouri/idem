// Package provider is the engine's client for the payment provider. Its main
// job is classifying every outcome as success, permanent failure, or ambiguous
// (design §9.3): if the request might have reached the provider and we cannot
// prove what happened, the outcome is ambiguous.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const maxResponseBytes = 1 << 20

// Class is the engine-relevant meaning of a provider response.
type Class int

const (
	// Success: the effect happened and we know its provider reference.
	Success Class = iota
	// Permanent: the provider definitively refused; no effect happened.
	Permanent
	// Ambiguous: the effect may or may not have happened. Never treat as failure.
	Ambiguous
)

func (c Class) String() string {
	switch c {
	case Success:
		return "success"
	case Permanent:
		return "permanent"
	case Ambiguous:
		return "ambiguous"
	}
	return fmt.Sprintf("Class(%d)", int(c))
}

// Outcome is the classified result of one provider call.
type Outcome struct {
	Class       Class
	ProviderRef string
	HTTPStatus  int
	Err         error // set for every non-success outcome
}

// ChargeRequest is the body of POST /charges.
type ChargeRequest struct {
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	CustomerID  string    `json:"customer_id"`
	WorkflowID  uuid.UUID `json:"workflow_id"`
}

// Client calls the payment provider.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient returns a client whose every call is bounded by timeout.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: timeout}}
}

// CreateCharge asks the provider to charge the customer, using idempotencyKey
// so that retries of the same intended charge are deduplicated by the provider.
func (c *Client) CreateCharge(ctx context.Context, idempotencyKey string, req ChargeRequest) Outcome {
	body, err := json.Marshal(req)
	if err != nil {
		// Nothing was sent, but marshalling a fixed struct cannot fail; stay conservative.
		return Outcome{Class: Ambiguous, Err: fmt.Errorf("marshal charge: %w", err)}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/charges", bytes.NewReader(body))
	if err != nil {
		return Outcome{Class: Ambiguous, Err: fmt.Errorf("build request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		// Timeouts, resets and cancellations: the request may have been processed.
		return Outcome{Class: Ambiguous, Err: fmt.Errorf("charge request: %w", err)}
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return Outcome{Class: Ambiguous, HTTPStatus: resp.StatusCode, Err: fmt.Errorf("read response: %w", err)}
	}
	return classifyCharge(resp.StatusCode, respBody, req.AmountCents)
}

func classifyCharge(status int, body []byte, wantAmount int64) Outcome {
	out := Outcome{HTTPStatus: status}

	switch {
	case status >= 200 && status < 300:
		var r struct {
			ID          string `json:"id"`
			Status      string `json:"status"`
			AmountCents int64  `json:"amount_cents"`
		}
		switch {
		case json.Unmarshal(body, &r) != nil:
			out.Class, out.Err = Ambiguous, fmt.Errorf("unparseable %d response", status)
		case r.ID == "" || r.Status != "succeeded":
			out.Class, out.Err = Ambiguous, fmt.Errorf("unexpected charge response: id=%q status=%q", r.ID, r.Status)
		case r.AmountCents != wantAmount:
			out.Class, out.Err = Ambiguous, fmt.Errorf("charge amount mismatch: requested %d, provider reports %d", wantAmount, r.AmountCents)
		default:
			out.Class, out.ProviderRef = Success, r.ID
		}

	case status == http.StatusBadRequest || status == http.StatusPaymentRequired:
		out.Class, out.Err = Permanent, fmt.Errorf("provider refused charge: %d %s", status, errorCode(body))

	case status == http.StatusUnprocessableEntity:
		code := errorCode(body)
		if code == "idempotency_key_reused" {
			// Our bug: the same key was sent with a different body. We cannot
			// claim nothing happened, so this must not become a failure.
			out.Class, out.Err = Ambiguous, fmt.Errorf("provider reports idempotency key reused with a different body")
		} else {
			out.Class, out.Err = Permanent, fmt.Errorf("provider refused charge: %d %s", status, code)
		}

	default:
		// 409 in flight, 429, 5xx and anything unexpected.
		out.Class, out.Err = Ambiguous, fmt.Errorf("provider returned %d %s", status, errorCode(body))
	}
	return out
}

func errorCode(body []byte) string {
	var e struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &e) // best effort, for messages only
	return e.Error.Code
}
