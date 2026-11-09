// Package provider is the engine's client for the payment provider. Its main
// job is classifying every outcome (design §9.3): if the request might have
// reached the provider and we cannot prove what happened, it is ambiguous.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	maxResponseBytes = 1 << 20
	maxRetryAfter    = time.Hour
)

// Class is the engine-relevant meaning of a provider response.
type Class int

const (
	// Success: the effect happened and we know its provider reference.
	Success Class = iota
	// Permanent: the provider definitively refused; no effect happened.
	Permanent
	// Ambiguous: the effect may or may not have happened. Never treat as failure.
	Ambiguous
	// NoEffect: this request provably did not reach the provider (refused
	// connection, DNS failure) or was rejected before processing (429).
	NoEffect
)

func (c Class) String() string {
	switch c {
	case Success:
		return "success"
	case Permanent:
		return "permanent"
	case Ambiguous:
		return "ambiguous"
	case NoEffect:
		return "no_effect"
	}
	return fmt.Sprintf("Class(%d)", int(c))
}

// Outcome is the classified result of one provider call.
type Outcome struct {
	Class       Class
	ProviderRef string
	HTTPStatus  int
	InFlight    bool          // 409: the same key is being processed right now
	RetryAfter  time.Duration // from a Retry-After header (seconds form), capped at 1h
	Err         error         // set for every non-success outcome
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
		return Outcome{Class: Ambiguous, Err: fmt.Errorf("marshal charge: %w", err)}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/charges", bytes.NewReader(body))
	if err != nil {
		return Outcome{Class: Ambiguous, Err: fmt.Errorf("build request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", idempotencyKey)

	status, header, respBody, out, ok := c.do(httpReq, "charge request")
	if !ok {
		return out
	}
	return classifyCharge(status, header, respBody, req)
}

// LookupCharge asks the provider what happened to the charge sent with
// idempotencyKey. found is false when the provider has no record (404) or the
// lookup itself failed; Class tells the two apart (Permanent vs Ambiguous).
func (c *Client) LookupCharge(ctx context.Context, idempotencyKey string, want ChargeRequest) (Outcome, bool) {
	u := c.baseURL + "/charges?idempotency_key=" + url.QueryEscape(idempotencyKey)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Outcome{Class: Ambiguous, Err: fmt.Errorf("build lookup: %w", err)}, false
	}

	status, header, respBody, out, ok := c.do(httpReq, "charge lookup")
	if !ok {
		// A failed lookup tells us nothing about the charge itself.
		out.Class = Ambiguous
		return out, false
	}
	switch status {
	case http.StatusNotFound:
		return Outcome{Class: Permanent, HTTPStatus: status, Err: errors.New("idempotency key unknown to provider")}, false
	case http.StatusOK, http.StatusPaymentRequired:
		return classifyCharge(status, header, respBody, want), true
	default:
		out := classifyCharge(status, header, respBody, want)
		if out.Class != Ambiguous {
			out = Outcome{Class: Ambiguous, HTTPStatus: status, Err: fmt.Errorf("lookup returned %d", status)}
		}
		return out, false
	}
}

// do sends req. ok is false when no response was received; out then holds the
// classified transport failure.
func (c *Client) do(req *http.Request, what string) (int, http.Header, []byte, Outcome, bool) {
	// The transport may silently resend this request on a new connection (it
	// treats requests with an Idempotency-Key as replayable). Then the final
	// error describes only the last try, so record whether ANY try wrote the
	// request to a connection: if one did, the provider may have processed it.
	var wrote atomic.Bool
	trace := &httptrace.ClientTrace{WroteHeaders: func() { wrote.Store(true) }}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := c.http.Do(req)
	if err != nil {
		class := Ambiguous // timeouts, resets, cancellations: may have been processed
		if !wrote.Load() && notSent(err) {
			class = NoEffect
		}
		return 0, nil, nil, Outcome{Class: class, Err: fmt.Errorf("%s: %w", what, err)}, false
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return 0, nil, nil, Outcome{Class: Ambiguous, HTTPStatus: resp.StatusCode, Err: fmt.Errorf("read response: %w", err)}, false
	}
	return resp.StatusCode, resp.Header, body, Outcome{}, true
}

// notSent reports whether err is a connection-establishment failure (refused
// dial, DNS). On its own this is not proof: callers must also know that no
// earlier try wrote the request (see do).
func notSent(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

func classifyCharge(status int, header http.Header, body []byte, want ChargeRequest) Outcome {
	out := Outcome{HTTPStatus: status}

	switch {
	case status >= 200 && status < 300:
		var r struct {
			ID          string `json:"id"`
			Status      string `json:"status"`
			AmountCents int64  `json:"amount_cents"`
			Currency    string `json:"currency"`
		}
		switch {
		case json.Unmarshal(body, &r) != nil:
			out.Class, out.Err = Ambiguous, fmt.Errorf("unparseable %d response", status)
		case r.ID == "" || r.Status != "succeeded":
			out.Class, out.Err = Ambiguous, fmt.Errorf("unexpected charge response: id=%q status=%q", r.ID, r.Status)
		case r.AmountCents != want.AmountCents:
			out.Class, out.Err = Ambiguous, fmt.Errorf("charge amount mismatch: requested %d, provider reports %d", want.AmountCents, r.AmountCents)
		case r.Currency != "" && r.Currency != want.Currency:
			out.Class, out.Err = Ambiguous, fmt.Errorf("charge currency mismatch: requested %s, provider reports %s", want.Currency, r.Currency)
		default:
			out.Class, out.ProviderRef = Success, r.ID
		}

	case status == http.StatusBadRequest || status == http.StatusPaymentRequired:
		out.Class, out.Err = Permanent, fmt.Errorf("provider refused charge: %d %s", status, errorCode(body))

	case status == http.StatusUnprocessableEntity:
		code := errorCode(body)
		if code == "idempotency_key_reused" {
			// Our bug: same key, different body. We cannot claim nothing happened.
			out.Class, out.Err = Ambiguous, errors.New("provider reports idempotency key reused with a different body")
		} else {
			out.Class, out.Err = Permanent, fmt.Errorf("provider refused charge: %d %s", status, code)
		}

	case status == http.StatusConflict:
		out.Class, out.InFlight = Ambiguous, true
		out.Err = errors.New("provider is still processing this idempotency key")

	case status == http.StatusTooManyRequests:
		out.Class, out.RetryAfter = NoEffect, retryAfter(header)
		out.Err = fmt.Errorf("provider rate limited the request (retry after %s)", out.RetryAfter)

	default:
		// 5xx and anything unexpected.
		out.Class, out.Err = Ambiguous, fmt.Errorf("provider returned %d %s", status, errorCode(body))
	}
	return out
}

// retryAfter parses the delta-seconds form of Retry-After. Anything else
// (absent, negative, HTTP-date) yields 0; large values are capped.
func retryAfter(h http.Header) time.Duration {
	secs, err := strconv.ParseInt(h.Get("Retry-After"), 10, 64)
	if err != nil || secs <= 0 {
		return 0
	}
	if secs > int64(maxRetryAfter/time.Second) {
		return maxRetryAfter
	}
	return time.Duration(secs) * time.Second
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
