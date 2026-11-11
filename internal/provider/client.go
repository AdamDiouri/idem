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
	// Accepted: the provider took the request (202) and will settle it later;
	// ProviderRef identifies it. Async payouts only (design §4.2).
	Accepted
	// Pending: a status poll found the request accepted but not settled yet.
	Pending
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
	case Accepted:
		return "accepted"
	case Pending:
		return "pending"
	}
	return fmt.Sprintf("Class(%d)", int(c))
}

// Outcome is the classified result of one provider call.
type Outcome struct {
	Class       Class
	ProviderRef string
	HTTPStatus  int
	InFlight    bool          // 409: the same key is being processed right now
	RetryAfter  time.Duration // from a Retry-After header (seconds or HTTP date), capped at 1h
	Err         error         // set for every non-success outcome
	// Code is the provider's reason for a Permanent refusal (e.g. card_declined),
	// as the provider sent it; empty otherwise. Untrusted text: whoever shows it
	// to a client must validate it.
	Code string
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

// PayoutRequest is the body of POST /payouts.
type PayoutRequest struct {
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	MerchantID  string    `json:"merchant_id"`
	WorkflowID  uuid.UUID `json:"workflow_id"`
}

// RefundRequest is the body of POST /refunds.
type RefundRequest struct {
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	ChargeID    string    `json:"charge_id"`
	WorkflowID  uuid.UUID `json:"workflow_id"`
}

// expect is what a successful response must report to count as Success.
type expect struct {
	AmountCents   int64
	Currency      string
	SuccessStatus string
	// AcceptedStatus, when set, is the status of an accepted-but-unsettled
	// request: a 202 with it is Accepted (as is a 200 with it from a lookup).
	AcceptedStatus string
	acceptOnOK     bool
}

// CreateCharge asks the provider to charge the customer, using idempotencyKey
// so that retries of the same intended charge are deduplicated by the provider.
func (c *Client) CreateCharge(ctx context.Context, idempotencyKey string, req ChargeRequest) Outcome {
	return c.create(ctx, "/charges", idempotencyKey, req, expect{AmountCents: req.AmountCents, Currency: req.Currency, SuccessStatus: "succeeded"})
}

// LookupCharge asks what happened to the charge sent with idempotencyKey.
// found is false when the provider has no record (404, Class Permanent) or
// the lookup itself failed (Class Ambiguous).
func (c *Client) LookupCharge(ctx context.Context, idempotencyKey string, want ChargeRequest) (Outcome, bool) {
	return c.lookup(ctx, "/charges", idempotencyKey, expect{AmountCents: want.AmountCents, Currency: want.Currency, SuccessStatus: "succeeded"})
}

// CreatePayout asks the provider to pay the merchant.
func (c *Client) CreatePayout(ctx context.Context, idempotencyKey string, req PayoutRequest) Outcome {
	return c.create(ctx, "/payouts", idempotencyKey, req, payoutExpect(req))
}

func payoutExpect(req PayoutRequest) expect {
	return expect{AmountCents: req.AmountCents, Currency: req.Currency, SuccessStatus: "paid", AcceptedStatus: "pending"}
}

// GetPayout polls the current state of an accepted payout (design §4.5):
// paid → Success, failed → Permanent (Err carries the failure code), pending →
// Pending. A poll sends nothing, so every other answer (404, garbage, a
// mismatch, a transport failure) only means "we still don't know".
func (c *Client) GetPayout(ctx context.Context, payoutID string, want PayoutRequest) Outcome {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/payouts/"+url.PathEscape(payoutID), nil)
	if err != nil {
		return Outcome{Class: Ambiguous, Err: fmt.Errorf("build poll: %w", err)}
	}
	status, _, body, out, ok := c.do(httpReq, "GET /payouts/{id}")
	if !ok {
		return out
	}
	if status != http.StatusOK {
		return Outcome{Class: Ambiguous, HTTPStatus: status, Err: fmt.Errorf("payout poll returned %d %s", status, errorCode(body))}
	}
	var r struct {
		ID          string `json:"id"`
		Status      string `json:"status"`
		FailureCode string `json:"failure_code"`
		AmountCents int64  `json:"amount_cents"`
		Currency    string `json:"currency"`
	}
	out = Outcome{HTTPStatus: status}
	switch {
	case json.Unmarshal(body, &r) != nil || r.ID != payoutID:
		out.Class, out.Err = Ambiguous, fmt.Errorf("unexpected payout poll response: id=%q", r.ID)
	case r.AmountCents != want.AmountCents || (r.Currency != "" && r.Currency != want.Currency):
		out.Class, out.Err = Ambiguous, fmt.Errorf("payout amount mismatch: requested %d %s, provider reports %d %s",
			want.AmountCents, want.Currency, r.AmountCents, r.Currency)
	case r.Status == "paid":
		out.Class, out.ProviderRef = Success, r.ID
	case r.Status == "failed":
		out.Class, out.Code, out.Err = Permanent, r.FailureCode, fmt.Errorf("payout failed: %s", r.FailureCode)
	case r.Status == "pending":
		out.Class, out.ProviderRef, out.Err = Pending, r.ID, errors.New("payout still pending")
	default:
		out.Class, out.Err = Ambiguous, fmt.Errorf("unknown payout status %q", r.Status)
	}
	return out
}

// LookupPayout asks what happened to the payout sent with idempotencyKey.
func (c *Client) LookupPayout(ctx context.Context, idempotencyKey string, want PayoutRequest) (Outcome, bool) {
	return c.lookup(ctx, "/payouts", idempotencyKey, payoutExpect(want))
}

// CreateRefund asks the provider to return (part of) a charge to the customer.
func (c *Client) CreateRefund(ctx context.Context, idempotencyKey string, req RefundRequest) Outcome {
	return c.create(ctx, "/refunds", idempotencyKey, req, expect{AmountCents: req.AmountCents, Currency: req.Currency, SuccessStatus: "succeeded"})
}

// LookupRefund asks what happened to the refund sent with idempotencyKey.
func (c *Client) LookupRefund(ctx context.Context, idempotencyKey string, want RefundRequest) (Outcome, bool) {
	return c.lookup(ctx, "/refunds", idempotencyKey, expect{AmountCents: want.AmountCents, Currency: want.Currency, SuccessStatus: "succeeded"})
}

func (c *Client) create(ctx context.Context, path, idempotencyKey string, body any, want expect) Outcome {
	b, err := json.Marshal(body)
	if err != nil {
		return Outcome{Class: Ambiguous, Err: fmt.Errorf("marshal request: %w", err)}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return Outcome{Class: Ambiguous, Err: fmt.Errorf("build request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", idempotencyKey)

	status, header, respBody, out, ok := c.do(httpReq, "POST "+path)
	if !ok {
		return out
	}
	return classify(status, header, respBody, want)
}

func (c *Client) lookup(ctx context.Context, path, idempotencyKey string, want expect) (Outcome, bool) {
	u := c.baseURL + path + "?idempotency_key=" + url.QueryEscape(idempotencyKey)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Outcome{Class: Ambiguous, Err: fmt.Errorf("build lookup: %w", err)}, false
	}

	status, header, respBody, out, ok := c.do(httpReq, "GET "+path)
	if !ok {
		// A failed lookup tells us nothing about the request itself.
		out.Class = Ambiguous
		return out, false
	}
	switch status {
	case http.StatusNotFound:
		return Outcome{Class: Permanent, HTTPStatus: status, Err: errors.New("idempotency key unknown to provider")}, false
	case http.StatusOK, http.StatusPaymentRequired, http.StatusUnprocessableEntity:
		// A stored outcome: success, or a definitive refusal (declined card,
		// invalid account, invalid refund); for an async payout, possibly
		// accepted and not settled yet.
		want.acceptOnOK = true
		out := classify(status, header, respBody, want)
		return out, out.Class != Ambiguous
	default:
		out := classify(status, header, respBody, want)
		if out.Class != Ambiguous {
			// Keep Retry-After: a rate-limited lookup must still slow the retry.
			out = Outcome{Class: Ambiguous, HTTPStatus: status, RetryAfter: out.RetryAfter,
				Err: fmt.Errorf("lookup returned %d", status)}
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

func classify(status int, header http.Header, body []byte, want expect) Outcome {
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
		case r.ID != "" && want.AcceptedStatus != "" && r.Status == want.AcceptedStatus &&
			(status == http.StatusAccepted || want.acceptOnOK) &&
			r.AmountCents == want.AmountCents && (r.Currency == "" || r.Currency == want.Currency):
			out.Class, out.ProviderRef = Accepted, r.ID
		case want.acceptOnOK && want.AcceptedStatus != "" && r.ID != "" && r.Status == "failed":
			// A lookup found the accepted request settled failed: a definite refusal.
			out.Class, out.Code = Permanent, failureCode(body)
			out.Err = fmt.Errorf("provider reports %s failed: %s", r.ID, out.Code)
		case r.ID == "" || r.Status != want.SuccessStatus:
			out.Class, out.Err = Ambiguous, fmt.Errorf("unexpected response: id=%q status=%q", r.ID, r.Status)
		case r.AmountCents != want.AmountCents:
			out.Class, out.Err = Ambiguous, fmt.Errorf("amount mismatch: requested %d, provider reports %d", want.AmountCents, r.AmountCents)
		case r.Currency != "" && r.Currency != want.Currency:
			out.Class, out.Err = Ambiguous, fmt.Errorf("currency mismatch: requested %s, provider reports %s", want.Currency, r.Currency)
		default:
			out.Class, out.ProviderRef = Success, r.ID
		}

	case status == http.StatusBadRequest || status == http.StatusPaymentRequired:
		out.Class, out.Code = Permanent, errorCode(body)
		out.Err = fmt.Errorf("provider refused request: %d %s", status, out.Code)

	case status == http.StatusUnprocessableEntity:
		code := errorCode(body)
		if code == "idempotency_key_reused" {
			// Our bug: same key, different body. We cannot claim nothing happened.
			out.Class, out.Err = Ambiguous, errors.New("provider reports idempotency key reused with a different body")
		} else {
			out.Class, out.Code, out.Err = Permanent, code, fmt.Errorf("provider refused request: %d %s", status, code)
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

// retryAfter parses Retry-After in either form (RFC 9110): delta-seconds or
// an HTTP date. Absent, invalid, negative or past values yield 0; large values
// are capped at maxRetryAfter.
func retryAfter(h http.Header) time.Duration {
	v := h.Get("Retry-After")
	var d time.Duration
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		d = time.Until(t)
	}
	return min(max(d, 0), maxRetryAfter)
}

func failureCode(body []byte) string {
	var f struct {
		FailureCode string `json:"failure_code"`
	}
	_ = json.Unmarshal(body, &f) // best effort, for messages only
	return f.FailureCode
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

// BalanceTransaction is one money movement in the provider's report (M2
// design §6): charge (+), payout (−, when paid), refund (−).
type BalanceTransaction struct {
	ID             string
	Type           string
	Amount         int64 // signed, minor units
	Currency       string
	Created        time.Time
	Source         string // the charge, payout or refund it records
	WorkflowID     string // metadata; may name a workflow idem does not know
	IdempotencyKey string
}

// ListBalanceTransactions reads every movement created in [from, to), page by
// page. Any failed or unreadable page is an error: a reconciliation must
// never run on a partial report.
func (c *Client) ListBalanceTransactions(ctx context.Context, from, to time.Time) ([]BalanceTransaction, error) {
	lt := to.Unix()
	if to.After(time.Unix(lt, 0)) {
		lt++ // whole seconds: round the end up so nothing before `to` is left out
	}
	var out []BalanceTransaction
	after := ""
	for {
		q := url.Values{"created_gte": {strconv.FormatInt(from.Unix(), 10)}, "created_lt": {strconv.FormatInt(lt, 10)}, "limit": {"100"}}
		if after != "" {
			q.Set("starting_after", after)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/reports/balance_transactions?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("balance transactions: %w", err)
		}
		var page struct {
			Data []struct {
				ID       string            `json:"id"`
				Type     string            `json:"type"`
				Amount   int64             `json:"amount"`
				Currency string            `json:"currency"`
				Created  int64             `json:"created"`
				Source   string            `json:"source"`
				Metadata map[string]string `json:"metadata"`
			} `json:"data"`
			HasMore bool `json:"has_more"`
		}
		err = func() error {
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("balance transactions: provider answered %d", resp.StatusCode)
			}
			return json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&page)
		}()
		if err != nil {
			return nil, fmt.Errorf("balance transactions: %w", err)
		}
		for _, d := range page.Data {
			out = append(out, BalanceTransaction{ID: d.ID, Type: d.Type, Amount: d.Amount, Currency: d.Currency,
				Created: time.Unix(d.Created, 0), Source: d.Source, WorkflowID: d.Metadata["workflow_id"],
				IdempotencyKey: d.Metadata["idempotency_key"]})
		}
		if !page.HasMore {
			return out, nil
		}
		if len(page.Data) == 0 {
			return nil, errors.New("balance transactions: has_more with an empty page")
		}
		after = page.Data[len(page.Data)-1].ID
	}
}
