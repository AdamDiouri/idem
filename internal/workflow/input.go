// Package workflow holds the payment_payout workflow's domain rules and the
// transaction that submits new workflows idempotently.
package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/AdamDiouri/idem/internal/money"
)

const (
	// MaxAmountCents caps a single workflow at €1,000,000.
	MaxAmountCents = 100_000_000
	// MaxFeeRateBps is 100%.
	MaxFeeRateBps = 10_000
	// Currency is the only currency accepted in milestone 1 (design D10).
	Currency = "EUR"
)

var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// CreateWorkflowRequest is the client's POST /workflows body. The field order
// is fixed: Fingerprint relies on encoding/json marshalling fields in this order.
type CreateWorkflowRequest struct {
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
	CustomerID  string `json:"customer_id"`
	MerchantID  string `json:"merchant_id"`
}

// FeeConfig is the platform fee applied at workflow creation.
type FeeConfig struct {
	RateBps    int64
	FixedCents int64
}

// PaymentInput is the validated workflow input, stored as workflows.input.
// The fee is fixed here, at creation, so retries and config changes cannot alter it.
type PaymentInput struct {
	AmountCents int64  `json:"amount_cents"`
	FeeCents    int64  `json:"fee_cents"`
	Currency    string `json:"currency"`
	CustomerID  string `json:"customer_id"`
	MerchantID  string `json:"merchant_id"`
}

// ValidationError reports the first request field that failed validation.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Message)
}

// NewPaymentInput validates req and computes its fee.
func NewPaymentInput(req CreateWorkflowRequest, fees FeeConfig) (PaymentInput, error) {
	if req.AmountCents < 1 || req.AmountCents > MaxAmountCents {
		return PaymentInput{}, &ValidationError{"amount_cents", fmt.Sprintf("must be between 1 and %d", MaxAmountCents)}
	}
	if req.Currency != Currency {
		return PaymentInput{}, &ValidationError{"currency", `must be "EUR"`}
	}
	if !idPattern.MatchString(req.CustomerID) {
		return PaymentInput{}, &ValidationError{"customer_id", "must be 1-64 characters of A-Z, a-z, 0-9, '_' or '-'"}
	}
	if !idPattern.MatchString(req.MerchantID) {
		return PaymentInput{}, &ValidationError{"merchant_id", "must be 1-64 characters of A-Z, a-z, 0-9, '_' or '-'"}
	}

	// Bounds keep money.Fee free of overflow and the fee non-negative. A bad
	// config is a server fault, so it is not reported as a ValidationError.
	if fees.RateBps < 0 || fees.RateBps > MaxFeeRateBps || fees.FixedCents < 0 || fees.FixedCents > MaxAmountCents {
		return PaymentInput{}, fmt.Errorf("invalid fee config %+v", fees)
	}
	fee := money.Fee(req.AmountCents, fees.RateBps, fees.FixedCents)
	if req.AmountCents-fee < 1 {
		return PaymentInput{}, &ValidationError{"amount_cents", "amount does not cover fee"}
	}

	return PaymentInput{
		AmountCents: req.AmountCents,
		FeeCents:    fee,
		Currency:    req.Currency,
		CustomerID:  req.CustomerID,
		MerchantID:  req.MerchantID,
	}, nil
}

// Fingerprint identifies a request's content independently of JSON formatting:
// hex(sha256(json.Marshal(req))). Two requests with the same values but different
// whitespace or key order produce the same fingerprint.
func Fingerprint(req CreateWorkflowRequest) string {
	b, err := json.Marshal(req)
	if err != nil {
		// Marshalling a struct of ints and strings cannot fail.
		panic(fmt.Sprintf("marshal request: %v", err))
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
