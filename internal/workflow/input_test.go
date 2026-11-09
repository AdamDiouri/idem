package workflow_test

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AdamDiouri/idem/internal/workflow"
)

var defaultFees = workflow.FeeConfig{RateBps: 150, FixedCents: 25}

func validReq() workflow.CreateWorkflowRequest {
	return workflow.CreateWorkflowRequest{
		AmountCents: 10000,
		Currency:    "EUR",
		CustomerID:  "cus_1",
		MerchantID:  "mer_1",
	}
}

func TestNewPaymentInput_Valid(t *testing.T) {
	in, err := workflow.NewPaymentInput(validReq(), defaultFees)
	require.NoError(t, err)
	require.Equal(t, workflow.PaymentInput{
		AmountCents: 10000,
		FeeCents:    175,
		Currency:    "EUR",
		CustomerID:  "cus_1",
		MerchantID:  "mer_1",
	}, in)
}

func TestNewPaymentInput_SmallestAmountCoveringFee(t *testing.T) {
	req := validReq()
	req.AmountCents = 26 // fee 25 → net 1
	in, err := workflow.NewPaymentInput(req, defaultFees)
	require.NoError(t, err)
	require.Equal(t, int64(25), in.FeeCents)
}

func TestNewPaymentInput_Invalid(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*workflow.CreateWorkflowRequest)
		field   string
		message string
	}{
		{"zero amount", func(r *workflow.CreateWorkflowRequest) { r.AmountCents = 0 }, "amount_cents", ""},
		{"negative amount", func(r *workflow.CreateWorkflowRequest) { r.AmountCents = -1 }, "amount_cents", ""},
		{"amount above max", func(r *workflow.CreateWorkflowRequest) { r.AmountCents = 100_000_001 }, "amount_cents", ""},
		{"amount equals fee", func(r *workflow.CreateWorkflowRequest) { r.AmountCents = 25 }, "amount_cents", "amount does not cover fee"},
		{"lowercase currency", func(r *workflow.CreateWorkflowRequest) { r.Currency = "eur" }, "currency", ""},
		{"other currency", func(r *workflow.CreateWorkflowRequest) { r.Currency = "USD" }, "currency", ""},
		{"empty currency", func(r *workflow.CreateWorkflowRequest) { r.Currency = "" }, "currency", ""},
		{"empty customer", func(r *workflow.CreateWorkflowRequest) { r.CustomerID = "" }, "customer_id", ""},
		{"long customer", func(r *workflow.CreateWorkflowRequest) { r.CustomerID = strings.Repeat("a", 65) }, "customer_id", ""},
		{"customer with space", func(r *workflow.CreateWorkflowRequest) { r.CustomerID = "cus 1" }, "customer_id", ""},
		{"customer non-ascii", func(r *workflow.CreateWorkflowRequest) { r.CustomerID = "cüs" }, "customer_id", ""},
		{"empty merchant", func(r *workflow.CreateWorkflowRequest) { r.MerchantID = "" }, "merchant_id", ""},
		{"long merchant", func(r *workflow.CreateWorkflowRequest) { r.MerchantID = strings.Repeat("m", 65) }, "merchant_id", ""},
		{"merchant with space", func(r *workflow.CreateWorkflowRequest) { r.MerchantID = "mer 1" }, "merchant_id", ""},
		{"merchant non-ascii", func(r *workflow.CreateWorkflowRequest) { r.MerchantID = "mér" }, "merchant_id", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := validReq()
			c.mutate(&req)
			_, err := workflow.NewPaymentInput(req, defaultFees)
			var ve *workflow.ValidationError
			require.True(t, errors.As(err, &ve), "expected *ValidationError, got %v", err)
			require.Equal(t, c.field, ve.Field)
			if c.message != "" {
				require.Equal(t, c.message, ve.Message)
			}
		})
	}
}

func TestNewPaymentInput_MaxLengthIDsAccepted(t *testing.T) {
	req := validReq()
	req.CustomerID = strings.Repeat("a", 64)
	req.MerchantID = "mer_A-9"
	_, err := workflow.NewPaymentInput(req, defaultFees)
	require.NoError(t, err)
}

// Review I2: a misconfigured fee must never produce a negative or overflowed
// fee that slips past the "amount covers fee" check. It is a server fault, not
// a client validation error.
func TestNewPaymentInput_RejectsInvalidFeeConfig(t *testing.T) {
	for _, fees := range []workflow.FeeConfig{
		{RateBps: 0, FixedCents: -5},
		{RateBps: 150, FixedCents: math.MaxInt64},
		{RateBps: -150, FixedCents: 0},
	} {
		_, err := workflow.NewPaymentInput(validReq(), fees)
		require.Error(t, err, "fees %+v", fees)
		var ve *workflow.ValidationError
		require.False(t, errors.As(err, &ve), "fees %+v must not be reported as a client error", fees)
	}
}

func TestFingerprint_IgnoresFormatting(t *testing.T) {
	var a, b workflow.CreateWorkflowRequest
	require.NoError(t, json.Unmarshal(
		[]byte(`{"amount_cents":10000,"currency":"EUR","customer_id":"c","merchant_id":"m"}`), &a))
	require.NoError(t, json.Unmarshal(
		[]byte("{ \"merchant_id\": \"m\",\n \"customer_id\":\"c\", \"currency\":\"EUR\", \"amount_cents\": 10000 }"), &b))
	require.Equal(t, workflow.Fingerprint(a), workflow.Fingerprint(b))
	require.Len(t, workflow.Fingerprint(a), 64)

	b.AmountCents = 10001
	require.NotEqual(t, workflow.Fingerprint(a), workflow.Fingerprint(b))
}
