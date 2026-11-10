package webhook

// Provider event types (M2 design §4.1).
const (
	EventPayoutPaid   = "payout.paid"
	EventPayoutFailed = "payout.failed"
)

// ProviderEvent is a webhook from the payment provider.
type ProviderEvent struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Created int64           `json:"created"` // unix seconds
	Data    PayoutEventData `json:"data"`
}

// PayoutEventData describes the payout an event is about.
type PayoutEventData struct {
	PayoutID       string `json:"payout_id"`
	Status         string `json:"status"` // paid | failed
	FailureCode    string `json:"failure_code,omitempty"`
	AmountCents    int64  `json:"amount_cents"`
	Currency       string `json:"currency"`
	IdempotencyKey string `json:"idempotency_key"`
	Metadata       struct {
		WorkflowID string `json:"workflow_id"`
	} `json:"metadata"`
}
