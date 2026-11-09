package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AdamDiouri/idem/internal/store"
	"github.com/AdamDiouri/idem/internal/store/db"
)

const (
	typePaymentPayout = "payment_payout"
	statusPending     = "PENDING"
	stepCharge        = "charge"
	chargeMaxAttempts = 8
)

var (
	// ErrKeyReuseMismatch means the idempotency key was already used with a different request body.
	ErrKeyReuseMismatch = errors.New("idempotency key reused with a different request")
	// ErrKeyInUse means another request with the same key is still in flight and did not finish within the lock timeout.
	ErrKeyInUse = errors.New("idempotency key is in use by a concurrent request")
)

// SubmitRequest is a decoded, not yet validated, workflow submission.
// Validation happens inside Submit, only when the key is new: a replay must
// return the stored response even if validation rules or fees changed since.
type SubmitRequest struct {
	ClientID       string
	IdempotencyKey string
	Request        CreateWorkflowRequest
}

// SubmitResult is the response to return to the client. Body is byte-identical
// across the original request and every replay.
type SubmitResult struct {
	Body     json.RawMessage
	Replayed bool
}

// Created is the response body stored with the idempotency key and returned to clients.
type Created struct {
	WorkflowID  uuid.UUID `json:"workflow_id"`
	Status      string    `json:"status"`
	AmountCents int64     `json:"amount_cents"`
	FeeCents    int64     `json:"fee_cents"`
	Currency    string    `json:"currency"`
	CreatedAt   time.Time `json:"created_at"`
}

// Submitter creates workflows idempotently.
type Submitter struct {
	pool        *pgxpool.Pool
	lockTimeout time.Duration
	fees        FeeConfig

	beforeCommit func() error // test hook; nil in production
}

// NewSubmitter returns a Submitter. lockTimeout bounds how long a request waits
// for a concurrent request holding the same idempotency key; fees apply to new workflows only.
func NewSubmitter(pool *pgxpool.Pool, lockTimeout time.Duration, fees FeeConfig) *Submitter {
	return &Submitter{pool: pool, lockTimeout: lockTimeout, fees: fees}
}

// Submit creates the workflow, its first step and the stored response in one
// transaction, or replays the stored response if the key was already used with
// the same request. Nothing is visible to other transactions until COMMIT.
//
// A new request that fails validation returns *ValidationError and rolls back,
// so the key is not reserved and the client may fix the body and reuse it.
func (s *Submitter) Submit(ctx context.Context, req SubmitRequest) (SubmitResult, error) {
	fingerprint := Fingerprint(req.Request)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SubmitResult{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	// SET cannot take bind parameters; the value is an integer we format ourselves.
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", s.lockTimeout.Milliseconds())); err != nil {
		return SubmitResult{}, fmt.Errorf("set lock_timeout: %w", err)
	}

	q := db.New(tx)
	inserted, err := q.InsertIdempotencyKey(ctx, db.InsertIdempotencyKeyParams{
		ClientID:           req.ClientID,
		Key:                req.IdempotencyKey,
		RequestFingerprint: fingerprint,
	})
	if store.IsLockTimeout(err) {
		return SubmitResult{}, ErrKeyInUse
	}
	if err != nil {
		return SubmitResult{}, fmt.Errorf("reserve idempotency key: %w", err)
	}

	if inserted == 0 {
		return s.replay(ctx, q, req.ClientID, req.IdempotencyKey, fingerprint)
	}

	input, err := NewPaymentInput(req.Request, s.fees)
	if err != nil {
		return SubmitResult{}, err // rollback releases the key
	}

	body, err := s.create(ctx, q, req.ClientID, req.IdempotencyKey, input)
	if err != nil {
		return SubmitResult{}, err
	}

	if s.beforeCommit != nil {
		if err := s.beforeCommit(); err != nil {
			return SubmitResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return SubmitResult{}, fmt.Errorf("commit: %w", err)
	}
	return SubmitResult{Body: body, Replayed: false}, nil
}

func (s *Submitter) replay(ctx context.Context, q *db.Queries, clientID, key, fingerprint string) (SubmitResult, error) {
	row, err := q.GetIdempotencyKey(ctx, db.GetIdempotencyKeyParams{ClientID: clientID, Key: key})
	if err != nil {
		return SubmitResult{}, fmt.Errorf("load idempotency key: %w", err)
	}
	if row.RequestFingerprint != fingerprint {
		return SubmitResult{}, ErrKeyReuseMismatch
	}
	if row.ResponseBody == nil {
		// The key row and its response are written in the same transaction,
		// so a committed key without a response means an invariant was broken.
		return SubmitResult{}, fmt.Errorf("idempotency key %q has no stored response", key)
	}
	return SubmitResult{Body: row.ResponseBody, Replayed: true}, nil
}

func (s *Submitter) create(ctx context.Context, q *db.Queries, clientID, key string, in PaymentInput) (json.RawMessage, error) {
	workflowID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("new workflow id: %w", err)
	}
	stepID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("new step id: %w", err)
	}

	input, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("marshal input: %w", err)
	}

	createdAt, err := q.InsertWorkflow(ctx, db.InsertWorkflowParams{
		ID:       workflowID,
		ClientID: clientID,
		Type:     typePaymentPayout,
		Status:   statusPending,
		Input:    input,
	})
	if err != nil {
		return nil, fmt.Errorf("insert workflow: %w", err)
	}

	if err := q.InsertStep(ctx, db.InsertStepParams{
		ID:             stepID,
		WorkflowID:     workflowID,
		Name:           stepCharge,
		Generation:     0,
		Status:         statusPending,
		IdempotencyKey: StepIdempotencyKey(workflowID, stepCharge, 0),
		MaxAttempts:    chargeMaxAttempts,
	}); err != nil {
		return nil, fmt.Errorf("insert charge step: %w", err)
	}

	body, err := json.Marshal(Created{
		WorkflowID:  workflowID,
		Status:      statusPending,
		AmountCents: in.AmountCents,
		FeeCents:    in.FeeCents,
		Currency:    in.Currency,
		CreatedAt:   createdAt.Time.UTC(),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal response: %w", err)
	}

	if err := q.SetIdempotencyKeyResponse(ctx, db.SetIdempotencyKeyResponseParams{
		WorkflowID:   &workflowID,
		ResponseBody: body,
		ClientID:     clientID,
		Key:          key,
	}); err != nil {
		return nil, fmt.Errorf("store response: %w", err)
	}
	return body, nil
}
