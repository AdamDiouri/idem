// Package ledger posts double-entry transactions. Amounts are signed cents:
// debits positive, credits negative; every transaction sums to zero. The
// database enforces the same rules (deferred balance check, append-only
// triggers), so a bug here fails loudly instead of corrupting the books.
package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/AdamDiouri/idem/internal/store/db"
)

// Account codes (seeded by migration 00002).
const (
	ProviderReceivable = "provider_receivable"
	MerchantPayable    = "merchant_payable"
	PlatformRevenue    = "platform_revenue"
)

// Currency is the only currency in milestone 1.
const Currency = "EUR"

// ErrUnbalanced means a transaction was rejected before reaching the database.
var ErrUnbalanced = errors.New("ledger transaction is not balanced")

// Entry is one leg of a transaction.
type Entry struct {
	Account     string
	AmountCents int64
}

// Txn is a set of entries posted together for one step.
type Txn struct {
	WorkflowID uuid.UUID
	StepID     uuid.UUID
	Kind       string // charge | payout | refund
	Entries    []Entry
}

// Post writes t using q, which must be bound to the caller's transaction so the
// posting commits or rolls back together with the step's state change (D2).
func Post(ctx context.Context, q *db.Queries, t Txn) error {
	if err := validate(t.Entries); err != nil {
		return err
	}

	txnID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("new ledger transaction id: %w", err)
	}
	if err := q.InsertLedgerTransaction(ctx, db.InsertLedgerTransactionParams{
		ID:         txnID,
		WorkflowID: t.WorkflowID,
		StepID:     t.StepID,
		Kind:       t.Kind,
	}); err != nil {
		return fmt.Errorf("insert ledger transaction: %w", err)
	}

	for _, e := range t.Entries {
		entryID, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("new ledger entry id: %w", err)
		}
		if err := q.InsertLedgerEntry(ctx, db.InsertLedgerEntryParams{
			ID:            entryID,
			TransactionID: txnID,
			Account:       e.Account,
			AmountCents:   e.AmountCents,
			Currency:      Currency,
		}); err != nil {
			return fmt.Errorf("insert ledger entry: %w", err)
		}
	}
	return nil
}

func validate(entries []Entry) error {
	if len(entries) < 2 {
		return fmt.Errorf("%w: need at least 2 entries, got %d", ErrUnbalanced, len(entries))
	}
	var total int64
	for _, e := range entries {
		if e.AmountCents == 0 {
			return fmt.Errorf("%w: zero-amount entry on %s", ErrUnbalanced, e.Account)
		}
		total += e.AmountCents
	}
	if total != 0 {
		return fmt.Errorf("%w: entries sum to %d", ErrUnbalanced, total)
	}
	return nil
}

// ChargeEntries records a successful charge: the provider now holds the full
// amount for us, which we owe to the merchant minus the platform fee.
func ChargeEntries(amountCents, feeCents int64) []Entry {
	entries := []Entry{
		{Account: ProviderReceivable, AmountCents: amountCents},
		{Account: MerchantPayable, AmountCents: -(amountCents - feeCents)},
	}
	if feeCents != 0 {
		entries = append(entries, Entry{Account: PlatformRevenue, AmountCents: -feeCents})
	}
	return entries
}

// PayoutEntries records a successful payout: the merchant is paid what we owed
// them, from the money the provider held for us.
func PayoutEntries(amountCents, feeCents int64) []Entry {
	net := amountCents - feeCents
	return []Entry{
		{Account: MerchantPayable, AmountCents: net},
		{Account: ProviderReceivable, AmountCents: -net},
	}
}

// RefundEntries records a successful refund of the full charge: it reverses the
// charge's postings, including the platform fee.
func RefundEntries(amountCents, feeCents int64) []Entry {
	entries := []Entry{{Account: MerchantPayable, AmountCents: amountCents - feeCents}}
	if feeCents != 0 {
		entries = append(entries, Entry{Account: PlatformRevenue, AmountCents: feeCents})
	}
	return append(entries, Entry{Account: ProviderReceivable, AmountCents: -amountCents})
}
