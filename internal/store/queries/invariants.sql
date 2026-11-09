-- name: UnbalancedLedgerTransactions :many
SELECT t.id, t.workflow_id, COALESCE(sum(e.amount_cents), 0)::bigint AS total
FROM ledger_transactions t
LEFT JOIN ledger_entries e ON e.transaction_id = t.id
GROUP BY t.id, t.workflow_id
HAVING COALESCE(sum(e.amount_cents), 0) <> 0;

-- name: LedgerGlobalSum :one
SELECT COALESCE(sum(amount_cents), 0)::bigint AS total FROM ledger_entries;

-- name: LedgerTransactionsSummary :many
-- One row per ledger transaction with the sum of its debits, which equals the
-- amount of money that moved at the provider for every kind:
-- charge +amount; payout +(amount-fee); refund (amount-fee)+fee.
SELECT t.workflow_id, t.kind,
       COALESCE(sum(e.amount_cents) FILTER (WHERE e.amount_cents > 0), 0)::bigint AS debits
FROM ledger_transactions t
LEFT JOIN ledger_entries e ON e.transaction_id = t.id
GROUP BY t.id, t.workflow_id, t.kind;

-- name: TerminalWorkflows :many
SELECT id, status FROM workflows WHERE status IN ('SUCCEEDED', 'FAILED', 'COMPENSATED');
