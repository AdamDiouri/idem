-- name: UnbalancedLedgerTransactions :many
SELECT t.id, t.workflow_id, COALESCE(sum(e.amount_cents), 0)::bigint AS total
FROM ledger_transactions t
LEFT JOIN ledger_entries e ON e.transaction_id = t.id
GROUP BY t.id, t.workflow_id
HAVING COALESCE(sum(e.amount_cents), 0) <> 0;

-- name: LedgerGlobalSum :one
SELECT COALESCE(sum(amount_cents), 0)::bigint AS total FROM ledger_entries;
