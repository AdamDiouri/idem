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

-- name: NeedsAttentionWorkflows :many
SELECT id FROM workflows WHERE status = 'NEEDS_ATTENTION' ORDER BY updated_at;

-- name: NonTerminalWorkflows :many
SELECT id, status, updated_at FROM workflows
WHERE status IN ('PENDING', 'RUNNING', 'COMPENSATING') ORDER BY updated_at;

-- name: KeysWithoutWorkflow :many
SELECT client_id, key FROM api_idempotency_keys
WHERE workflow_id IS NULL OR response_body IS NULL;

-- name: WorkflowsWithoutKey :many
SELECT w.id FROM workflows w
WHERE NOT EXISTS (SELECT 1 FROM api_idempotency_keys k WHERE k.workflow_id = w.id);
