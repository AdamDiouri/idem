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

-- name: UnsettledSteps :many
SELECT workflow_id, name, status FROM steps
WHERE status IN ('RUNNING', 'IN_DOUBT', 'AWAITING_CALLBACK') ORDER BY created_at;

-- name: ConflictEvents :many
SELECT i.provider_event_id, s.workflow_id, COALESCE(i.note, '')::text AS note
FROM webhook_inbox i JOIN steps s ON s.id = i.step_id
WHERE i.status = 'CONFLICT' ORDER BY i.received_at;

-- name: PendingInboxEvents :many
SELECT provider_event_id, received_at FROM webhook_inbox WHERE status = 'PENDING' ORDER BY received_at;

-- name: InboxRowsForCheck :many
-- Every inbox row with the status of the workflow it was tied to (if any),
-- for checking against the provider's delivery log (I10, I11).
SELECT i.provider_event_id, i.status, COALESCE(i.note, '')::text AS note, w.status AS workflow_status
FROM webhook_inbox i
LEFT JOIN steps s ON s.id = i.step_id
LEFT JOIN workflows w ON w.id = s.workflow_id
ORDER BY i.received_at;
