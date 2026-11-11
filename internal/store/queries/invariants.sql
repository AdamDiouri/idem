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
SELECT id, status FROM workflows WHERE workflow_is_terminal(status);

-- name: NeedsAttentionWorkflows :many
SELECT id FROM workflows WHERE status = 'NEEDS_ATTENTION' ORDER BY updated_at;

-- name: NonTerminalWorkflows :many
SELECT id, status, updated_at FROM workflows
WHERE workflow_is_active(status) ORDER BY updated_at;

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
-- step_applied: the event's step records an attempt of kind webhook, the
-- trace the apply path leaves (so "applied" cannot be a label alone).
SELECT i.provider_event_id, i.status, COALESCE(i.note, '')::text AS note, w.status AS workflow_status,
       EXISTS (SELECT 1 FROM step_attempts a WHERE a.step_id = i.step_id AND a.kind = 'webhook')::bool AS step_applied,
       -- an operator acted on the workflow (its evidence outlives the parking)
       EXISTS (SELECT 1 FROM admin_actions x WHERE x.workflow_id = s.workflow_id)::bool AS operator_acted
FROM webhook_inbox i
LEFT JOIN steps s ON s.id = i.step_id
LEFT JOIN workflows w ON w.id = s.workflow_id
ORDER BY i.received_at;

-- name: WorkflowsWithManyKeys :many
SELECT workflow_id, count(*)::int AS keys FROM api_idempotency_keys
WHERE workflow_id IS NOT NULL GROUP BY workflow_id HAVING count(*) > 1;

-- name: OutboxMismatches :many
-- I8: workflows whose outbox events are wrong. A terminal workflow needs
-- exactly one event, of its status's type, whose frozen payload is still its
-- view (nothing changes a workflow after it ends); any other workflow needs
-- none.
-- Each terminal workflow's view is computed once (final review M4).
WITH w AS (
  SELECT id, status, workflow_event_type(status) AS event_type,
         CASE WHEN workflow_event_type(status) IS NOT NULL
              THEN convert_to(workflow_view(id), 'UTF8') END AS view
  FROM workflows
)
SELECT w.id AS workflow_id, w.status,
       count(o.id)::int AS events,
       COALESCE(string_agg(DISTINCT o.type, ',') FILTER (WHERE o.type IS DISTINCT FROM w.event_type), '')::text AS wrong_types,
       (count(o.id) FILTER (WHERE o.payload IS DISTINCT FROM w.view))::int AS wrong_payloads
FROM w
LEFT JOIN outbox o ON o.workflow_id = w.id
GROUP BY w.id, w.status, w.event_type, w.view
HAVING (w.event_type IS NOT NULL AND count(o.id) <> 1)
    OR (w.event_type IS NULL AND count(o.id) > 0)
    OR count(o.id) FILTER (WHERE o.type IS DISTINCT FROM w.event_type) > 0
    OR count(o.id) FILTER (WHERE o.payload IS DISTINCT FROM w.view) > 0
ORDER BY w.id;

-- name: ParkedStepReasons :many
-- The DEAD steps of workflows that need attention, with why they died.
SELECT s.workflow_id, s.name, COALESCE(s.dead_reason, '')::text AS dead_reason
FROM steps s JOIN workflows w ON w.id = s.workflow_id
WHERE w.status = 'NEEDS_ATTENTION' AND s.status = 'DEAD';

-- name: OutboxEventsForShopCheck :many
-- I9: every event of the given clients' workflows, with what it announces.
SELECT o.id, o.workflow_id, o.type, o.status,
       (workflow_view(o.workflow_id)::json ->> 'status')::text AS view_status
FROM outbox o
JOIN workflows w ON w.id = o.workflow_id
WHERE w.client_id = ANY(sqlc.arg(clients)::text[])
ORDER BY o.created_at;

-- name: PendingOutboxEvents :many
-- I4: outcomes still waiting to reach their shop.
SELECT id, workflow_id, attempts FROM outbox WHERE status = 'PENDING' ORDER BY created_at;
