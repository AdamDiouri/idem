-- name: InsertReconRun :exec
INSERT INTO recon_runs (id, window_from, window_to) VALUES (sqlc.arg(id), sqlc.arg(window_from), sqlc.arg(window_to));

-- name: FinishReconRun :exec
UPDATE recon_runs
SET status = sqlc.arg(status)::text, finished_at = now(), records = sqlc.arg(records)::int,
    workflows = sqlc.arg(workflows)::int, in_flight = sqlc.arg(in_flight)::int,
    discrepancies = sqlc.arg(discrepancies)::int, error = sqlc.narg(error)::text
WHERE id = sqlc.arg(id) AND status = 'RUNNING';

-- name: WorkflowsPostedBetween :many
-- Workflows with a ledger posting in the window (one side of the anchor set).
SELECT DISTINCT workflow_id FROM ledger_transactions
WHERE created_at >= sqlc.arg(window_from) AND created_at < sqlc.arg(window_to);

-- name: ReconWorkflowStatuses :many
SELECT id, status FROM workflows WHERE id = ANY(sqlc.arg(ids)::uuid[]);

-- name: ReconSteps :many
-- Every step of the workflows, every generation, with the provider object it
-- learned (a movement is matched to its step through provider_ref).
SELECT workflow_id, id, name, status, COALESCE(provider_ref, '')::text AS provider_ref
FROM steps WHERE workflow_id = ANY(sqlc.arg(ids)::uuid[])
ORDER BY workflow_id, name, generation, created_at;

-- name: ReconPostings :many
-- Each posting's provider_receivable entry: charge +amount, payout −(amount −
-- fee), refund −amount, signed exactly like the provider's report.
SELECT t.workflow_id, t.kind, e.amount_cents, t.step_id, t.created_at, e.currency
FROM ledger_transactions t
JOIN ledger_entries e ON e.transaction_id = t.id AND e.account = 'provider_receivable'
WHERE t.workflow_id = ANY(sqlc.arg(ids)::uuid[])
ORDER BY t.created_at;

-- name: InsertReconDiscrepancy :exec
INSERT INTO recon_discrepancies (id, run_id, workflow_id, kind, effect_type, provider_refs, provider_amount, ledger_amount, parked, detail,
                                 item_key, item_at)
VALUES (sqlc.arg(id), sqlc.arg(run_id), sqlc.narg(workflow_id), sqlc.arg(kind), sqlc.arg(effect_type), sqlc.arg(provider_refs)::text[],
        sqlc.narg(provider_amount), sqlc.narg(ledger_amount), sqlc.arg(parked), sqlc.arg(detail), sqlc.arg(item_key), sqlc.arg(item_at));
