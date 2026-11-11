-- name: ListParkedSteps :many
-- Each parked workflow with the step that parked it (its most recent step).
-- w.version changes whenever the workflow leaves NEEDS_ATTENTION and comes back,
-- so it identifies what the operator saw.
SELECT DISTINCT ON (w.id)
  w.id AS workflow_id, w.version, s.id AS step_id, s.name, s.status, s.dead_reason, s.last_error, s.attempt_count, s.poll_count, w.input,
  (EXTRACT(EPOCH FROM (now() - w.updated_at)) * 1000)::bigint AS parked_ms,
  -- The provider's latest news about this step (D18), if any.
  COALESCE((SELECT i.note FROM webhook_inbox i WHERE i.step_id = s.id AND i.status = 'EVIDENCE'
            ORDER BY i.processed_at DESC LIMIT 1), '')::text AS evidence
FROM workflows w
JOIN steps s ON s.workflow_id = w.id AND s.status IN ('DEAD', 'FAILED_PERMANENT')
WHERE w.status = 'NEEDS_ATTENTION'
ORDER BY w.id, s.created_at DESC;

-- name: LockParkedStep :one
-- Locks a step only if it is the parked step of a NEEDS_ATTENTION workflow:
-- dead or refused, and the workflow's most recent step. Anything else (a step a
-- worker may run, a superseded step) yields no row.
SELECT s.id, s.workflow_id, s.name, s.generation, s.status, s.dead_reason, s.idempotency_key,
       s.refund_charge_ref, s.provider_ref, w.input, w.version AS workflow_version
FROM steps s
JOIN workflows w ON w.id = s.workflow_id
WHERE s.id = sqlc.arg(id)
  AND w.status = 'NEEDS_ATTENTION'
  AND s.status IN ('DEAD', 'FAILED_PERMANENT')
  AND NOT EXISTS (SELECT 1 FROM steps o WHERE o.workflow_id = s.workflow_id AND o.created_at > s.created_at)
FOR UPDATE OF s, w;

-- name: ParkedStepOfWorkflow :one
SELECT s.id FROM steps s
JOIN workflows w ON w.id = s.workflow_id
WHERE s.workflow_id = sqlc.arg(workflow_id)
  AND w.status = 'NEEDS_ATTENTION'
  AND s.status IN ('DEAD', 'FAILED_PERMANENT')
  AND NOT EXISTS (SELECT 1 FROM steps o WHERE o.workflow_id = s.workflow_id AND o.created_at > s.created_at);

-- name: ResetStepForRetry :execrows
-- Hands the step back to workers: a new owner generation (lease_epoch + 1) and
-- a fresh attempt budget. attempt_count is kept: attempt numbers are unique
-- per step (step_attempts primary key), so resetting it would block the claim.
UPDATE steps
SET status = sqlc.arg(status)::text, max_attempts = attempt_count + sqlc.arg(extra_attempts)::int,
    dead_reason = NULL, last_error = NULL, hook_failures = 0,
    run_after = now(), lease_epoch = lease_epoch + 1, lease_owner = NULL, lease_expires_at = NULL
WHERE id = sqlc.arg(id) AND status IN ('DEAD', 'FAILED_PERMANENT');

-- name: ConfirmStepSucceeded :execrows
UPDATE steps
SET status = 'SUCCEEDED', provider_ref = sqlc.arg(provider_ref)::text, dead_reason = NULL,
    lease_epoch = lease_epoch + 1, lease_owner = NULL, lease_expires_at = NULL
WHERE id = sqlc.arg(id) AND status IN ('DEAD', 'FAILED_PERMANENT');

-- name: NextStepGeneration :one
SELECT (COALESCE(max(generation), -1) + 1)::int AS generation
FROM steps WHERE workflow_id = sqlc.arg(workflow_id) AND name = sqlc.arg(name);

-- name: InsertAdminAction :exec
INSERT INTO admin_actions (id, actor, action, workflow_id, step_id, reason, details)
VALUES (sqlc.arg(id), sqlc.arg(actor), sqlc.arg(action), sqlc.arg(workflow_id), sqlc.narg(step_id),
        sqlc.arg(reason), sqlc.arg(details));

-- name: StepHadInDoubtAttempt :one
-- Whether any SEND attempt of the step may have moved money: the one shared
-- definition (migration 00012). Polls send nothing, so they never count (D17).
SELECT step_send_doubt(sqlc.arg(step_id))::bool AS had;

-- name: ProviderRefInUse :one
-- Whether another step already records this provider reference. The step's own
-- reference (e.g. the payout ID it was waiting on) is not a reuse.
SELECT EXISTS (
  SELECT 1 FROM steps WHERE provider_ref = sqlc.arg(provider_ref) AND id <> sqlc.arg(step_id)
)::bool AS used;

-- name: ListConflicts :many
-- Provider news that truly contradicts idem (D16): money may have moved both ways.
SELECT i.provider_event_id, i.type, COALESCE(i.note, '')::text AS note, i.received_at, s.id AS step_id, s.workflow_id
FROM webhook_inbox i JOIN steps s ON s.id = i.step_id
WHERE i.status = 'CONFLICT'
ORDER BY i.received_at;

-- name: ResolveConflict :one
-- A human resolved a conflict outside idem (e.g. clawed money back).
UPDATE webhook_inbox i
SET status = 'CONFLICT_RESOLVED'
FROM steps s
WHERE i.provider_event_id = sqlc.arg(provider_event_id) AND i.status = 'CONFLICT' AND s.id = i.step_id
RETURNING s.workflow_id, s.id AS step_id, COALESCE(i.note, '')::text AS note;

-- name: ListOutboxEvents :many
-- Events that have not reached their shop (slice 2.5), oldest first; with
-- include_delivered, every event (to resend one a shop lost).
SELECT id, workflow_id, client_id, type, status, attempts, lease_epoch, COALESCE(last_error, '')::text AS last_error, created_at
FROM outbox
WHERE (sqlc.arg(include_delivered)::bool OR status <> 'DELIVERED') AND (NOT sqlc.arg(only_dead)::bool OR status = 'DEAD')
ORDER BY created_at, id;

-- name: LockOutboxEvent :one
SELECT id, workflow_id, status, attempts, lease_epoch FROM outbox WHERE id = sqlc.arg(id)::uuid FOR UPDATE;

-- name: LockClientOutboxEvents :many
-- Every event of one client in one status, locked, for a bulk redelivery.
SELECT id, workflow_id, status, attempts, lease_epoch FROM outbox
WHERE client_id = sqlc.arg(client_id)::text AND status = sqlc.arg(status)::text
ORDER BY created_at
FOR UPDATE;

-- name: RequeueOutboxEvent :exec
-- An operator sends the event again, with a fresh budget; the epoch bump
-- fences any relay still holding an old lease on it (D7).
UPDATE outbox
SET status = 'PENDING', attempts = 0, next_attempt_at = now(), last_error = NULL,
    lease_owner = NULL, lease_expires_at = NULL, lease_epoch = lease_epoch + 1
WHERE id = sqlc.arg(id)::uuid;
