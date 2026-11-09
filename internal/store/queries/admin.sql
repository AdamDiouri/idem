-- name: ListParkedSteps :many
-- Each parked workflow with the step that parked it (its most recent step).
SELECT DISTINCT ON (w.id)
  w.id AS workflow_id, s.id AS step_id, s.name, s.status, s.dead_reason, s.last_error, s.attempt_count, w.input,
  (EXTRACT(EPOCH FROM (now() - w.updated_at)) * 1000)::bigint AS parked_ms
FROM workflows w
JOIN steps s ON s.workflow_id = w.id AND s.status IN ('DEAD', 'FAILED_PERMANENT')
WHERE w.status = 'NEEDS_ATTENTION'
ORDER BY w.id, s.created_at DESC;

-- name: LockParkedStep :one
-- Locks a step only if it is the parked step of a NEEDS_ATTENTION workflow:
-- dead or refused, and the workflow's most recent step. Anything else (a step a
-- worker may run, a superseded step) yields no row.
SELECT s.id, s.workflow_id, s.name, s.generation, s.status, s.dead_reason, s.idempotency_key,
       s.refund_charge_ref, w.input
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
    dead_reason = NULL, last_error = NULL,
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
-- Whether any attempt of the step may have moved money (ambiguous or abandoned).
SELECT EXISTS (
  SELECT 1 FROM step_attempts WHERE step_id = sqlc.arg(step_id) AND outcome IN ('IN_DOUBT', 'ABANDONED')
)::bool AS had;

-- name: ProviderRefInUse :one
SELECT EXISTS (SELECT 1 FROM steps WHERE provider_ref = sqlc.arg(provider_ref))::bool AS used;
