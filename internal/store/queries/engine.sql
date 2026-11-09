-- name: ClaimStep :one
-- Claims the next runnable step (design section 9.1). SKIP LOCKED lets many workers
-- claim concurrently without blocking or double-claiming; lease_epoch + 1 is
-- the fencing token of the new owner. All times come from the database clock.
WITH next AS (
  SELECT id FROM steps
  WHERE status IN ('PENDING', 'IN_DOUBT')
    AND run_after <= now()
    AND name = ANY(sqlc.arg(names)::text[])
  ORDER BY run_after
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE steps s
SET status             = 'RUNNING',
    prior_status       = s.status,
    lease_owner        = sqlc.arg(worker_id)::text,
    lease_epoch        = s.lease_epoch + 1,
    lease_expires_at   = now() + (sqlc.arg(lease_ms)::bigint * interval '1 millisecond'),
    attempt_count      = s.attempt_count + 1,
    first_attempted_at = COALESCE(s.first_attempted_at, now())
FROM next, workflows w
WHERE s.id = next.id AND w.id = s.workflow_id
RETURNING s.id, s.workflow_id, s.name, s.idempotency_key, s.lease_epoch, s.attempt_count, s.prior_status, w.input;

-- name: InsertStepAttempt :exec
INSERT INTO step_attempts (step_id, attempt_no, lease_epoch, worker_id)
VALUES (sqlc.arg(step_id), sqlc.arg(attempt_no), sqlc.arg(lease_epoch), sqlc.arg(worker_id));

-- name: MarkWorkflowRunning :exec
UPDATE workflows
SET status = 'RUNNING', version = version + 1, updated_at = now()
WHERE id = sqlc.arg(id) AND status = 'PENDING';

-- name: CompleteStep :execrows
-- Fenced compare-and-swap: only the current lease holder can complete.
UPDATE steps
SET status           = sqlc.arg(status)::text,
    lease_owner      = NULL,
    lease_expires_at = NULL,
    provider_ref     = COALESCE(sqlc.narg(provider_ref)::text, provider_ref),
    last_error       = sqlc.narg(last_error)::text,
    run_after        = CASE WHEN sqlc.arg(status)::text = 'IN_DOUBT'
                            THEN now() + (sqlc.arg(retry_ms)::bigint * interval '1 millisecond')
                            ELSE run_after END
WHERE id = sqlc.arg(id) AND lease_epoch = sqlc.arg(lease_epoch) AND status = 'RUNNING';

-- name: FinishStepAttempt :exec
UPDATE step_attempts
SET outcome     = sqlc.arg(outcome)::text,
    http_status = sqlc.narg(http_status)::int,
    error       = sqlc.narg(error)::text,
    finished_at = now()
WHERE step_id = sqlc.arg(step_id) AND attempt_no = sqlc.arg(attempt_no) AND outcome IS NULL;

-- name: ReclaimExpiredLeases :one
-- A worker that holds an expired lease may have crashed or be paused. We cannot
-- know whether it called the provider, so the step becomes IN_DOUBT. Bumping
-- lease_epoch fences the old holder. Concurrent sweepers serialise on the row
-- lock and the loser no longer matches status = 'RUNNING'.
WITH reclaimed AS (
  UPDATE steps
  SET status           = 'IN_DOUBT',
      lease_epoch      = lease_epoch + 1,
      lease_owner      = NULL,
      lease_expires_at = NULL,
      run_after        = now()
  WHERE status = 'RUNNING' AND lease_expires_at < now()
  RETURNING id, attempt_count
), abandoned AS (
  UPDATE step_attempts a
  SET outcome = 'ABANDONED', finished_at = now()
  FROM reclaimed r
  WHERE a.step_id = r.id AND a.attempt_no = r.attempt_count AND a.outcome IS NULL
  RETURNING a.step_id
)
SELECT count(*)::bigint AS reclaimed FROM reclaimed;
