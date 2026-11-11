-- name: ClaimStep :one
-- Claims the next runnable step (design section 9.1). SKIP LOCKED lets many workers
-- claim concurrently without blocking or double-claiming; lease_epoch + 1 is
-- the fencing token of the new owner. All times come from the database clock.
-- Only steps of active workflows are claimable: a parked (NEEDS_ATTENTION) or
-- finished workflow must not move money it could no longer record.
-- An AWAITING_CALLBACK step past its deadline is claimed for a poll: poll_count
-- grows instead of attempt_count (the send budget). send_doubt reports whether
-- any earlier SEND attempt may have moved money (the pivot rule reads it);
-- DEAD counts too, because an exhausted ambiguous attempt is recorded as DEAD.
WITH next AS (
  SELECT st.id FROM steps st
  JOIN workflows wf ON wf.id = st.workflow_id
  WHERE st.status IN ('PENDING', 'IN_DOUBT', 'AWAITING_CALLBACK')
    AND st.run_after <= now()
    AND st.name = ANY(sqlc.arg(names)::text[])
    AND workflow_is_active(wf.status)
  ORDER BY st.run_after
  LIMIT 1
  FOR UPDATE OF st SKIP LOCKED
)
UPDATE steps s
SET status             = 'RUNNING',
    prior_status       = s.status,
    lease_owner        = sqlc.arg(worker_id)::text,
    lease_epoch        = s.lease_epoch + 1,
    lease_expires_at   = now() + (sqlc.arg(lease_ms)::bigint * interval '1 millisecond'),
    attempt_count      = s.attempt_count + CASE WHEN s.status = 'AWAITING_CALLBACK' THEN 0 ELSE 1 END,
    poll_count         = s.poll_count + CASE WHEN s.status = 'AWAITING_CALLBACK' THEN 1 ELSE 0 END,
    first_attempted_at = COALESCE(s.first_attempted_at, now())
FROM next, workflows w
WHERE s.id = next.id AND w.id = s.workflow_id
RETURNING s.id, s.workflow_id, s.name, s.idempotency_key, s.lease_epoch, s.attempt_count, s.poll_count, s.prior_status,
          s.max_attempts, s.refund_charge_ref, s.provider_ref,
          (EXTRACT(EPOCH FROM (now() - s.first_attempted_at)) * 1000)::bigint AS key_age_ms,
          w.input,
          step_send_doubt(s.id)::bool AS send_doubt,
          -- Polls since the latest send, this one included: the poll budget is
          -- per acceptance, so an operator's retry (a re-send) gets a fresh one.
          ((SELECT count(*) FROM step_attempts a
            WHERE a.step_id = s.id AND a.kind = 'poll'
              AND a.attempt_no > COALESCE((SELECT max(b.attempt_no) FROM step_attempts b
                                           WHERE b.step_id = s.id AND b.kind = 'send'), 0))
           + CASE WHEN s.prior_status = 'AWAITING_CALLBACK' THEN 1 ELSE 0 END)::int AS polls_since_send,
          -- How long since the provider accepted the request (the latest send
          -- attempt ended): the poll budget is mainly a total wait.
          COALESCE((EXTRACT(EPOCH FROM (now() - (SELECT max(a.finished_at) FROM step_attempts a
                                                  WHERE a.step_id = s.id AND a.kind = 'send'))) * 1000)::bigint, 0)::bigint AS waited_ms;

-- name: InsertStepAttempt :exec
INSERT INTO step_attempts (step_id, attempt_no, lease_epoch, worker_id, kind)
VALUES (sqlc.arg(step_id), sqlc.arg(attempt_no), sqlc.arg(lease_epoch), sqlc.arg(worker_id), sqlc.arg(kind));

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
    dead_reason      = sqlc.narg(dead_reason)::text,
    failure_code     = sqlc.narg(failure_code)::text,
    run_after        = CASE WHEN sqlc.arg(status)::text IN ('IN_DOUBT', 'PENDING', 'AWAITING_CALLBACK')
                            THEN now() + (sqlc.arg(retry_ms)::bigint * interval '1 millisecond')
                            ELSE run_after END,
    -- A waiting step is polled at its callback deadline (run_after, same instant).
    callback_deadline = CASE WHEN sqlc.arg(status)::text = 'AWAITING_CALLBACK'
                             THEN now() + (sqlc.arg(retry_ms)::bigint * interval '1 millisecond')
                             ELSE NULL END
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
-- Exception: a step claimed for a poll (prior_status AWAITING_CALLBACK) sent
-- nothing, so it goes back to waiting, due now (design D17).
WITH reclaimed AS (
  UPDATE steps
  SET status            = CASE WHEN prior_status = 'AWAITING_CALLBACK' THEN 'AWAITING_CALLBACK' ELSE 'IN_DOUBT' END,
      callback_deadline = CASE WHEN prior_status = 'AWAITING_CALLBACK' THEN now() ELSE NULL END,
      lease_epoch       = lease_epoch + 1,
      lease_owner       = NULL,
      lease_expires_at  = NULL,
      run_after         = now()
  WHERE status = 'RUNNING' AND lease_expires_at < now()
  RETURNING id, attempt_count + poll_count AS attempt_no
), abandoned AS (
  UPDATE step_attempts a
  SET outcome = 'ABANDONED', finished_at = now()
  FROM reclaimed r
  WHERE a.step_id = r.id AND a.attempt_no = r.attempt_no AND a.outcome IS NULL
  RETURNING a.step_id
)
SELECT count(*)::bigint AS reclaimed FROM reclaimed;

-- name: BumpWorkflowVersion :exec
UPDATE workflows SET version = version + 1, updated_at = now() WHERE id = sqlc.arg(id);

-- name: TransitionWorkflow :execrows
-- Compare-and-swap: only moves the workflow if it is in one of the expected states.
UPDATE workflows
SET status = sqlc.arg(to_status)::text, version = version + 1, updated_at = now()
WHERE id = sqlc.arg(id) AND status = ANY(sqlc.arg(from_statuses)::text[]);

-- name: ChargeProviderRef :one
-- The latest successful charge (admin new-generation can create several charge steps).
SELECT provider_ref::text FROM steps
WHERE workflow_id = sqlc.arg(workflow_id) AND name = 'charge' AND status = 'SUCCEEDED' AND provider_ref IS NOT NULL
ORDER BY generation DESC
LIMIT 1;

-- name: FindStuckWorkflows :many
-- Unsettled workflows without progress (a status change or a FINISHED step
-- attempt: a claim alone, or an attempt abandoned by a crash, is not; a step
-- waiting for a callback counts as progress until its poll is due) for
-- older_than_ms, longest idle first. Same definition as UnsettledIdleMs.
SELECT id, status, idle_ms FROM (
  SELECT w.id, w.status,
         (EXTRACT(EPOCH FROM (now() - GREATEST(w.updated_at, a.last_attempt, c.waiting_until))) * 1000)::bigint AS idle_ms
  FROM workflows w
  LEFT JOIN LATERAL (
    SELECT max(sa.finished_at) AS last_attempt
    FROM steps s JOIN step_attempts sa ON sa.step_id = s.id
    WHERE s.workflow_id = w.id AND sa.outcome IS DISTINCT FROM 'ABANDONED'
  ) a ON true
  LEFT JOIN LATERAL (
    -- A step waiting for its callback is healthy until its poll is due.
    SELECT max(LEAST(ws.callback_deadline, now())) AS waiting_until
    FROM steps ws WHERE ws.workflow_id = w.id AND ws.status = 'AWAITING_CALLBACK'
  ) c ON true
  WHERE workflow_is_active(w.status)
) idle
WHERE idle_ms > sqlc.arg(older_than_ms)::bigint
ORDER BY idle_ms DESC;

-- name: WorkflowStatusCounts :many
SELECT status, count(*)::int AS n FROM workflows GROUP BY status;

-- name: StepStatusCounts :many
SELECT name, status, count(*)::int AS n FROM steps GROUP BY name, status;

-- name: UnsettledIdleMs :one
-- The longest any unsettled workflow has gone without progress: a status
-- change or a finished step attempt (0 if none); a step waiting for its
-- callback is healthy until its poll is due, however long that is. Claims alone and attempts
-- abandoned by a crash don't count, or a crash loop would look busy forever. Age since creation would page as soon
-- as an operator retried an old parked workflow.
SELECT COALESCE(max(EXTRACT(EPOCH FROM (now() - GREATEST(w.updated_at, a.last_attempt, c.waiting_until))) * 1000), 0)::bigint AS idle_ms
FROM workflows w
LEFT JOIN LATERAL (
  SELECT max(sa.finished_at) AS last_attempt
  FROM steps s JOIN step_attempts sa ON sa.step_id = s.id
  WHERE s.workflow_id = w.id AND sa.outcome IS DISTINCT FROM 'ABANDONED'
) a ON true
LEFT JOIN LATERAL (
  -- A step waiting for its callback is healthy until its poll is due.
  SELECT max(LEAST(ws.callback_deadline, now())) AS waiting_until
  FROM steps ws WHERE ws.workflow_id = w.id AND ws.status = 'AWAITING_CALLBACK'
) c ON true
WHERE workflow_is_active(w.status);

-- name: StepsOverAttemptLimit :one
-- Unfinished steps claimed more than once past their limit. (A crash or lost
-- lease on the final attempt legitimately leaves attempt_count = max + 1.)
SELECT count(*)::int AS n FROM steps
WHERE status IN ('PENDING', 'IN_DOUBT', 'RUNNING') AND attempt_count > max_attempts + 1;

-- name: InboxStats :one
-- For the sweeper's gauges: the inbox backlog, open conflicts, and waiting
-- steps whose poll is more than 5 minutes overdue (polls are not keeping up).
SELECT
  (SELECT count(*) FROM webhook_inbox WHERE status = 'PENDING')::int AS pending,
  COALESCE((SELECT EXTRACT(EPOCH FROM (now() - min(received_at))) * 1000 FROM webhook_inbox WHERE status = 'PENDING'), 0)::bigint AS oldest_pending_ms,
  (SELECT count(*) FROM webhook_inbox WHERE status = 'CONFLICT')::int AS conflicts_open,
  (SELECT count(*) FROM steps WHERE status = 'AWAITING_CALLBACK' AND callback_deadline < now() - interval '5 minutes')::int AS callbacks_overdue;

-- name: OutboxStats :one
-- For the sweeper's gauges: outcome events not delivered yet.
SELECT
  (SELECT count(*) FROM outbox WHERE status = 'PENDING')::int AS pending,
  COALESCE((SELECT EXTRACT(EPOCH FROM (now() - min(created_at))) * 1000 FROM outbox WHERE status = 'PENDING'), 0)::bigint AS oldest_pending_ms,
  (SELECT count(*) FROM outbox WHERE status = 'DEAD')::int AS dead;

-- name: IncrementHookFailures :one
-- Fenced like CompleteStep: only the current lease holder counts a failure.
UPDATE steps SET hook_failures = hook_failures + 1
WHERE id = sqlc.arg(id) AND lease_epoch = sqlc.arg(lease_epoch) AND status = 'RUNNING'
RETURNING hook_failures;
