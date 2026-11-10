-- name: InsertInboxEvent :execrows
-- Stores a verified provider event once; a duplicate inserts nothing (F15).
INSERT INTO webhook_inbox (provider_event_id, type, payload)
VALUES (sqlc.arg(provider_event_id), sqlc.arg(type), sqlc.arg(payload))
ON CONFLICT (provider_event_id) DO NOTHING;

-- name: ClaimInboxEvent :one
-- Takes the next due inbox event under a lease (lease_epoch + 1 fences the
-- previous holder). SKIP LOCKED lets several workers process the inbox.
UPDATE webhook_inbox
SET lease_owner      = sqlc.arg(owner)::text,
    lease_epoch      = lease_epoch + 1,
    lease_expires_at = now() + (sqlc.arg(lease_ms)::bigint * interval '1 millisecond'),
    attempts         = attempts + 1
WHERE provider_event_id = (
  SELECT provider_event_id FROM webhook_inbox
  WHERE status = 'PENDING' AND next_attempt_at <= now()
    AND (lease_expires_at IS NULL OR lease_expires_at < now())
  ORDER BY next_attempt_at
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING provider_event_id, type, payload, lease_epoch, attempts,
          (EXTRACT(EPOCH FROM (now() - received_at)) * 1000)::bigint AS age_ms;

-- name: StepForEvent :one
-- The step an event is about: by the provider's reference, else by our
-- idempotency key (the webhook may beat our own commit of the reference).
SELECT s.id, s.workflow_id, s.name, s.status, s.dead_reason, s.idempotency_key, s.provider_ref,
       s.refund_charge_ref, s.lease_epoch, s.attempt_count, s.poll_count, s.max_attempts,
       w.status AS workflow_status, w.input,
       (NOT EXISTS (SELECT 1 FROM steps o WHERE o.workflow_id = s.workflow_id AND o.created_at > s.created_at))::bool AS newest,
       step_send_doubt(s.id)::bool AS send_doubt
FROM steps s JOIN workflows w ON w.id = s.workflow_id
WHERE s.provider_ref = sqlc.arg(payout_id)::text OR s.idempotency_key = sqlc.arg(idempotency_key)::text
ORDER BY (s.provider_ref IS NOT DISTINCT FROM sqlc.arg(payout_id)::text) DESC, s.generation DESC
LIMIT 1;

-- name: LockStepForEvent :one
-- The same view, locked with its workflow, for the apply transaction.
SELECT s.id, s.workflow_id, s.name, s.status, s.dead_reason, s.idempotency_key, s.provider_ref,
       s.refund_charge_ref, s.lease_epoch, s.attempt_count, s.poll_count, s.max_attempts,
       w.status AS workflow_status, w.input,
       (NOT EXISTS (SELECT 1 FROM steps o WHERE o.workflow_id = s.workflow_id AND o.created_at > s.created_at))::bool AS newest,
       step_send_doubt(s.id)::bool AS send_doubt
FROM steps s JOIN workflows w ON w.id = s.workflow_id
WHERE s.id = sqlc.arg(id)
FOR UPDATE OF s, w;

-- name: ApplyCallbackToStep :execrows
-- Applies a provider callback to a waiting step: compare-and-swap on
-- AWAITING_CALLBACK, a new owner generation (lease_epoch + 1). poll_count is
-- the counter for non-send attempt numbers (polls and webhooks).
UPDATE steps
SET status            = sqlc.arg(status)::text,
    provider_ref      = COALESCE(sqlc.narg(provider_ref)::text, provider_ref),
    dead_reason       = sqlc.narg(dead_reason)::text,
    last_error        = sqlc.narg(last_error)::text,
    lease_epoch       = lease_epoch + 1,
    poll_count        = poll_count + 1,
    callback_deadline = NULL
WHERE id = sqlc.arg(id) AND status = 'AWAITING_CALLBACK';

-- name: InsertWebhookAttempt :exec
INSERT INTO step_attempts (step_id, attempt_no, lease_epoch, worker_id, kind, outcome, error, finished_at)
VALUES (sqlc.arg(step_id), sqlc.arg(attempt_no), sqlc.arg(lease_epoch), sqlc.arg(worker_id), 'webhook',
        sqlc.arg(outcome), sqlc.narg(error), now());

-- name: FinishInboxEvent :execrows
-- Fenced by the inbox lease: a processor that lost it writes nothing.
UPDATE webhook_inbox
SET status = sqlc.arg(status)::text, note = sqlc.narg(note)::text, step_id = sqlc.narg(step_id),
    processed_at = now(), last_error = NULL, lease_owner = NULL, lease_expires_at = NULL
WHERE provider_event_id = sqlc.arg(provider_event_id) AND lease_epoch = sqlc.arg(lease_epoch);

-- name: DeferInboxEvent :execrows
UPDATE webhook_inbox
SET next_attempt_at = now() + (sqlc.arg(delay_ms)::bigint * interval '1 millisecond'),
    last_error = sqlc.narg(last_error)::text, lease_owner = NULL, lease_expires_at = NULL
WHERE provider_event_id = sqlc.arg(provider_event_id) AND lease_epoch = sqlc.arg(lease_epoch);

-- name: PruneInbox :execrows
-- Retention: forget settled news. Evidence and conflicts (open or resolved)
-- stay for operators and audits; pending events are never pruned. Bounded
-- per call so a large backlog is pruned over several sweeps.
DELETE FROM webhook_inbox
WHERE provider_event_id IN (
  SELECT provider_event_id FROM webhook_inbox
  WHERE status IN ('PROCESSED', 'IGNORED')
    AND processed_at < now() - (sqlc.arg(retention_ms)::bigint * interval '1 millisecond')
  LIMIT 1000
);
