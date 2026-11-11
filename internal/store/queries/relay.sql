-- name: ClaimOutboxEvent :one
-- Takes the next due outbox event under a lease (lease_epoch + 1 fences the
-- previous holder, D7). SKIP LOCKED lets several relays run.
UPDATE outbox
SET lease_owner      = sqlc.arg(owner)::text,
    lease_epoch      = lease_epoch + 1,
    lease_expires_at = now() + (sqlc.arg(lease_ms)::bigint * interval '1 millisecond'),
    attempts         = attempts + 1
WHERE id = (
  SELECT id FROM outbox
  WHERE status = 'PENDING' AND next_attempt_at <= now()
    AND (lease_expires_at IS NULL OR lease_expires_at < now())
  ORDER BY next_attempt_at
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING id, client_id, workflow_id, type, payload, attempts, lease_epoch;

-- name: FinishOutboxEvent :execrows
-- Fenced compare-and-swap: only the current lease holder records the result.
UPDATE outbox
SET status           = sqlc.arg(status)::text,
    last_error       = sqlc.narg(last_error)::text,
    lease_owner      = NULL,
    lease_expires_at = NULL,
    delivered_at     = CASE WHEN sqlc.arg(status)::text = 'DELIVERED' THEN now() ELSE delivered_at END,
    next_attempt_at  = CASE WHEN sqlc.arg(status)::text = 'PENDING'
                            THEN now() + (sqlc.arg(retry_ms)::bigint * interval '1 millisecond')
                            ELSE next_attempt_at END
WHERE id = sqlc.arg(id)::uuid AND lease_epoch = sqlc.arg(lease_epoch)::bigint AND status = 'PENDING';
