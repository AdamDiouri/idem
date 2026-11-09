-- name: InsertIdempotencyKey :execrows
-- Reserves (client_id, key). If another transaction holds the same key
-- uncommitted, this waits on the unique index until it commits or aborts.
INSERT INTO api_idempotency_keys (client_id, key, request_fingerprint)
VALUES (@client_id, @key, @request_fingerprint)
ON CONFLICT (client_id, key) DO NOTHING;

-- name: GetIdempotencyKey :one
SELECT request_fingerprint, response_body
FROM api_idempotency_keys
WHERE client_id = @client_id AND key = @key;

-- name: InsertWorkflow :one
INSERT INTO workflows (id, client_id, type, status, input)
VALUES (@id, @client_id, @type, @status, @input)
RETURNING created_at;

-- name: InsertStep :exec
INSERT INTO steps (id, workflow_id, name, generation, status, idempotency_key, max_attempts, refund_charge_ref)
VALUES (sqlc.arg(id), sqlc.arg(workflow_id), sqlc.arg(name), sqlc.arg(generation), sqlc.arg(status),
        sqlc.arg(idempotency_key), sqlc.arg(max_attempts), sqlc.narg(refund_charge_ref));

-- name: SetIdempotencyKeyResponse :exec
UPDATE api_idempotency_keys
SET workflow_id = @workflow_id, response_body = @response_body
WHERE client_id = @client_id AND key = @key;
