-- +goose Up
-- I6 in both directions (slice 2.3b): a key maps to one workflow (the primary
-- key) and a workflow to one key. Without this, a replay bug that answered key
-- B with workflow A would store it, and payment B would be silently lost.
-- On a database that already breaks this, the migration fails; it never drops
-- a row.
ALTER TABLE api_idempotency_keys ADD CONSTRAINT api_idempotency_keys_workflow_unique UNIQUE (workflow_id);

-- +goose Down
ALTER TABLE api_idempotency_keys DROP CONSTRAINT api_idempotency_keys_workflow_unique;
