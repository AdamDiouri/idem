-- name: InsertInboxEvent :execrows
-- Stores a verified provider event once; a duplicate inserts nothing (F15).
INSERT INTO webhook_inbox (provider_event_id, type, payload)
VALUES (sqlc.arg(provider_event_id), sqlc.arg(type), sqlc.arg(payload))
ON CONFLICT (provider_event_id) DO NOTHING;
