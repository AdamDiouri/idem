-- +goose Up
-- Workflows that finished before the outbox existed get their event, so every
-- terminal workflow has exactly one (I8). Their IDs are v4. A migration of its
-- own (each runs in its own transaction): 00014's locks on steps and workflows
-- are released by now, so on a large table the services keep running while
-- this reads workflows and writes outbox rows.
INSERT INTO outbox (id, client_id, workflow_id, type, payload)
SELECT gen_random_uuid(), w.client_id, w.id, workflow_event_type(w.status), convert_to(workflow_view(w.id), 'UTF8')
FROM workflows w
WHERE workflow_event_type(w.status) IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.workflow_id = w.id);

-- +goose Down
-- Backfilled rows are indistinguishable from others once delivered; 00014's
-- Down drops the table.
SELECT 1;
