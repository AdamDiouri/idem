-- name: InsertOutboxEvent :execrows
-- The event of a workflow's terminal transition (D13), written in the same
-- transaction by transitionWorkflow. The payload is the shop's view, frozen.
INSERT INTO outbox (id, client_id, workflow_id, type, payload)
SELECT sqlc.arg(id)::uuid, w.client_id, w.id, workflow_event_type(w.status), convert_to(workflow_view(w.id), 'UTF8')
FROM workflows w
WHERE w.id = sqlc.arg(workflow_id)::uuid AND workflow_event_type(w.status) IS NOT NULL;

-- name: OutboxEventsForWorkflow :many
SELECT id, type, payload, status FROM outbox WHERE workflow_id = sqlc.arg(workflow_id)::uuid ORDER BY created_at;

-- name: WorkflowViewForClient :one
-- GET /workflows/{id}: the shop's view, only for the workflow's own client.
SELECT workflow_view(id)::text AS view FROM workflows
WHERE id = sqlc.arg(id)::uuid AND client_id = sqlc.arg(client_id)::text;
