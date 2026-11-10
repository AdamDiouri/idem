-- +goose Up
-- Slice 2.4 (M2 design §5.1, §5.2; D13): the shop's view of a workflow and the
-- outbox of terminal events.

-- The provider's code when it refused a step for good (e.g. card_declined),
-- so a failure has a reason the shop can use. NULL for any other outcome.
ALTER TABLE steps ADD COLUMN failure_code text;

CREATE TABLE outbox (
  id               uuid        PRIMARY KEY, -- v7; the event ID shops deduplicate on
  client_id        text        NOT NULL,
  workflow_id      uuid        NOT NULL REFERENCES workflows(id),
  type             text        NOT NULL CHECK (type IN ('workflow.succeeded', 'workflow.failed', 'workflow.compensated')),
  payload          bytea       NOT NULL, -- workflow_view at the transition, frozen
  created_at       timestamptz NOT NULL DEFAULT now(),
  status           text        NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'DELIVERED', 'NO_ENDPOINT', 'DEAD')),
  attempts         int         NOT NULL DEFAULT 0,
  next_attempt_at  timestamptz NOT NULL DEFAULT now(),
  lease_owner      text,
  lease_expires_at timestamptz,
  delivered_at     timestamptz,
  last_error       text,
  -- One terminal transition, one event: a second is a bug, and fails loudly.
  UNIQUE (workflow_id, type)
);
CREATE INDEX outbox_due ON outbox (next_attempt_at) WHERE status = 'PENDING';

-- The event type a workflow status announces; NULL while it is not terminal.
CREATE FUNCTION workflow_event_type(p_status text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE p_status
    WHEN 'SUCCEEDED'   THEN 'workflow.succeeded'
    WHEN 'FAILED'      THEN 'workflow.failed'
    WHEN 'COMPENSATED' THEN 'workflow.compensated'
  END
$$;

-- What a shop may see of a workflow, as JSON text (NULL if it does not exist).
-- The outbox stores it at the terminal transition and GET /workflows/{id}
-- returns it live, so the two cannot drift. Statuses that are not an outcome
-- yet (including NEEDS_ATTENTION: parking is internal) are "processing". The
-- reason of a failed workflow is the charge's provider code when it is a
-- plain token, never raw provider text.
CREATE FUNCTION workflow_view(p_workflow_id uuid) RETURNS text
LANGUAGE sql STABLE AS $$
  SELECT json_strip_nulls(json_build_object(
    'workflow_id',  w.id,
    'status',       CASE w.status
                      WHEN 'SUCCEEDED'   THEN 'succeeded'
                      WHEN 'FAILED'      THEN 'failed'
                      WHEN 'COMPENSATED' THEN 'compensated'
                      ELSE 'processing'
                    END,
    'reason',       CASE w.status
                      WHEN 'FAILED' THEN COALESCE((
                        SELECT s.failure_code FROM steps s
                        WHERE s.workflow_id = w.id AND s.name = 'charge' AND s.status = 'FAILED_PERMANENT'
                          AND s.failure_code ~ '^[a-z][a-z0-9_]{0,63}$'
                        ORDER BY s.generation DESC LIMIT 1), 'charge_failed')
                      WHEN 'COMPENSATED' THEN 'payout_failed'
                    END,
    'amount_cents', (w.input ->> 'amount_cents')::bigint,
    'fee_cents',    (w.input ->> 'fee_cents')::bigint,
    'currency',     w.input ->> 'currency',
    'created_at',   to_char(w.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'updated_at',   to_char(w.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
  ))::text
  FROM workflows w WHERE w.id = p_workflow_id
$$;

-- Workflows that finished before the outbox existed get their event now, so
-- every terminal workflow has exactly one (I8). Their IDs are v4.
INSERT INTO outbox (id, client_id, workflow_id, type, payload)
SELECT gen_random_uuid(), client_id, id, workflow_event_type(status), convert_to(workflow_view(id), 'UTF8')
FROM workflows WHERE workflow_event_type(status) IS NOT NULL;

-- +goose Down
DROP FUNCTION workflow_view(uuid);
DROP FUNCTION workflow_event_type(text);
DROP TABLE outbox;
ALTER TABLE steps DROP COLUMN failure_code;
