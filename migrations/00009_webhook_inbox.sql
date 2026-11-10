-- +goose Up
-- Provider webhooks (M2 slice 2.2, design §4.3): every verified event is stored
-- here, byte-exact, BEFORE the API acknowledges it. Workers then apply it under
-- a lease (lease_epoch fences a processor that lost its lease). status:
--   PENDING           not applied yet (next_attempt_at says when to try)
--   PROCESSED         applied, or a duplicate / agrees with what idem recorded
--   IGNORED           stale (re-checked: idem is right), orphan or malformed
--   EVIDENCE          news about a parked step, shown to the operator (D18)
--   CONFLICT          the provider truly disagrees with idem; pages
--   CONFLICT_RESOLVED a human resolved the conflict (audited)
CREATE TABLE webhook_inbox (
  provider_event_id text        PRIMARY KEY,
  type              text        NOT NULL,
  payload           bytea       NOT NULL,
  received_at       timestamptz NOT NULL DEFAULT now(),
  status            text        NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'PROCESSED', 'IGNORED', 'EVIDENCE', 'CONFLICT', 'CONFLICT_RESOLVED')),
  attempts          int         NOT NULL DEFAULT 0,
  next_attempt_at   timestamptz NOT NULL DEFAULT now(),
  lease_owner       text,
  lease_epoch       bigint      NOT NULL DEFAULT 0,
  lease_expires_at  timestamptz,
  processed_at      timestamptz,
  step_id           uuid        REFERENCES steps(id),
  note              text,
  last_error        text
);
CREATE INDEX webhook_inbox_due ON webhook_inbox (next_attempt_at) WHERE status = 'PENDING';

-- A webhook applied to a step is recorded as one of its attempts.
ALTER TABLE step_attempts DROP CONSTRAINT step_attempts_kind_check;
ALTER TABLE step_attempts ADD CONSTRAINT step_attempts_kind_check CHECK (kind IN ('send', 'poll', 'webhook'));

-- Resolving a conflict is an audited admin action.
ALTER TABLE admin_actions DROP CONSTRAINT admin_actions_action_check;
ALTER TABLE admin_actions ADD CONSTRAINT admin_actions_action_check
  CHECK (action IN ('retry_step', 'new_generation', 'confirm_step', 'compensate', 'fail', 'resolve_conflict'));

-- +goose Down
-- Audit rows are never deleted: the narrower rule only applies to new rows.
ALTER TABLE admin_actions DROP CONSTRAINT admin_actions_action_check;
ALTER TABLE admin_actions ADD CONSTRAINT admin_actions_action_check
  CHECK (action IN ('retry_step', 'new_generation', 'confirm_step', 'compensate', 'fail')) NOT VALID;
UPDATE step_attempts SET kind = 'poll' WHERE kind = 'webhook';
ALTER TABLE step_attempts DROP CONSTRAINT step_attempts_kind_check;
ALTER TABLE step_attempts ADD CONSTRAINT step_attempts_kind_check CHECK (kind IN ('send', 'poll'));
DROP TABLE webhook_inbox;
