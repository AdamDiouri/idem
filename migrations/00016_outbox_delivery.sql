-- +goose Up
-- Slice 2.5 (M2 design §5.3): what the relay needs to deliver outbox events.
-- The constraint changes and the UPDATE scan outbox under the table's lock,
-- briefly blocking new outcomes; fine at this project's size. A large
-- deployment would add the constraints NOT VALID and validate them in a
-- separate, non-transactional migration.

-- Every change of an event's owner bumps lease_epoch, and a relay's
-- completion is a compare-and-swap on it (D7): a relay that paused past its
-- lease cannot overwrite the result of the relay that took over.
ALTER TABLE outbox ADD COLUMN lease_epoch bigint NOT NULL DEFAULT 0;

-- SKIPPED: deliberately not sent. Only for events backfilled by 00015, whose
-- outcomes predate the outbox: announcing them all when the first relay
-- starts would flood shops with old news (owner's decision). An operator can
-- still send one with `idem admin outbox redeliver`.
ALTER TABLE outbox DROP CONSTRAINT outbox_status_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_status_check
  CHECK (status IN ('PENDING', 'DELIVERED', 'NO_ENDPOINT', 'DEAD', 'SKIPPED'));

-- Backfilled events have random (v4) IDs; transitionWorkflow writes v7.
UPDATE outbox
SET status = 'SKIPPED',
    last_error = 'backfilled at upgrade: the outcome predates the outbox, so it was not announced'
WHERE status = 'PENDING' AND uuid_extract_version(id) = 4;

-- Redelivering an event is an audited admin action.
ALTER TABLE admin_actions DROP CONSTRAINT admin_actions_action_check;
ALTER TABLE admin_actions ADD CONSTRAINT admin_actions_action_check
  CHECK (action IN ('retry_step', 'new_generation', 'confirm_step', 'compensate', 'fail', 'resolve_conflict',
                    'redeliver_event'));

-- +goose Down
-- Audit rows are never deleted: the narrower rule only applies to new rows.
ALTER TABLE admin_actions DROP CONSTRAINT admin_actions_action_check;
ALTER TABLE admin_actions ADD CONSTRAINT admin_actions_action_check
  CHECK (action IN ('retry_step', 'new_generation', 'confirm_step', 'compensate', 'fail', 'resolve_conflict')) NOT VALID;
UPDATE outbox SET status = 'PENDING', last_error = NULL WHERE status = 'SKIPPED';
ALTER TABLE outbox DROP CONSTRAINT outbox_status_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_status_check
  CHECK (status IN ('PENDING', 'DELIVERED', 'NO_ENDPOINT', 'DEAD'));
ALTER TABLE outbox DROP COLUMN lease_epoch;
