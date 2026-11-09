-- +goose Up
-- DEAD is the dead-letter state: the step will not run again without a human.
-- dead_reason says whether money may have moved (the *_in_doubt and
-- key_expired_unknown reasons) so the alert can say so.
ALTER TABLE steps ADD COLUMN dead_reason text
  CHECK (dead_reason IN ('attempts_exhausted_no_effect', 'attempts_exhausted_in_doubt', 'key_expired_unknown'));
ALTER TABLE steps ADD CONSTRAINT steps_dead_has_reason
  CHECK ((status = 'DEAD') = (dead_reason IS NOT NULL));

ALTER TABLE step_attempts DROP CONSTRAINT step_attempts_outcome_check;
ALTER TABLE step_attempts ADD CONSTRAINT step_attempts_outcome_check
  CHECK (outcome IN ('SUCCEEDED', 'FAILED_PERMANENT', 'IN_DOUBT', 'RETRY_NO_EFFECT', 'DEAD', 'ABANDONED'));

-- The sweeper looks for expired leases on every tick.
CREATE INDEX steps_running_lease ON steps (lease_expires_at) WHERE status = 'RUNNING';

-- +goose Down
DROP INDEX steps_running_lease;
ALTER TABLE step_attempts DROP CONSTRAINT step_attempts_outcome_check;
ALTER TABLE step_attempts ADD CONSTRAINT step_attempts_outcome_check
  CHECK (outcome IN ('SUCCEEDED', 'FAILED_PERMANENT', 'IN_DOUBT', 'ABANDONED'));
ALTER TABLE steps DROP CONSTRAINT steps_dead_has_reason;
ALTER TABLE steps DROP COLUMN dead_reason;
