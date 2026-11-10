-- +goose Up
-- Async payouts (M2 slice 2.1): a step accepted by the provider waits in
-- AWAITING_CALLBACK until its callback deadline, then workers poll it. Polls
-- are attempts of kind 'poll'; they send nothing, so they never create doubt
-- (design D17). attempt_count counts sends (the retry budget), poll_count
-- counts polls, and attempt numbers are attempt_count + poll_count.
ALTER TABLE step_attempts ADD COLUMN kind text NOT NULL DEFAULT 'send' CHECK (kind IN ('send', 'poll'));
ALTER TABLE steps ADD COLUMN poll_count int NOT NULL DEFAULT 0 CHECK (poll_count >= 0);

ALTER TABLE step_attempts DROP CONSTRAINT step_attempts_outcome_check;
ALTER TABLE step_attempts ADD CONSTRAINT step_attempts_outcome_check
  CHECK (outcome IN ('SUCCEEDED', 'FAILED_PERMANENT', 'IN_DOUBT', 'RETRY_NO_EFFECT', 'DEAD', 'ABANDONED',
                     'AWAITING_CALLBACK'));

ALTER TABLE steps DROP CONSTRAINT steps_dead_reason_check;
ALTER TABLE steps ADD CONSTRAINT steps_dead_reason_check
  CHECK (dead_reason IN ('attempts_exhausted_no_effect', 'attempts_exhausted_in_doubt', 'key_expired_unknown',
                         'permanent_after_in_doubt', 'callback_unresolved'));

-- A waiting step must know what it waits for and until when.
ALTER TABLE steps ADD CONSTRAINT steps_awaiting_has_ref
  CHECK (status <> 'AWAITING_CALLBACK' OR (provider_ref IS NOT NULL AND callback_deadline IS NOT NULL));

DROP INDEX steps_claimable;
CREATE INDEX steps_claimable ON steps (run_after) WHERE status IN ('PENDING', 'IN_DOUBT', 'AWAITING_CALLBACK');

-- +goose Down
-- Map what only 00008 can express to its conservative pre-00008 meaning, so a
-- rollback works on a database that already ran async payouts: a waiting or
-- unresolved payout may have moved money, i.e. it is in doubt.
UPDATE step_attempts SET outcome = 'IN_DOUBT' WHERE outcome = 'AWAITING_CALLBACK';
UPDATE steps SET status = 'IN_DOUBT', callback_deadline = NULL, run_after = now() WHERE status = 'AWAITING_CALLBACK';
UPDATE steps SET dead_reason = 'attempts_exhausted_in_doubt' WHERE dead_reason = 'callback_unresolved';
DROP INDEX steps_claimable;
CREATE INDEX steps_claimable ON steps (run_after) WHERE status IN ('PENDING', 'IN_DOUBT');
ALTER TABLE steps DROP CONSTRAINT steps_awaiting_has_ref;
ALTER TABLE steps DROP CONSTRAINT steps_dead_reason_check;
ALTER TABLE steps ADD CONSTRAINT steps_dead_reason_check
  CHECK (dead_reason IN ('attempts_exhausted_no_effect', 'attempts_exhausted_in_doubt', 'key_expired_unknown',
                         'permanent_after_in_doubt'));
ALTER TABLE step_attempts DROP CONSTRAINT step_attempts_outcome_check;
ALTER TABLE step_attempts ADD CONSTRAINT step_attempts_outcome_check
  CHECK (outcome IN ('SUCCEEDED', 'FAILED_PERMANENT', 'IN_DOUBT', 'RETRY_NO_EFFECT', 'DEAD', 'ABANDONED'));
ALTER TABLE steps DROP COLUMN poll_count;
ALTER TABLE step_attempts DROP COLUMN kind;
