-- +goose Up
-- A step whose success or failure hook keeps failing (a bug in posting or the
-- saga) used to loop: the completion rolled back, the lease expired, and a
-- poll went back to waiting with no budget counting it. Each failure is now
-- counted; at IDEM_HOOK_MAX_FAILURES the step is DEAD with hook_failed (slice
-- 2.3b). The provider DID act: if it succeeded, money moved (hook_failed);
-- if it refused with no earlier doubt, nothing moved (hook_failed_no_effect).
ALTER TABLE steps ADD COLUMN hook_failures int NOT NULL DEFAULT 0;

ALTER TABLE steps DROP CONSTRAINT steps_dead_reason_check;
ALTER TABLE steps ADD CONSTRAINT steps_dead_reason_check
  CHECK (dead_reason IN ('attempts_exhausted_no_effect', 'attempts_exhausted_in_doubt', 'key_expired_unknown',
                         'permanent_after_in_doubt', 'callback_unresolved', 'hook_failed',
                         'hook_failed_no_effect'));

-- +goose Down
UPDATE steps SET dead_reason = 'attempts_exhausted_in_doubt' WHERE dead_reason = 'hook_failed';
UPDATE steps SET dead_reason = 'attempts_exhausted_no_effect' WHERE dead_reason = 'hook_failed_no_effect';
ALTER TABLE steps DROP CONSTRAINT steps_dead_reason_check;
ALTER TABLE steps ADD CONSTRAINT steps_dead_reason_check
  CHECK (dead_reason IN ('attempts_exhausted_no_effect', 'attempts_exhausted_in_doubt', 'key_expired_unknown',
                         'permanent_after_in_doubt', 'callback_unresolved'));
ALTER TABLE steps DROP COLUMN hook_failures;
