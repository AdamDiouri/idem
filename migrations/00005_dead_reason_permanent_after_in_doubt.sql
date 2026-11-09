-- +goose Up
-- Pivot safety rule: a payout refused after an ambiguous attempt may still have
-- paid the merchant on that earlier attempt, so it is parked, not refunded.
ALTER TABLE steps DROP CONSTRAINT steps_dead_reason_check;
ALTER TABLE steps ADD CONSTRAINT steps_dead_reason_check
  CHECK (dead_reason IN ('attempts_exhausted_no_effect', 'attempts_exhausted_in_doubt', 'key_expired_unknown',
                         'permanent_after_in_doubt'));

-- +goose Down
ALTER TABLE steps DROP CONSTRAINT steps_dead_reason_check;
ALTER TABLE steps ADD CONSTRAINT steps_dead_reason_check
  CHECK (dead_reason IN ('attempts_exhausted_no_effect', 'attempts_exhausted_in_doubt', 'key_expired_unknown'));
