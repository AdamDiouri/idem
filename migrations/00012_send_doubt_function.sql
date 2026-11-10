-- +goose Up
-- One definition of "an earlier SEND attempt may have moved money" (slice
-- 2.3b): ambiguous, abandoned by a crash, or the attempt that left the step
-- DEAD (whose outcome is recorded as DEAD; a provably no-effect exhaustion is
-- recorded RETRY_NO_EFFECT instead). Polls send nothing and never count
-- (design D17). The claim, inbox and admin queries all call this.
-- +goose StatementBegin
CREATE FUNCTION step_send_doubt(p_step_id uuid) RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS (
    SELECT 1 FROM step_attempts
    WHERE step_id = p_step_id AND kind = 'send' AND outcome IN ('IN_DOUBT', 'ABANDONED', 'DEAD'))
$$;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION step_send_doubt(uuid);
