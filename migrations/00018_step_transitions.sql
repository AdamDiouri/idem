-- +goose Up
-- Slice 2.5b: the step state machine, enforced by the database (D23). Before,
-- CompleteStep accepted any target status from RUNNING, and only the Go code
-- choosing it kept the moves legal.

-- The allowed moves (design §8.3) and who makes them:
--   claim (ClaimStep)                      PENDING, IN_DOUBT, AWAITING_CALLBACK → RUNNING
--   completion, lease reclaim, hook limit  RUNNING → PENDING, IN_DOUBT, AWAITING_CALLBACK,
--                                                    SUCCEEDED, FAILED_PERMANENT, DEAD
--   provider webhook (ApplyCallbackToStep) AWAITING_CALLBACK → SUCCEEDED, FAILED_PERMANENT, DEAD
--   operator (retry-step, confirm-step)    DEAD, FAILED_PERMANENT → PENDING, IN_DOUBT, SUCCEEDED
-- SUCCEEDED is final. Writing a status again is always allowed.
CREATE FUNCTION step_transition_allowed(p_from text, p_to text) RETURNS bool
LANGUAGE sql IMMUTABLE AS $$
  SELECT p_from = p_to OR (p_from, p_to) IN (
    ('PENDING', 'RUNNING'), ('IN_DOUBT', 'RUNNING'), ('AWAITING_CALLBACK', 'RUNNING'),
    ('RUNNING', 'PENDING'), ('RUNNING', 'IN_DOUBT'), ('RUNNING', 'AWAITING_CALLBACK'),
    ('RUNNING', 'SUCCEEDED'), ('RUNNING', 'FAILED_PERMANENT'), ('RUNNING', 'DEAD'),
    ('AWAITING_CALLBACK', 'SUCCEEDED'), ('AWAITING_CALLBACK', 'FAILED_PERMANENT'), ('AWAITING_CALLBACK', 'DEAD'),
    ('DEAD', 'PENDING'), ('DEAD', 'IN_DOUBT'), ('DEAD', 'SUCCEEDED'),
    ('FAILED_PERMANENT', 'PENDING'), ('FAILED_PERMANENT', 'IN_DOUBT'), ('FAILED_PERMANENT', 'SUCCEEDED')
  )
$$;

-- +goose StatementBegin
CREATE FUNCTION step_check_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT step_transition_allowed(OLD.status, NEW.status) THEN
    RAISE EXCEPTION 'step %: % → % is not an allowed transition', OLD.id, OLD.status, NEW.status
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER steps_transition_check
  BEFORE UPDATE OF status ON steps
  FOR EACH ROW EXECUTE FUNCTION step_check_transition();

-- +goose Down
DROP TRIGGER steps_transition_check ON steps;
DROP FUNCTION step_check_transition();
DROP FUNCTION step_transition_allowed(text, text);
