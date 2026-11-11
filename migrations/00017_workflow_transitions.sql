-- +goose Up
-- Slice 2.5b: the workflow state machine, written down once and enforced by
-- the database (D23). Before, the allowed moves existed only as the "from"
-- lists of each TransitionWorkflow call; a bug could make any move.

-- Final outcomes: never left again, each with exactly one outbox event (D13).
CREATE FUNCTION workflow_is_terminal(p_status text) RETURNS bool
LANGUAGE sql IMMUTABLE AS $$ SELECT p_status IN ('SUCCEEDED', 'FAILED', 'COMPENSATED') $$;

-- Work in progress: claimable, and watched by the stuck-workflow alert.
CREATE FUNCTION workflow_is_active(p_status text) RETURNS bool
LANGUAGE sql IMMUTABLE AS $$ SELECT p_status IN ('PENDING', 'RUNNING', 'COMPENSATING') $$;

-- The allowed moves (design §8.2). Writing a status again is always allowed.
CREATE FUNCTION workflow_transition_allowed(p_from text, p_to text) RETURNS bool
LANGUAGE sql IMMUTABLE AS $$
  SELECT p_from = p_to OR (p_from, p_to) IN (
    ('PENDING', 'RUNNING'),
    ('RUNNING', 'SUCCEEDED'), ('RUNNING', 'FAILED'), ('RUNNING', 'COMPENSATING'), ('RUNNING', 'NEEDS_ATTENTION'),
    ('COMPENSATING', 'COMPENSATED'), ('COMPENSATING', 'NEEDS_ATTENTION'),
    ('NEEDS_ATTENTION', 'RUNNING'), ('NEEDS_ATTENTION', 'COMPENSATING'), ('NEEDS_ATTENTION', 'FAILED')
  )
$$;

-- +goose StatementBegin
CREATE FUNCTION workflow_check_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT workflow_transition_allowed(OLD.status, NEW.status) THEN
    RAISE EXCEPTION 'workflow %: % → % is not an allowed transition', OLD.id, OLD.status, NEW.status
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER workflows_transition_check
  BEFORE UPDATE OF status ON workflows
  FOR EACH ROW EXECUTE FUNCTION workflow_check_transition();

-- The outbox rule (00014) now reads the one definition of terminal.
DROP TRIGGER workflows_terminal_has_event ON workflows;
CREATE CONSTRAINT TRIGGER workflows_terminal_has_event
  AFTER INSERT OR UPDATE OF status ON workflows
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW WHEN (workflow_is_terminal(NEW.status))
  EXECUTE FUNCTION workflow_check_has_event();

-- +goose Down
DROP TRIGGER workflows_terminal_has_event ON workflows;
CREATE CONSTRAINT TRIGGER workflows_terminal_has_event
  AFTER INSERT OR UPDATE OF status ON workflows
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW WHEN (NEW.status IN ('SUCCEEDED', 'FAILED', 'COMPENSATED'))
  EXECUTE FUNCTION workflow_check_has_event();
DROP TRIGGER workflows_transition_check ON workflows;
DROP FUNCTION workflow_check_transition();
DROP FUNCTION workflow_transition_allowed(text, text);
DROP FUNCTION workflow_is_active(text);
DROP FUNCTION workflow_is_terminal(text);
