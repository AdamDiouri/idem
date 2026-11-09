-- +goose Up
-- Every human intervention on a parked workflow is recorded here, in the same
-- transaction as the change it describes.
CREATE TABLE admin_actions (
  id          uuid        PRIMARY KEY,
  at          timestamptz NOT NULL DEFAULT now(),
  actor       text        NOT NULL CHECK (actor <> ''),
  action      text        NOT NULL CHECK (action IN ('retry_step', 'new_generation', 'confirm_step', 'compensate', 'fail')),
  workflow_id uuid        NOT NULL REFERENCES workflows(id),
  step_id     uuid        REFERENCES steps(id),
  reason      text        NOT NULL CHECK (length(trim(reason)) > 0),
  details     jsonb       NOT NULL DEFAULT '{}'
);
CREATE INDEX admin_actions_workflow ON admin_actions (workflow_id);

-- A refund reverses the charge recorded when it was created; that reference
-- must never change afterwards (every retry must send an identical request).
-- +goose StatementBegin
CREATE FUNCTION steps_refund_charge_ref_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.refund_charge_ref IS DISTINCT FROM NEW.refund_charge_ref THEN
    RAISE EXCEPTION 'refund_charge_ref of step % is immutable', OLD.id USING ERRCODE = '42501';
  END IF;
  RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER steps_refund_charge_ref_immutable
  BEFORE UPDATE OF refund_charge_ref ON steps
  FOR EACH ROW EXECUTE FUNCTION steps_refund_charge_ref_immutable();

-- +goose Down
DROP TRIGGER steps_refund_charge_ref_immutable ON steps;
DROP FUNCTION steps_refund_charge_ref_immutable();
DROP TABLE admin_actions;
