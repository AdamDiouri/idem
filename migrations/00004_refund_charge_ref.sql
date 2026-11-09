-- +goose Up
-- A refund reverses one specific charge. The reference is frozen when the
-- refund step is created, so every retry sends an identical request, and the
-- database refuses a refund without one (or any other step with one).
ALTER TABLE steps ADD COLUMN refund_charge_ref text;
ALTER TABLE steps ADD CONSTRAINT steps_refund_has_charge
  CHECK ((name = 'refund') = (refund_charge_ref IS NOT NULL));
CREATE INDEX steps_refund_charge_ref ON steps (refund_charge_ref) WHERE refund_charge_ref IS NOT NULL;

-- +goose Down
DROP INDEX steps_refund_charge_ref;
ALTER TABLE steps DROP CONSTRAINT steps_refund_has_charge;
ALTER TABLE steps DROP COLUMN refund_charge_ref;
