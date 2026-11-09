-- +goose Up
-- A provider reference identifies exactly one effect; recording it on two steps
-- (e.g. an operator confirming a step with another workflow's charge ID) is a bug.
CREATE UNIQUE INDEX steps_provider_ref_unique ON steps (provider_ref) WHERE provider_ref IS NOT NULL;

-- +goose Down
DROP INDEX steps_provider_ref_unique;
