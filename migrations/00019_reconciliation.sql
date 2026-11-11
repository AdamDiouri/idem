-- +goose Up
-- Slice 2.6 (M2 design §6, D15): reconciliation reports. A run compares the
-- provider's record of money movements with idem's ledger and steps over a
-- window, and records every difference. It changes nothing else.

CREATE TABLE recon_runs (
  id            uuid        PRIMARY KEY,
  started_at    timestamptz NOT NULL DEFAULT now(),
  finished_at   timestamptz,
  window_from   timestamptz NOT NULL,
  window_to     timestamptz NOT NULL CHECK (window_to > window_from),
  status        text        NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
  records       int         NOT NULL DEFAULT 0, -- provider records read (window plus margin)
  workflows     int         NOT NULL DEFAULT 0, -- workflows compared
  in_flight     int         NOT NULL DEFAULT 0, -- skipped: not final yet
  discrepancies int         NOT NULL DEFAULT 0,
  error         text
);
CREATE INDEX recon_runs_latest ON recon_runs (finished_at DESC) WHERE status = 'SUCCEEDED';

CREATE TABLE recon_discrepancies (
  id              uuid    PRIMARY KEY,
  run_id          uuid    NOT NULL REFERENCES recon_runs(id),
  -- No foreign key: the provider may name a workflow idem does not know.
  workflow_id     uuid,
  kind            text    NOT NULL CHECK (kind IN ('missing_in_ledger', 'missing_at_provider', 'amount_mismatch',
                                                    'duplicate_effect', 'status_mismatch')),
  effect_type     text    NOT NULL CHECK (effect_type IN ('charge', 'payout', 'refund')),
  provider_refs   text[]  NOT NULL DEFAULT '{}',
  provider_amount bigint, -- signed, as the provider reports it; NULL when it has no record
  ledger_amount   bigint, -- the posting's provider_receivable entry; NULL when there is none
  parked          bool    NOT NULL, -- the workflow was in NEEDS_ATTENTION: evidence for a human
  detail          text    NOT NULL,
  -- What the discrepancy is about (workflow and effect type), and when that
  -- item happened (its provider movement or its posting): a later run whose
  -- window contains item_at and that does not find the same item_key again
  -- resolves it.
  item_key        text        NOT NULL,
  item_at         timestamptz NOT NULL
);
CREATE INDEX recon_discrepancies_run ON recon_discrepancies (run_id);
CREATE INDEX recon_discrepancies_workflow ON recon_discrepancies (workflow_id);
CREATE INDEX recon_discrepancies_item ON recon_discrepancies (item_key);

-- Discrepancies still open: the most recent finding of each item, unless a
-- later successful run covered it (its window contains item_at) and did not
-- find it again. A run over another window, or a quiet one, resolves nothing.
CREATE VIEW recon_open_discrepancies AS
SELECT DISTINCT ON (d.item_key) d.*, r.finished_at AS found_at
FROM recon_discrepancies d
JOIN recon_runs r ON r.id = d.run_id AND r.status = 'SUCCEEDED'
WHERE NOT EXISTS (
  SELECT 1 FROM recon_runs later
  WHERE later.status = 'SUCCEEDED' AND later.finished_at > r.finished_at
    AND later.window_from <= d.item_at AND d.item_at < later.window_to
    AND NOT EXISTS (SELECT 1 FROM recon_discrepancies again WHERE again.run_id = later.id AND again.item_key = d.item_key)
)
ORDER BY d.item_key, r.finished_at DESC;

-- A report is history: the services write it once and never rewrite it.
REVOKE DELETE ON recon_runs FROM idem_app;
REVOKE UPDATE, DELETE ON recon_discrepancies FROM idem_app;

-- +goose Down
DROP VIEW recon_open_discrepancies;
DROP TABLE recon_discrepancies;
DROP TABLE recon_runs;
