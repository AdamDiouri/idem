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
  detail          text    NOT NULL
);
CREATE INDEX recon_discrepancies_run ON recon_discrepancies (run_id);
CREATE INDEX recon_discrepancies_workflow ON recon_discrepancies (workflow_id);

-- A report is history: the services write it once and never rewrite it.
REVOKE DELETE ON recon_runs FROM idem_app;
REVOKE UPDATE, DELETE ON recon_discrepancies FROM idem_app;

-- +goose Down
DROP TABLE recon_discrepancies;
DROP TABLE recon_runs;
