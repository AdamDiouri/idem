-- +goose Up
CREATE TABLE step_attempts (
  step_id     uuid        NOT NULL REFERENCES steps(id),
  attempt_no  int         NOT NULL,
  lease_epoch bigint      NOT NULL,
  worker_id   text        NOT NULL,
  started_at  timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  outcome     text        CHECK (outcome IN ('SUCCEEDED','FAILED_PERMANENT','IN_DOUBT','ABANDONED')),
  http_status int,
  error       text,
  PRIMARY KEY (step_id, attempt_no)
);

CREATE TABLE accounts (
  code        text PRIMARY KEY,
  description text NOT NULL
);
INSERT INTO accounts (code, description) VALUES
  ('provider_receivable', 'Money held for us at the payment provider'),
  ('merchant_payable',    'Money owed to merchants'),
  ('platform_revenue',    'Platform fees earned');

-- Double-entry ledger: append-only, every transaction sums to zero.
-- Amounts are signed cents: debits positive, credits negative.
CREATE TABLE ledger_transactions (
  id          uuid        PRIMARY KEY,
  workflow_id uuid        NOT NULL REFERENCES workflows(id),
  step_id     uuid        NOT NULL REFERENCES steps(id),
  kind        text        NOT NULL CHECK (kind IN ('charge','payout','refund')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (step_id, kind)  -- a step posts at most once (GR2)
);

CREATE TABLE ledger_entries (
  id             uuid   PRIMARY KEY,
  transaction_id uuid   NOT NULL REFERENCES ledger_transactions(id),
  account        text   NOT NULL REFERENCES accounts(code),
  amount_cents   bigint NOT NULL CHECK (amount_cents <> 0),
  currency       text   NOT NULL CHECK (currency = 'EUR')
);
CREATE INDEX ledger_entries_transaction ON ledger_entries (transaction_id);

-- +goose StatementBegin
CREATE FUNCTION ledger_check_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  total bigint;
BEGIN
  SELECT COALESCE(sum(amount_cents), 0) INTO total
  FROM ledger_entries WHERE transaction_id = NEW.transaction_id;
  IF total <> 0 THEN
    RAISE EXCEPTION 'ledger transaction % is unbalanced (sum %)', NEW.transaction_id, total
      USING ERRCODE = '23514';
  END IF;
  RETURN NULL;
END;
$$;
-- +goose StatementEnd

-- Checked at COMMIT so all entries of a transaction can be inserted first.
CREATE CONSTRAINT TRIGGER ledger_entries_balanced
  AFTER INSERT ON ledger_entries
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION ledger_check_balanced();

-- +goose StatementBegin
CREATE FUNCTION ledger_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ledger is append-only: % on % is not allowed', TG_OP, TG_TABLE_NAME
    USING ERRCODE = '42501';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER ledger_entries_append_only
  BEFORE UPDATE OR DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
CREATE TRIGGER ledger_transactions_append_only
  BEFORE UPDATE OR DELETE ON ledger_transactions
  FOR EACH ROW EXECUTE FUNCTION ledger_append_only();

-- +goose Down
DROP TABLE ledger_entries;
DROP TABLE ledger_transactions;
DROP FUNCTION ledger_append_only();
DROP FUNCTION ledger_check_balanced();
DROP TABLE accounts;
DROP TABLE step_attempts;
