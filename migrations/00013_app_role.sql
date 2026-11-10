-- +goose Up
-- The application role (slice 2.3b, design §10). Migrations run as the table
-- owner; the API, workers and sweepers log in as a user that is a member of
-- idem_app (created by the operator's infrastructure, not by idem). That
-- role may read and append to the ledger but never UPDATE, DELETE or TRUNCATE
-- it, so the ledger is append-only by privilege, not only by triggers the
-- owner could drop.
-- +goose StatementBegin
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'idem_app') THEN
    CREATE ROLE idem_app NOLOGIN;
  END IF;
END
$$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO idem_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO idem_app;
REVOKE UPDATE, DELETE ON ledger_transactions, ledger_entries FROM idem_app;
REVOKE INSERT, UPDATE, DELETE ON accounts FROM idem_app;
-- The audit trail is append-only like the ledger; migrations are the owner's.
REVOKE UPDATE, DELETE ON admin_actions FROM idem_app;
REVOKE ALL ON goose_db_version FROM idem_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO idem_app;
-- Tables later migrations create are usable too (a new ledger table must
-- revoke UPDATE and DELETE itself). TRUNCATE is never granted.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO idem_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO idem_app;

-- A ledger transaction must have entries: an empty one passes the balance
-- check (nothing sums to zero) and records nothing. Checked at COMMIT, after
-- the entries are inserted.
-- +goose StatementBegin
CREATE FUNCTION ledger_check_has_entries() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM ledger_entries WHERE transaction_id = NEW.id) THEN
    RAISE EXCEPTION 'ledger transaction % has no entries', NEW.id USING ERRCODE = '23514';
  END IF;
  RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER ledger_transactions_has_entries
  AFTER INSERT ON ledger_transactions
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION ledger_check_has_entries();

-- +goose Down
DROP TRIGGER ledger_transactions_has_entries ON ledger_transactions;
DROP FUNCTION ledger_check_has_entries();
-- Plain REVOKEs, which the table owner may run (dropping what the role owns
-- needs its own privileges, which a managed-Postgres owner may lack). The role
-- itself is cluster-wide and its login users belong to it, so it is kept.
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE USAGE, SELECT ON SEQUENCES FROM idem_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM idem_app;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM idem_app;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM idem_app;
REVOKE USAGE ON SCHEMA public FROM idem_app;
