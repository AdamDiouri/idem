-- name: InsertLedgerTransaction :exec
INSERT INTO ledger_transactions (id, workflow_id, step_id, kind)
VALUES (@id, @workflow_id, @step_id, @kind);

-- name: InsertLedgerEntry :exec
INSERT INTO ledger_entries (id, transaction_id, account, amount_cents, currency)
VALUES (@id, @transaction_id, @account, @amount_cents, @currency);
