# D21. The ledger is append-only by privilege; database logins are infrastructure

**Status:** Accepted 2026-11-10 · built in slice 2.3b (PR #11)

## Context
The design promised an append-only ledger, but every process connected as the table owner, which can update rows or drop the triggers that refuse updates. An external review flagged it. The first fix had `idem migrate` create the application's login; the owner then asked why an application manages database users at all.

## Decision
- Migration 00013 creates the group role `idem_app`: it may read and insert ledger rows and the `admin_actions` audit trail, but has no `UPDATE`, `DELETE` or `TRUNCATE` on them and no rights on the migrations table.
- Migrations run as the owner; the API, workers and sweepers log in as a member of `idem_app`.
- idem never creates logins. Whatever provisions the database creates the login: compose from `deploy/postgres/initdb` (re-runnable with `make app-user`), the tests and chaos harness as the owner, production with `CREATE ROLE … LOGIN IN ROLE idem_app`.

## Consequences
- A bug or compromise in a running service cannot rewrite money history; row triggers remain as a second layer that even the owner meets.
- The whole test suite and every chaos process run as the restricted user, so a missing grant fails a test, not production.
- `idem migrate` does not need the right to create roles.

## Alternatives rejected
- **Triggers only:** the owner can drop them.
- **`idem migrate` creates the login:** works, but makes the application manage credentials and need role-creation rights.
