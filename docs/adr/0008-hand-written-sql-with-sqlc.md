# D8. Hand-written SQL, `sqlc`-generated Go on `pgx/v5`, no ORM

**Status:** Accepted 2026-11-09 · built from slice 1.1

## Context
The project is about transactions, locking and crash windows. Those must be visible in review. It is also a learning project: the SQL is part of what is being learned.

## Decision
Queries are written by hand in `internal/store/queries/*.sql`; `sqlc` generates typed Go for them on `pgx/v5`. Migrations are plain SQL run by goose. CI fails if the generated code is out of date with the queries.

## Consequences
- `FOR UPDATE SKIP LOCKED`, compare-and-swap predicates and `now()` are in plain sight.
- Type safety without hand-written row scanning.
- Dynamic queries are awkward; we have not needed them.

## Alternatives rejected
- **An ORM (GORM, ent):** hides transactions and locking, the very things this project is about.
