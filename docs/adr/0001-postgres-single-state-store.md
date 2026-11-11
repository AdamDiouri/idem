# D1. Postgres is the single state store

**Status:** Accepted 2026-11-08 · built from slice 1.1

## Context
Every guarantee idem makes is a multi-row fact: a step finishes, its ledger entries post, the next step appears and the workflow moves on, all or nothing. An inbound request's idempotency key, its workflow and its first step must appear together. Leases need compare-and-swap. Real payment systems keep this kind of state in a relational database for the same reasons.

## Decision
All workflow, step, ledger, inbox and audit state lives in one Postgres database. Processes (API, workers, sweepers) are stateless and coordinate only through it: row locks, `FOR UPDATE SKIP LOCKED` claims, unique constraints and deferred constraint triggers.

## Consequences
- Atomicity, uniqueness and fencing come from the database, not from code we would have to prove correct.
- Any number of each process can run; a process that dies loses nothing.
- Postgres is a single point of failure and the throughput ceiling. Database restarts are part of the chaos plan (F14); scaling it is a later concern (M5 explores an etcd-backed lease alternative behind the same interface).

## Alternatives rejected
- **A distributed key-value store (etcd, Redis):** we would rebuild transactions and constraints on top of it, and the project would drift from the payments problem to the storage problem.
