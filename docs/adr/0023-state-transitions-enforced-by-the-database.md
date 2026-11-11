# D23. Workflow and step transitions are enforced by the database

**Status:** Accepted 2026-11-11 · built in slice 2.5b

## Context
`workflows.status` and `steps.status` were constrained to their values, but not to their moves. The allowed transitions existed only as the "from" lists of each compare-and-swap in Go, spread across the claim, completion, lease reclaim, webhook, hook-failure and admin code. `CompleteStep` accepted any target status from `RUNNING`. A review of implicit state machines found the design doc had drifted (it claimed `PENDING → NEEDS_ATTENTION` exists and omitted the admin exits), "terminal" was defined three times, "active" four, and four call sites each decided "money may have moved" their own way.

## Decision
- Each table has an allowed-edges function (`workflow_transition_allowed`, `step_transition_allowed`) and a `BEFORE UPDATE OF status` trigger that refuses anything else with SQLSTATE `23514`, naming the row and both statuses. Writing a status again is always allowed. The tables are in design §8.2 and §8.3.
- "Terminal" and "active" workflow statuses are SQL functions (`workflow_is_terminal`, `workflow_is_active`) reused by every query and by the outbox trigger; a test checks Go's `terminal()` agrees.
- In Go, `ClaimedStep.MoneyMayHaveMoved()` is the one definition of "an earlier send may have moved money", read by every decision.
- Test fixtures that need a state no legal path reaches use `testutil.Plant`, which runs as the table owner with triggers off. The services cannot bypass the triggers.

## Consequences
- A bug that makes an illegal move fails its transaction loudly instead of leaving a wrong state behind; the transition tables in the docs are the ones the database enforces.
- Adding a state or an edge is a migration, reviewed like schema.
- Inserts are not checked (only `Submit` and new steps insert, always `PENDING`).
- One inconsistency was fixed on the way: a step whose earlier send was ambiguous, refused "no effect" until its budget ran out, parked as `attempts_exhausted_no_effect` (money certainly not moved) when its status just before was not `IN_DOUBT`. It now parks as `attempts_exhausted_in_doubt`. The state appears unreachable today; the predicate makes it impossible.

## Alternatives rejected
- **Transition checks in Go only:** one more place to keep in sync, and bypassed by any query written directly.
- **A state-machine library:** an abstraction layer for two small tables that SQL already expresses.
