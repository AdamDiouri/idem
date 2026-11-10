# D13. The outbox is written inside `transitionWorkflow`

**Status:** Accepted 2026-11-10 · built in slice 2.4 ([M2 design](../superpowers/specs/2026-11-10-milestone-2-design.md) §5.2)

## Context
Shops must learn each workflow's final outcome. Terminal transitions happen on many paths: worker completion, webhook apply, poll, admin actions. A notification sent from each path separately would eventually be forgotten on one of them, and one sent outside the transaction can be lost in a crash.

## Decision
The outbox row is inserted in the same transaction as the workflow's transition to a terminal state, inside the single function every path uses (`transitionWorkflow`). `UNIQUE (workflow_id, type)` makes a second event for the same transition fail loudly. The payload is frozen at the transition.

## Consequences
- No path can forget the event, and no event exists for a transition that rolled back.
- As built: the payload comes from the SQL function `workflow_view`, which `GET /workflows/{id}` also returns, so what a shop reads and what it is told cannot drift. Invariant I8 checks one event per terminal workflow, with the right type and payload. Workflows that ended before the outbox existed were backfilled by migration 00014.
- Delivery is a separate concern (D14).

## Alternatives rejected
- **Publish directly from each code path:** dual-write; loses events on crashes and forgets paths.
