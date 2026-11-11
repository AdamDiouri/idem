# D4. A dead letter is a `DEAD` status, not a separate table

**Status:** Accepted 2026-11-08 · built in slice 1.3

## Context
Steps that run out of attempts, or that idem cannot finish without guessing about money, need to stop and wait for a human. Many queue systems move such messages to a dead-letter queue.

## Decision
A step that gives up becomes `DEAD` with a `dead_reason`, in place, and its workflow becomes `NEEDS_ATTENTION`. Admin actions work on that row.

## Consequences
- One source of truth: the step's history, attempts and keys stay where they were.
- No crash window between "removed from the queue" and "added to the dead-letter table".
- Parked work is found with an index on status; `idem admin list` and the `IdemWorkflowsNeedAttention` alert read it directly.

## Alternatives rejected
- **A dead-letter table:** two places to look, and a move that can half-happen.
