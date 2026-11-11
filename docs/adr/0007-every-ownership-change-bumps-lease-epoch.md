# D7. Every ownership change bumps `lease_epoch`

**Status:** Accepted 2026-11-08 · built in slice 1.2

## Context
A worker can pause (GC, a stopped container, a slow disk) past its lease. Another worker reclaims the step, and then the first one wakes up and tries to commit its result.

## Decision
`lease_epoch` is a fencing token. Every change of ownership increments it: a claim, a sweeper reclaim, an admin action, a webhook or poll applying a result. Every completion is a compare-and-swap on `(id, lease_epoch, status)`; zero rows affected means the lease was lost, so the worker rolls back, counts it as `fenced` and walks away.

## Consequences
- One rule with no special cases; reviewers check one thing.
- A fenced worker's provider call is not wasted: the new owner sends the same key and gets the same answer.

## Alternatives rejected
- **Bump the epoch only on claims:** leaves admin and sweeper changes unfenced, each needing its own reasoning.
