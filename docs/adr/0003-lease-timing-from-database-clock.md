# D3. All lease and schedule timing comes from the database's `now()`

**Status:** Accepted 2026-11-08 · built from slice 1.2 · guarded by a test since slice 2.3b

## Context
Leases decide who owns a step. If workers compared their own clocks against expiry times, a worker whose clock runs slow would believe a lease is still valid after another worker took over (F24, clock skew).

## Decision
Every lease expiry, `run_after`, callback deadline and retry schedule is computed and compared in SQL with the database's `now()`. Go code passes durations, never timestamps.

## Consequences
- Worker clock skew cannot break ownership; there is only one clock.
- A static test (`internal/store/clock_test.go`) fails if a query touching a lease or schedule takes a time parameter from Go, including positional `$N` parameters.
- Fencing (D7) still covers the other way a lease goes stale: a paused process.

## Alternatives rejected
- **Worker-side timestamps:** correct only if every clock agrees.
