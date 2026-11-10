# D20. Trace context is stored in the rows that hand work between processes

**Status:** Accepted 2026-11-10 · not yet built (planned for milestone 3; design §15, Observability)

## Context
Distributed tracing usually propagates context in RPC headers. idem hands work off through database rows instead: the API commits a workflow that a worker claims later; a webhook lands in the inbox and another worker applies it. Headers alone would split one payment into unrelated traces.

## Decision
The W3C `traceparent` is stored in the rows that carry work (`workflows`, `steps`, `webhook_inbox`, `outbox`). Whichever process continues the work starts a span linked to it, so one trace shows API → queue wait → claim → provider call → commit. Metrics carry exemplars linking latency buckets to traces.

## Consequences
- One payment is one trace, including time spent waiting in the database.
- A small column on several tables; the context is not sensitive.

## Alternatives rejected
- **Trace each process separately, or header-only propagation:** loses the handoffs, which is where the waiting happens.
