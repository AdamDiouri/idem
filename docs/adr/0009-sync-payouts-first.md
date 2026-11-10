# D9. Payouts were synchronous in M1; async payouts and webhooks came first in M2

**Status:** Accepted 2026-11-08 · M1 synchronous; superseded in slice 2.1 (async payouts) and 2.2 (webhooks), as planned

## Context
Real payouts settle asynchronously and are announced by webhook. Building that first would have delayed a complete, chaos-tested flow by a milestone.

## Decision
M1 treated a payout as a synchronous call, while leaving room in the schema and state machine for a waiting state. M2 began with async payouts: `AWAITING_CALLBACK`, worker polling (D12), then signed provider webhooks.

## Consequences
- Milestone 1 ended with a full saga (charge, payout, refund compensation) proven by chaos.
- Slice 2.1 shipped polling before webhooks: the system is correct without webhooks, and webhooks only make it faster.

## Alternatives rejected
- **Async in M1:** more moving parts before the core was proven.
