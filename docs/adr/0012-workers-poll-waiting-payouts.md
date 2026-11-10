# D12. Workers poll waiting payouts, not the sweeper

**Status:** Accepted 2026-11-10 · built in slice 2.1

## Context
An accepted async payout waits in `AWAITING_CALLBACK`. If no webhook arrives by its deadline, someone must ask the provider (`GET /payouts/{id}`). The original sketch (design §9.5) gave that job to the sweeper.

## Decision
A step past its `callback_deadline` becomes claimable by workers, with the ordinary claim, lease and fencing machinery, and the worker polls instead of sending. Polls back off up to `IDEM_CALLBACK_POLL_CAP`; after `IDEM_CALLBACK_MAX_WAIT` (72h) the step parks as `callback_unresolved`.

## Consequences
- Every provider call goes through one code path with leases and fencing; the sweeper stays database-only.
- A poll and a webhook may race; both apply through the same compare-and-swap, so the second sees "terminal, same outcome" and does nothing.

## Alternatives rejected
- **Sweeper polls:** a second process making provider calls, with its own concurrency rules to get right.
