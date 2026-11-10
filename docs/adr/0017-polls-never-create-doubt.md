# D17. Polls are attempts of kind `poll` and never create doubt

**Status:** Accepted 2026-11-10 · built in slice 2.1

## Context
Polling a waiting payout (D12) uses the same claim and attempt machinery as sending. But a `GET` cannot move money. If a crashed or failed poll counted like a crashed send, it would make a step `IN_DOUBT`, and the pivot rule would then park clean failures.

## Decision
Attempts have a kind (`send`, `poll`, `webhook`). Polls are counted in `poll_count`, not in the send budget. A poll that errors or crashes returns the step to `AWAITING_CALLBACK`, never `IN_DOUBT`. The pivot rule reads only the history of send attempts (since slice 2.3b, through the SQL function `step_send_doubt`).

## Consequences
- Doubt means exactly "a send may have moved money", wherever it is read.
- A provider outage during polling delays a payout but never parks it as doubtful.

## Alternatives rejected
- **Treat every attempt alike:** simpler code, wrong parking decisions.
