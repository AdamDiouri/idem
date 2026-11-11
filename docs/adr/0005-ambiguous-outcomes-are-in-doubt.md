# D5. Ambiguous outcomes become `IN_DOUBT`, never `FAILED`

**Status:** Accepted 2026-11-08 · built in slice 1.3

## Context
A timeout, a connection reset after connecting, a `5xx` or an unparseable response does not tell us whether the provider moved money. Treating it as a failure and moving on (or refunding, or retrying with a new key) is how payments happen twice.

## Decision
If the request might have reached the provider, the outcome is ambiguous: the step becomes `IN_DOUBT` and is retried with the same idempotency key, so the provider either performs it once or replays the original answer (§9.3, §9.4). `IN_DOUBT` is sticky: once a send may have moved money, every later state of that step remembers it. Only outcomes that certainly did nothing (`429`, connection refused) count as no-effect.

## Consequences
- The pivot rule follows from it: a payout refused after an ambiguous attempt is parked for a human, not compensated, because the earlier attempt may have paid the merchant.
- Exhausting attempts on a doubtful step parks it with a reason that says money may have moved, so the alert tells the operator what to check.

## Alternatives rejected
- **Treat timeouts as failures:** simple, and wrong exactly when it matters.
