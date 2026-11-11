# D2. Ledger posting happens inside the step's success transaction

**Status:** Accepted 2026-11-08 · built in slice 1.2

## Context
After the provider confirms a charge, idem must record it in the double-entry ledger. If recording were a separate step, a crash between "step succeeded" and "ledger posted" would leave money moved but not booked, and recovering it would need its own detection and retry logic.

## Decision
The complete phase of an external step (§9.1, phase C) is one fenced transaction that marks the step `SUCCEEDED`, inserts the balanced ledger transaction, creates the next step or ends the workflow, and records the attempt's outcome. `UNIQUE (step_id, kind)` on `ledger_transactions` makes a second posting for the same step fail loudly.

## Consequences
- There is no window where the step and the ledger disagree; invariant I2 (every succeeded effect is posted exactly once) holds by construction.
- Step success hooks run inside the transaction, so a failing hook rolls back the whole completion. Slice 2.3b added a limit for hooks that keep failing (`IDEM_HOOK_MAX_FAILURES`, dead reason `hook_failed`).

## Alternatives rejected
- **A separate ledger step:** adds a crash window and another step to recover.
