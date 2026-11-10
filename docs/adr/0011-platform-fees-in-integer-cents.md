# D11. Platform fees: integer cents, half-to-even, fixed at creation

**Status:** Accepted 2026-11-08 · built in slice 1.1

## Context
Fees make the ledger realistic (three accounts, not two) and fee rounding is a classic source of ledger drift.

## Decision
A platform fee (default 1.5% + €0.25, configurable) is computed in integer cents with banker's rounding (half to even), never floats. It is fixed when the workflow is created and stored in its input, so retries and configuration changes cannot alter it mid-flight.

## Consequences
- The ledger's end state is exact and checkable: after a succeeded workflow, `provider_receivable` holds exactly the fee (design §10), and every transaction still sums to zero (I2).
- Changing the fee rate affects only new workflows.

## Alternatives rejected
- **No fees:** a simpler ledger that hides a real class of bugs.
