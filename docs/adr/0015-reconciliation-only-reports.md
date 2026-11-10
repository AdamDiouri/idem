# D15. Reconciliation only reports

**Status:** Accepted 2026-11-10 · not yet built (planned for slice 2.6; [M2 design](../superpowers/specs/2026-11-10-milestone-2-design.md) §6)

## Context
`idem reconcile` compares the provider's balance-transaction records with idem's ledger. When they disagree, idem is by definition unsure which side is right.

## Decision
The job records discrepancies (`missing_in_ledger`, `missing_at_provider`, `amount_mismatch`, `duplicate_effect`, `status_mismatch`) and alerts. It changes nothing else. For a parked workflow, a discrepancy is shown as evidence next to it in `idem admin list`.

## Consequences
- An automated job never moves money or rewrites the ledger on a guess.
- Humans resolve differences with the audited admin actions; the report gives them the evidence.

## Alternatives rejected
- **Auto-correct the ledger:** turns a detection tool into a source of new errors.
