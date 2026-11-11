# D18. Provider news about a parked step is evidence for a human

**Status:** Accepted 2026-11-10 (owner's decision) · built in slice 2.2

## Context
A step parked in `NEEDS_ATTENTION` because money may have moved is waiting for a human. Then a webhook arrives saying the payout was paid (or failed). Applying it automatically would resolve a money decision made under doubt without anyone looking.

## Decision
News about a parked step is re-checked with the provider and recorded as evidence on the inbox row (`EVIDENCE`, linked to the step). `idem admin list` shows it next to the workflow (e.g. `provider now says: paid po_…`), so resolving takes one `confirm-step` or `compensate`. The alert keeps paging until a human acts.

## Consequences
- Every money decision made under doubt ends with a human, who sees the answer.
- Resolution is fast, because the evidence is already in front of the operator.

## Alternatives rejected
- **Auto-resolve parked steps:** faster, and removes the human from exactly the cases that need one.
