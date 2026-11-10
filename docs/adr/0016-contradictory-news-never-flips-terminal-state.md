# D16. Contradictory provider news never flips a terminal state

**Status:** Accepted 2026-11-10 · built in slice 2.2

## Context
Webhooks arrive late, duplicated and out of order. A `payout.failed` may arrive after idem recorded the payout paid, either because the events were reordered or because something really went wrong.

## Decision
A step that is already terminal is never flipped by news. Instead, idem re-checks the provider's current status:
- it agrees with what idem recorded: the event was stale; the inbox row becomes `IGNORED` (`stale`), counted, nobody paged;
- it disagrees: a real conflict; the row becomes `CONFLICT` and `IdemWebhookConflict` pages. The workflow keeps its status and any refund in flight finishes.

"A webhook is a prompt to ask": every applied event is re-checked against the provider before it changes anything.

## Consequences
- Reordering is normal and harmless; only true disagreements reach a human.
- Invariant I10 checks it in chaos, including deliberately reordered events.

## Alternatives rejected
- **Last event wins:** how money moves twice.
