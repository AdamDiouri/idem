# D6. Outbound idempotency keys are stored at step creation

**Status:** Accepted 2026-11-08 · built in slice 1.1 (keys) and slice 1.6 (`new-generation`)

## Context
The provider deduplicates on our idempotency key. If the key were recomputed on each attempt, a code change, a bug or a changed input could produce a different key for the same payment, and the provider would perform it again.

## Decision
The key is derived once, when the step row is created (`hex(sha256(workflow_id ":" step_name ":" generation))`), stored on the row, and read from there forever after. A new key comes only from the audited admin action `idem admin new-generation --confirm-no-effect`, which an operator runs after verifying with the provider that nothing happened.

## Consequences
- Changing the derivation formula never affects existing steps.
- Every retry, reclaim and operator `retry-step` is safe by construction: same key.
- The one dangerous action is explicit, audited and needs a confirmation flag.

## Alternatives rejected
- **Recompute the key on each attempt:** silently double-pays the day the inputs or the formula change.
