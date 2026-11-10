# Journal

One write-up per delivery slice: what shipped, and what broke on the way. Entries up to slice 2.3b were written retrospectively on 2026-11-10 from the PR and commit records. Each new slice adds its entry in the same PR.

| Slice | Date | Entry | Summary |
|---|---|---|---|
| 1.1 | 2026-11-09 | [Skeleton and idempotent submit](2026-11-09-slice-1.1-skeleton.md) | `POST /workflows` replays safely on retry; one workflow per key. |
| 1.2 | 2026-11-09 | [Charge step end to end](2026-11-09-slice-1.2-charge-end-to-end.md) | Leases, fencing, a fake provider and a ledger; no double charge under crashes. |
| 1.3 | 2026-11-09 | [Retries, dead letters and ambiguity](2026-11-09-slice-1.3-ambiguity.md) | Backoff, sticky doubt, `DEAD` steps; Go's transport silently resent charges. |
| 1.4 | 2026-11-09 | [Payout and refund compensation](2026-11-09-slice-1.4-payout-and-compensation.md) | The full saga; an ambiguous payout is never refunded. |
| 1.5 | 2026-11-09 | [Chaos harness](2026-11-09-slice-1.5-chaos-harness.md) | Real processes under kill, pause and restart; the first verdicts were vacuous. |
| 1.6 | 2026-11-09 | [Operability](2026-11-09-slice-1.6-operability.md) | Audited admin actions, metrics, alerts and a runbook; milestone 1 complete. |
| 2.1 | 2026-11-10 | [Async payouts and polling](2026-11-10-slice-2.1-async-payouts.md) | Payouts accept with `202` and complete by polling; budgets in days, not minutes. |
| 2.2 | 2026-11-10 | [Provider webhooks](2026-11-10-slice-2.2-provider-webhooks.md) | Signed webhooks into a durable inbox; every event re-checked with the provider. |
| 2.3 | 2026-11-10 | [Async chaos](2026-11-10-slice-2.3-async-chaos.md) | Webhook faults and two API replicas in chaos; a verdict that cannot skip them. |
| 2.3b | 2026-11-10 | [Hardening](2026-11-10-slice-2.3b-hardening.md) | Chaos proves webhooks do real work; the ledger is append-only by privilege. |
