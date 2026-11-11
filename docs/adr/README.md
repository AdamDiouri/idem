# Architecture decision records

One file per decision, numbered like the decision IDs in [design §16](../design.md#16-decisions) and the [M2 design](../superpowers/specs/2026-11-10-milestone-2-design.md). Each records the context, what was decided, its consequences and the alternatives rejected.

D1 to D20 were first recorded as one-line rows in the design documents and expanded into these files on 2026-11-10. A new decision gets its row in the design document and its file here in the same pull request. A decision that is replaced keeps its file, with its status changed to "Superseded by Dn".

| ID | Decision | Status |
|---|---|---|
| [D1](0001-postgres-single-state-store.md) | Postgres is the single state store | Built (1.1) |
| [D2](0002-ledger-posting-in-success-transaction.md) | Ledger posting inside the step's success transaction | Built (1.2) |
| [D3](0003-lease-timing-from-database-clock.md) | All lease and schedule timing from the database's `now()` | Built (1.2), tested (2.3b) |
| [D4](0004-dead-letter-is-a-status.md) | A dead letter is a `DEAD` status, not a table | Built (1.3) |
| [D5](0005-ambiguous-outcomes-are-in-doubt.md) | Ambiguous outcomes become `IN_DOUBT`, never `FAILED` | Built (1.3) |
| [D6](0006-outbound-keys-stored-at-step-creation.md) | Outbound keys stored at step creation | Built (1.1, 1.6) |
| [D7](0007-every-ownership-change-bumps-lease-epoch.md) | Every ownership change bumps `lease_epoch` | Built (1.2) |
| [D8](0008-hand-written-sql-with-sqlc.md) | Hand-written SQL with `sqlc`, no ORM | Built (1.1) |
| [D9](0009-sync-payouts-first.md) | Synchronous payouts in M1, async first in M2 | Done (2.1, 2.2) |
| [D10](0010-single-currency.md) | A single currency, `currency` column kept | Built (1.1) |
| [D11](0011-platform-fees-in-integer-cents.md) | Fees in integer cents, half-to-even, fixed at creation | Built (1.1) |
| [D12](0012-workers-poll-waiting-payouts.md) | Workers poll waiting payouts, not the sweeper | Built (2.1) |
| [D13](0013-outbox-written-in-transition.md) | The outbox is written inside `transitionWorkflow` | Built (2.4) |
| [D14](0014-at-least-once-outbound-delivery.md) | At-least-once outbound delivery, stable event ID | Built (2.5) |
| [D15](0015-reconciliation-only-reports.md) | Reconciliation only reports | Built (2.6) |
| [D16](0016-contradictory-news-never-flips-terminal-state.md) | Contradictory news never flips a terminal state | Built (2.2) |
| [D17](0017-polls-never-create-doubt.md) | Polls never create doubt | Built (2.1) |
| [D18](0018-news-about-parked-steps-is-evidence.md) | News about a parked step is evidence for a human | Built (2.2) |
| [D19](0019-outbox-rows-for-every-client.md) | Outbox rows for every client, `NO_ENDPOINT` without a URL | Built (2.5) |
| [D20](0020-trace-context-stored-in-work-rows.md) | Trace context stored in work rows | Planned (M3) |
| [D21](0021-append-only-by-privilege-logins-are-infrastructure.md) | Ledger append-only by privilege; logins are infrastructure | Built (2.3b) |
| [D22](0022-toolchain-pinned-in-go-mod.md) | Go toolchain pinned in `go.mod`, on a supported release | Built (CI PR) |
| [D23](0023-state-transitions-enforced-by-the-database.md) | Workflow and step transitions enforced by the database | Built (2.5b) |
