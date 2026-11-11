# Glossary

The words this project uses, in plain language, for readers new to payment systems or workflow engines. Each entry says what the thing is, why idem needs it, and where to find it. Terms in *italics* have their own entry.

## The story in one paragraph

A *shop* asks idem to take €100 from a *customer* and pay it, minus a *fee*, to a *merchant*. idem records that request as a *workflow* made of *steps*: a *charge* (take the customer's money through the *provider*), then a *payout* (send the merchant's share). Every step that succeeds is recorded in the *ledger*. If the payout is definitely refused, a *refund* gives the customer their money back (*compensation*). The network can fail at any moment, so every call carries an *idempotency key*: if idem has to retry, the provider recognises the request and does not move money twice. When idem cannot tell what happened, it does not guess: the step is *in doubt*, and if it stays that way a human decides (*parked*). When the workflow ends, an *outbox event* records the outcome so the shop can be told.

## The payment

- **Shop (client).** The business calling idem's API to make payments, for example an online marketplace. Authenticated by a bearer token; each token maps to a `client_id`, and a shop only sees its own workflows. *Code:* `internal/api/auth.go`; tokens in `IDEM_API_TOKENS`.
- **Customer.** The person paying. Identified by `customer_id` in the request; idem never stores card details.
- **Merchant.** The party being paid (for example a seller on the marketplace). Identified by `merchant_id`.
- **Provider (payment provider, PSP).** The external company that actually moves money, like Stripe or Adyen. idem only asks it to: charge a customer, pay a merchant, refund a charge. The provider is a separate *system of record*: it can know something idem does not. *Code:* client in `internal/provider/`.
- **Fake shop.** A stand-in for a shop receiving idem's outcome webhooks, used in compose and chaos runs. It verifies signatures, processes each event ID once, can be told to fail (down, slow, `5xx`, answering too late), and reports what it learned (`GET /_seen`), which invariant I9 checks. *Code:* `internal/fakeshop/`, binary `fakeshop`.
- **Fake provider.** A stand-in for a real provider used in development, tests and chaos runs. It behaves like a real one (idempotency keys, asynchronous payouts, signed webhooks) and can be told to fail in specific ways (*faults*). It also keeps the *oracle*. *Code:* `internal/fakeprovider/`, binary `fakeprovider`.
- **Charge.** Taking money from the customer through the provider. The first step of a workflow.
- **Payout.** Sending money to the merchant (the amount minus the fee). Payouts are *asynchronous*: the provider accepts the request now and settles it later.
- **Refund.** Returning a charge to the customer. idem only refunds when a payout has **definitely** failed, never on a guess.
- **Fee (platform fee).** What the platform keeps: 1.5% + €0.25 by default, computed in whole cents, fixed when the workflow is created (D11).
- **Cents (minor units).** All amounts are integers in the smallest currency unit (`10000` = €100.00), never floating point, so no rounding drift. *Code:* `internal/money/`.
- **Effect.** Something the provider actually did with money: a charge, a payout or a refund. Retries and network errors must never create a second effect for the same intention.

## Workflows and steps

- **Workflow.** One payment from start to finish: charge, payout, and if needed refund. One row in `workflows`, with a status (below) and the request's details in `input`. Created by `POST /workflows`. *Code:* `internal/workflow/` (creation), `internal/engine/` (execution).
- **Step.** One external action within a workflow (`charge`, `payout`, `refund`), with its own status, attempts and idempotency key. One row in `steps`. Steps run one after another; each successful step creates the next.
- **Saga.** A sequence of steps where, instead of one big transaction (impossible across companies), each step is undone, if needed, by a later *compensating* step. idem's saga is charge → payout, compensated by refund.
- **Compensation.** Undoing an earlier step with a new action, not a rollback: the charge happened, so a refund is a second, opposite movement of money. Both stay in the history.
- **Pivot step.** The step after which the saga can only move forward. Here it is the payout: once the merchant may have been paid, refunding the customer could pay out twice. That is why an *ambiguous* payout is never refunded automatically (the *pivot rule*).
- **Pivot rule.** If a payout is refused after an earlier attempt might have succeeded, idem parks it for a human instead of refunding. A real provider could refuse the retry even though the first attempt paid the merchant.
- **Hook.** The bookkeeping a step does when it finishes: post to the ledger, create the next step, move the workflow on. It runs inside the same database transaction as the step's completion, so either all of it happens or none does (D2). A hook that keeps failing parks the step (`hook_failed`).

### Workflow statuses

- `PENDING` → `RUNNING`: created, then being worked on.
- `SUCCEEDED`: customer charged, merchant paid.
- `FAILED`: the charge never happened (declined, or an operator confirmed nothing moved). Nothing to undo.
- `COMPENSATING` → `COMPENSATED`: the payout definitely failed, so the customer is being, then has been, refunded.
- `NEEDS_ATTENTION`: *parked*; a human must decide (see *Parked*).
- **Terminal.** `SUCCEEDED`, `FAILED` and `COMPENSATED`: final outcomes that never change again. Each gets exactly one *outbox event*.

### Step statuses

- `PENDING`: waiting to be picked up (now or after a backoff).
- `RUNNING`: a worker holds its *lease* and is calling the provider.
- `IN_DOUBT`: a call might have moved money, and idem does not know yet (see *Ambiguous outcome*). Retried with the same key.
- `AWAITING_CALLBACK`: the provider accepted an asynchronous payout; idem waits for a *webhook*, and *polls* if none comes.
- `SUCCEEDED`: the provider confirmed the effect.
- `FAILED_PERMANENT`: the provider definitely refused (for example `card_declined`). Its code is kept in `failure_code`.
- `DEAD`: idem gave up without knowing or without being able to finish; the workflow is parked. A `dead_reason` says why (see the [runbook](runbooks/needs-attention.md)).

## Doing things exactly once

- **Idempotent.** An operation that has the same result whether it runs once or many times. "Set the balance to 100" is idempotent; "add 10" is not.
- **Effectively-once.** idem's guarantee: work may be *attempted* several times (crashes and retries are normal), but each intended effect happens once. Achieved by at-least-once attempts plus idempotent effects.
- **Idempotency key.** A unique label for one *intended* effect. The receiver performs the first request with a key and, for every repeat, returns the original result instead of acting again. idem uses keys in two directions:
  - **Inbound:** the shop sends `Idempotency-Key` on `POST /workflows`, so a shop retrying after a timeout gets the same workflow back (`Idempotent-Replayed: true`) instead of a second payment.
  - **Outbound:** each step has its own key, derived once when the step is created and stored forever (D6). Every retry of that step sends the same key to the provider.
- **Replay.** Answering a repeated request with the stored original response.
- **Generation.** A counter in the outbound key. Only an operator can bump it (`idem admin new-generation`), after confirming with the provider that nothing happened, because a new key means a new payment as far as the provider is concerned.
- **Ambiguous outcome.** A provider call whose result idem cannot know: a timeout, a dropped connection, a `5xx`. The request may or may not have been performed. idem never treats this as a failure (D5): the step becomes `IN_DOUBT`.
- **No-effect outcome.** A failure that certainly moved no money (connection refused, `429 Too Many Requests`). Safe to retry freely.
- **Sticky doubt.** Once any attempt of a step might have moved money, that step's history remembers it, even if later attempts are clean refusals. The pivot rule reads it (SQL function `step_send_doubt`).

## Workers, leases and crashes

- **Worker.** A process (`idem worker`) that picks up due steps, calls the provider and records the result. Any number can run; they coordinate only through the database.
- **Claim.** A worker taking a due step, in one transaction, with `FOR UPDATE SKIP LOCKED` so two workers never take the same row.
- **Lease.** Time-limited ownership of a step by one worker. If the worker crashes, the lease expires and another worker can take over. Lease times come from the database's clock, never a worker's (D3).
- **Fencing token (`lease_epoch`).** A number that goes up every time a step changes owner. A worker can only save its result if the number is still the one it claimed with. A worker that paused (say, for a long garbage collection) and lost its lease is "fenced": its late write is rejected instead of overwriting the new owner's work (D7).
- **Sweeper.** A process (`idem sweeper`) that finds expired leases and hands those steps back: to `IN_DOUBT`, since the crashed worker might have called the provider, or, for a crashed poll, back to waiting, and exports the state metrics. It never calls the provider.
- **Attempt.** One try at a step, recorded in `step_attempts` with its outcome. Kinds:
  - `send`: a request that can move money.
  - `poll`: a status check (`GET`), which cannot move money, so it never creates doubt (D17).
  - `webhook`: a result applied from a provider webhook.
- **Backoff (with jitter).** Waiting longer after each failed attempt, with randomness so that many retries don't all hit the provider at the same moment.
- **Polling / callback deadline.** If no webhook arrives for a waiting payout by its `callback_deadline`, a worker asks the provider (`GET /payouts/{id}`). Polling is the safety net; webhooks only make things faster (D12).

## Money: the ledger

- **Ledger.** idem's own record of where money is: the accounting book. Every successful step writes to it in the same transaction as the step's completion, so the ledger and the step can never disagree (D2). *Code:* `internal/ledger/`.
- **Double-entry.** Every movement is recorded as entries on at least two accounts that sum to zero: money taken from one place is added to another. A mistake shows up as a non-zero sum (invariant I2). The database checks the sum when the transaction commits.
- **Account.** A bucket in the ledger:
  - `provider_receivable`: money held for us at the provider;
  - `merchant_payable`: money we owe merchants;
  - `platform_revenue`: fees earned.
- **Ledger transaction / entry / posting.** A *ledger transaction* groups the *entries* of one money movement (one charge, one payout, one refund); *posting* means writing it. A step can post only once (`UNIQUE (step_id, kind)`).
- **Append-only.** Ledger rows are never changed or deleted; a correction is a new, reversing transaction. Enforced twice: row triggers, and database privileges (the services' role cannot update or delete them; D21).

## Talking to the outside world

- **Webhook.** An HTTP request one system sends another to announce that something happened ("payout po_123 was paid"). Webhooks can arrive late, twice, out of order, or never, and anyone can try to forge one.
- **Signature (HMAC).** Proof that a webhook really comes from its sender: a hash of the body and a timestamp, computed with a shared secret. idem rejects unsigned, wrongly signed and stale-timestamped webhooks.
- **Inbox.** The table (`webhook_inbox`) where idem stores each verified provider webhook before answering `200`, deduplicated by the provider's event ID. Workers apply stored events later. Storing first means a crash cannot lose an acknowledged event. *Code:* `internal/webhook/`, `internal/engine/inbox.go`.
- **"A webhook is a prompt to ask."** idem never trusts a webhook's claim on its own: before changing anything, it re-checks the payout's current state with the provider (D16).
- **Stale event.** A webhook older than what idem already knows (for example "payout pending" arriving after "payout paid"). Recognised by the re-check and ignored.
- **Conflict.** A webhook whose re-checked truth contradicts what idem recorded. A terminal state is never flipped; the event is stored as `CONFLICT` and pages a human (D16).
- **Evidence.** Provider news about a *parked* step. It is shown to the operator next to the workflow, never applied automatically (D18).
- **Outbox.** The table where idem writes an event about each workflow outcome, in the **same transaction** as the outcome itself. So an event exists if and only if the outcome committed, and a crash can lose neither (D13). A separate process delivers events later.
- **Outbox event.** One row in `outbox`: the outcome (`workflow.succeeded`, `workflow.failed`, `workflow.compensated`) and the *workflow view* at that moment, frozen. Its ID is what a shop uses to ignore duplicates.
- **Workflow view.** What a shop is allowed to see of a workflow: its ID, a public `status` (`processing`, `succeeded`, `failed`, `compensated`), a `reason` for failures, amounts and timestamps. No internal details. Defined once, as the SQL function `workflow_view`, used by both `GET /workflows/{id}` and the outbox.
- **Relay.** The process (`idem relay`) that delivers outbox events to each shop's webhook URL, signed with that shop's secret, retrying with backoff. At-least-once: a shop may receive an event twice and deduplicates on its ID (D14). *Code:* `internal/relay/`.
- **Outbox event statuses.** `PENDING` (waiting for the relay), `DELIVERED` (the shop answered `2xx`), `DEAD` (the relay gave up after its retry budget; an operator can resend it), `NO_ENDPOINT` (the client has no webhook URL; it may poll `GET` instead, D19), `SKIPPED` (an outcome from before the outbox existed, deliberately not announced at upgrade).
- **Reconciliation** *(slice 2.6)*. A job that compares idem's ledger with the provider's own records and reports every difference without changing anything (D15).

## When a human must decide

- **Parked (`NEEDS_ATTENTION`).** idem stopped a workflow because finishing it would mean guessing about money. A step is `DEAD` with a reason, an alert fires, and nothing more happens until an operator acts. Parking is internal: the shop sees `processing`.
- **Dead reason.** Why a step was parked, which tells the operator what to check. Examples: `attempts_exhausted_in_doubt` (money may have moved), `attempts_exhausted_no_effect` (it certainly did not), `permanent_after_in_doubt` (the pivot rule), `callback_unresolved` (a payout never settled), `hook_failed` (bookkeeping kept failing). The [runbook](runbooks/needs-attention.md) has a section for each.
- **Admin actions.** Commands an operator runs on a parked workflow: `idem admin list`, `retry-step`, `confirm-step`, `compensate`, `fail`, `new-generation`. Each needs a reason and the workflow's current version (so two operators cannot act on stale information), and each is recorded in `admin_actions`.
- **Runbook.** The step-by-step guide for operators: [`docs/runbooks/needs-attention.md`](runbooks/needs-attention.md).
- **Alert.** A Prometheus rule that pages a human, for example when workflows need attention or the inbox backs up. Defined in `deploy/prometheus/alerts.yml`; each links to the runbook.

## Proving it works

- **Invariant.** A rule that must always hold, checked after every chaos run and by `idem check`. The main ones:
  - I1: at most one charge, payout and refund per workflow at the provider;
  - I2: every ledger transaction sums to zero;
  - I3: the ledger matches what the provider did;
  - I4: after things calm down, nothing is left half-done;
  - I5: no payout without a charge;
  - I6: one workflow per inbound key;
  - I7: each outcome matches the money that moved;
  - I8: one outbox event per finished workflow;
  - I9: the shop ends up knowing exactly idem's outcomes;
  - I10 and I11: the webhook inbox handled every event correctly.

  Full list in [design §13](design.md#13-invariants-checked-after-every-chaos-run) and the [M2 design §9](superpowers/specs/2026-11-10-milestone-2-design.md).
- **Oracle.** An independent record of the truth used to check invariants: the fake provider's log of effects it really performed (`GET /_effects`) and of webhooks it sent (`GET /_webhooks`). It counts per workflow, not per key, so a bug that invents a new key is still caught.
- **Chaos campaign.** A test that runs the real processes (several workers, sweepers and API replicas), sends payments, and breaks things on purpose: kills processes, pauses them, restarts the database, makes the provider fail. Then it waits for the system to settle and checks every invariant. `make chaos`; *code:* `internal/chaos/`, binary `chaos`.
- **Seed.** The number that decides a campaign's random fault plan, so a failing run can be replayed exactly.
- **Fault.** A failure the fake provider injects on request: a timeout after performing the effect, an error before it, a duplicate or reordered webhook, a bad signature, and so on.
- **Failpoint.** A named place in idem's own code where a test can make the process crash or pause on purpose, for example `after_provider_call` (the provider acted, idem has not saved it yet). *Code:* `internal/failpoint/`.
- **Settle (quiescence).** The phase after faults stop, when the system is given time to finish everything before the invariants are judged.
- **Vacuous run.** A chaos run that passed without proving anything, for example because its faults hit no traffic. The verdict fails such runs.
- **F-numbers, I-numbers, D-numbers.** IDs used across the docs:
  - F1, F2…: failure modes, in [design §12](design.md#12-failure-mode-table) and the M2 design §8;
  - I1, I2…: invariants;
  - D1, D2…: design decisions, each explained in [`docs/adr/`](adr/README.md).

## The pieces you run

- **`idem`.** One binary with subcommands:
  - `api`: the HTTP API;
  - `worker`: runs steps and applies webhooks;
  - `sweeper`: reclaims leases and exports state;
  - `migrate`: database schema;
  - `check`: runs the invariants against a live database;
  - `admin`: operator commands.
- **Postgres.** The only place state lives (D1). Every guarantee above rests on its transactions, locks and constraints.
- **`idem_app` role.** The restricted database role the services log in as: it can append to the ledger, audit trail and outbox but never rewrite or delete them (D21).
- **Migration.** A numbered SQL file in `migrations/` that changes the schema, applied in order by `idem migrate`.
