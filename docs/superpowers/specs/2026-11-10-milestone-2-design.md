# idem Milestone 2: nobody silently disagrees

**Status:** approved by the owner (2026-11-10)
**Builds on:** [`docs/design.md`](../../design.md) (the M1 spec, as built through slice 1.6). Section numbers like §9.5 refer to it. Terms are defined in the [glossary](../../glossary.md).
**Order agreed:** provider side first (async payouts, webhooks, polling), then shop side (status, outbox, outbound webhooks), then reconciliation.

---

## 1. Intent

M1 made idem correct when the payment provider answers synchronously. Real payments are not like that, and a payments engine sits between two parties that can each be wrong about what happened:

- **The provider** accepts a payout now and tells us the result minutes or hours later, by webhook. Webhooks get duplicated, delayed, reordered, lost and forged.
- **The shop** (the client calling idem's API) needs to know the true outcome, so its user is never told "paid" when nothing happened, or the reverse. Today idem has no way to tell it: there is no status endpoint and no notifications.

**The story M2 must let the author tell**, in an SRE, platform or SWE interview:

> "Our database and the provider are separate systems of record, so network failures can make them disagree. I didn't try to prevent that. I made every disagreement *detectable and convergent*. The truth reaches us through three channels: the provider's webhook, our own polling when a webhook doesn't come, and a reconciliation job that audits the result. Our client learns the outcome only after we've committed it, never lost and safe to receive twice. And I broke every one of those paths on purpose to show they hold."

Success is measured the M1 way: each failure mode has a deterministic test, the chaos harness exercises it at process level, and the invariant checker (plus, new in M2, the reconciliation job) reports zero disagreements.

## 2. Goals, non-goals, anti-goals

### Goals
1. **Async payouts.** `POST /payouts` returns `202 pending`; the result arrives by signed webhook or by our polling. Failure modes F15–F19 (§12) each get a test and a chaos fault.
2. **The shop always learns the true outcome.** `GET /workflows/{id}`, plus signed outbound webhooks delivered through a transactional outbox, at least once, never before the outcome is committed.
3. **Reconciliation.** A job compares idem's ledger against the provider's production-style records (not the test oracle) and reports every discrepancy, changing nothing.
4. **Chaos and invariants cover all of it:** webhook faults, a misbehaving shop, relay crashes; new invariants I8–I10 (§9).
5. **Operable:** metrics, alerts, runbook sections and admin commands for every new parking reason or backlog, as in slice 1.6.

### Non-goals (M2)
- A real provider integration. The fake provider stays the only one; it just becomes realistic about async behaviour.
- Generic workflow definitions. They move to **M4**, next to the Kubernetes `WorkflowDefinition` resource.
- Multiple providers, multiple currencies, partial payouts or partial refunds.
- **Exactly-once delivery to shops.** It is impossible across a network; M2 delivers at least once with a stable event ID the shop deduplicates on.
- Handling a payout *returned after success* (a bank bounce days later). M2 **detects** it (a webhook conflict, and reconciliation) and alerts; it does not model returns.
- Inbound API key expiry (still "forever", cheap at this scale), and the small CLI fixes from the 1.6 follow-up review (a separate cleanup slice).

### Anti-goals (would make M2 a failure even if it works)
- **Announcing an outcome before it is committed**, or announcing one that later changes.
- **Acknowledging a provider webhook (`2xx`) before it is durably stored.**
- **Flipping a terminal state** because of a late or contradictory webhook.
- **Trusting an unsigned or stale webhook**, inbound or making outbound ones unverifiable.
- **Webhook processing that bypasses the M1 rules:** the ledger posting in the same transaction as the outcome, compare-and-swap transitions, and the pivot safety rule.
- **Reconciliation that "fixes" drift by itself.** Drift is a human decision; the job only makes it loud.

## 3. Constraints

Everything in `docs/design.md` §4–§7 still holds. In particular:
- Go, pinned by `go.mod`'s toolchain line and kept on a supported release (D22; Go 1.25 with `GOTOOLCHAIN=local` until 2026-11-10); Postgres as the only state store (D1); sqlc, goose, pgx, cobra, caarlos0/env, client_golang.
- Effectively-once = at-least-once attempts + idempotent effects. Every external call keeps its stored idempotency key (D6).
- **Durable before acknowledged** (design principle 6), now for webhooks in both directions.
- Each slice: plan → native execution → independent review → fixes → PR. Tests against real Postgres; `make test`, `make lint`, `make build-release` and a chaos campaign pass. Commits never carry a `Co-Authored-By` line.
- Runs on docker compose; the chaos harness runs real processes.

## 4. Part A — async payouts (provider → idem)

### 4.1 Fake provider becomes asynchronous
- `POST /payouts` validates and returns **`202`** with `{"id": "po_…", "status": "pending"}`. Idempotency is unchanged: the same key replays the same `202` and the same payout ID. Invalid merchants (`mer_invalid*`) are still refused synchronously with `422`, as real providers do for validation.
- Each pending payout **settles** after a configurable delay (default 2s; seconds in tests) to `paid`, or to `failed` when a fault rule says so (`payout_fail`, `failure_code` e.g. `account_closed`). Settlement is the moment the effect lands in the effects oracle (a `failed` payout is not an effect).
- On settlement the provider sends a **webhook** to its configured URL: `POST` with JSON `{"id": "evt_…", "type": "payout.paid" | "payout.failed", "created": <unix>, "data": {"payout_id", "status", "failure_code", "amount_cents", "currency", "idempotency_key", "metadata": {"workflow_id"}}}`.
  - Signed with header `Idem-Provider-Signature: t=<unix>,v1=<hex HMAC-SHA256(secret, "<t>.<raw body>")>` (Stripe's scheme, §9.2).
  - The provider **retries** any non-`2xx` or timeout with backoff (e.g. 1s, 2s, 4s… capped, for a bounded total time), like real providers.
- `GET /payouts/{id}` returns the current status (`pending`, `paid`, `failed`). This is what our polling uses.
- **Webhook faults** (`PUT /_faults`, seeded, counted in `FaultHits`): `dup_webhook` (send twice), `fast_webhook` (send before the `202` response is written), `delay_webhook`, `reorder` (a stale contradictory event after the real one), `drop_webhook` (never send, no retries), `bad_signature`.
- Charges and refunds stay synchronous.

### 4.2 Classification and the `AWAITING_CALLBACK` state
- `202` with a payout ID → class `ACCEPTED_ASYNC` (§9.3). The step goes `AWAITING_CALLBACK`, records `provider_ref = po_…`, and sets `callback_deadline = now() + IDEM_CALLBACK_DEADLINE` (default 10m; seconds in chaos), in the same fenced completion transaction as any M1 outcome. The lease is released: nobody owns a waiting step.
- Nothing is posted to the ledger at acceptance. The payout posting happens when the payout is **paid** (D2 still holds: posting and outcome in one transaction).
- A `202` without a parseable payout ID is `AMBIGUOUS` → `IN_DOUBT` (same key re-sent; the provider replays).
- The **pivot safety rule** still applies to the final outcome: a payout that ends `failed` after any earlier ambiguous **send** attempt is parked (`permanent_after_in_doubt`), not refunded. A payout that fails cleanly is compensated by refund, as in M1.
  - The rule reads the step's **attempt history** (an attempt that was `IN_DOUBT` or `ABANDONED`), not `prior_status`, which is lost once the step waits in `AWAITING_CALLBACK`.
  - Attempts get a `kind` (`send` or `poll`); only `send` attempts count. A poll is a `GET`: it cannot move money, so a crashed or timed-out poll is never doubt. *(builder check Q5; my call)*

### 4.3 Receiving provider webhooks: the inbox
- **Endpoint:** `POST /webhooks/provider` on the API process. It is authenticated by signature only (no bearer token).
- **Order of operations:**
  1. Read the raw body (size limit, e.g. 64 KiB).
  2. Verify the signature in constant time against the current secret, and also the previous one if configured (`IDEM_PROVIDER_WEBHOOK_SECRETS`, comma-separated, for rotation). Reject if `|now − t| > 5m`. Failures → `400`, nothing stored, counted by reason.
  3. Parse just enough to get `id` and `type`; malformed → `400`.
  4. `INSERT INTO webhook_inbox … ON CONFLICT (provider_event_id) DO NOTHING`, commit.
  5. Respond `200` (also for duplicates, so the provider stops retrying). If the insert fails → `503`, so the provider retries.
- **`webhook_inbox`** (new table): `provider_event_id text PRIMARY KEY`, `type`, `payload bytea` (raw body, byte-exact), `received_at`, `status` (`PENDING`, `PROCESSED`, `IGNORED`, `EVIDENCE`, `CONFLICT`), `attempts`, `next_attempt_at`, `processed_at`, `last_error`, `step_id` (once matched).
- Failpoint `webhook_after_insert` (after commit, before the `200`): the provider retries, the duplicate is absorbed.

### 4.4 Applying webhooks
- **Who:** the worker process runs an inbox loop next to its step loop. It claims due `PENDING` rows with `FOR UPDATE SKIP LOCKED`; any number of workers may run.
- **Matching:** by `provider_ref` (the payout ID); if no step has it yet, by the `idempotency_key` in the payload.
- **Applying** is one transaction with the same hooks as a worker completion:
  - The step is `AWAITING_CALLBACK` → compare-and-swap on that status, bump `lease_epoch`, and apply `paid` (step `SUCCEEDED`, ledger posting, workflow `SUCCEEDED`, outbox event) or `failed` (step `FAILED_PERMANENT`, then the M1 decision: refund, or park under the pivot rule). Inbox row `PROCESSED`.
  - **The webhook beat our commit (F16):** no step matches yet, or the matched step is still `RUNNING`/`IN_DOUBT`. Leave the row `PENDING` and retry with backoff. When our own commit lands (or our retry re-sends the same key and the provider replays the same payout ID), the next attempt matches.
  - **Duplicate or late, same outcome:** the step is already terminal with the same result → `PROCESSED`, no-op.
  - **Already decided, news disagrees (F17):** the step is already terminal with the *other* result (e.g. `payout.failed` after we recorded paid), or the event contradicts an earlier event. **Never flip, never interrupt the saga.** Instead, re-check the current truth with `GET /payouts/{id}`:
    - **The provider's current status agrees with what idem recorded:** the event was stale (a reorder, a provider retry of an old event). The row becomes `IGNORED` with reason `stale`; it is logged and counted, but nobody is paged.
    - **The provider's current status disagrees:** a real conflict, where money may have moved both ways (e.g. the merchant was paid *and* the customer refunded). The row becomes `CONFLICT`, and `IdemWebhookConflict` **pages**. The workflow keeps its status and any refund in flight finishes: stopping it halfway would leave a refund at the provider with no ledger posting. Recovering the money happens outside idem (e.g. claw back from the merchant). The operator closes the conflict with an audited `idem admin conflict resolve <event-id> --reason …`; `idem admin conflict list` shows the open ones.
    - The re-check runs in the worker's inbox loop like a poll. A failed `GET` leaves the row `PENDING` for retry.
    - *(builder check Q2; my call. There is no `webhook_conflict` parking, so nothing ever has to park a non-`DEAD` step.)*
  - **The step is parked (`DEAD`): evidence, not resolution** *(owner's decision, 2026-11-10)*. A parked workflow leaves `NEEDS_ATTENTION` only through a human (§8.2 unchanged). The news is re-checked with `GET /payouts/{id}` and recorded as **evidence** on the inbox row (status `EVIDENCE`, linked to the step). `idem admin list` shows it next to the workflow, e.g. `provider now says: paid po_…`, so resolving takes one `confirm-step` (or `compensate --confirm-no-effect` for `failed`). `IdemWorkflowsNeedAttention` keeps paging until a human acts. Rationale: money decisions made under doubt always end with a human, and the system puts the answer in front of them.
  - **An operator already resolved the workflow:** news that agrees with the operator's action is `PROCESSED` (no-op). News that contradicts it (e.g. `payout.paid` after `compensate`) is a real conflict as above and pages.
  - **Orphan:** a row that still matches nothing after `IDEM_INBOX_ORPHAN_AFTER` (default 1h) → `IGNORED` with `last_error`, counted, alerted (it may be a provider bug, or a payout idem never knew about, which reconciliation will also report).
- Failpoint `inbox_before_commit`.

### 4.5 When no webhook comes: deadline polling (F18)
- A step `AWAITING_CALLBACK` past `callback_deadline` becomes **claimable by workers**, with the ordinary claim, lease and fencing machinery. Instead of `POST`, the worker calls `GET /payouts/{provider_ref}`:
  - `paid` / `failed` → applied exactly like the webhook (same function).
  - still `pending` → back to `AWAITING_CALLBACK` with the next deadline (backoff, capped), counting a poll.
  - network error / `5xx` → back to `AWAITING_CALLBACK` with a short deadline; a poll is never ambiguous, because a `GET` moves no money.
- Polls back off from `IDEM_RETRY_BASE` up to `IDEM_CALLBACK_POLL_CAP` (default 1h). After `IDEM_CALLBACK_MAX_WAIT` since acceptance (default 72h; real bank payouts take hours or days), or an optional `IDEM_CALLBACK_MAX_POLLS` per acceptance (default 0 = none), the step goes `DEAD` with a new reason **`callback_unresolved`** (money may have moved) → workflow `NEEDS_ATTENTION`, with a runbook section. An operator's retry re-sends (the provider replays the same payout) and starts a fresh budget. *(Revised in slice 2.1 review: a fixed 8 polls was about 15 minutes.)*
- A waiting step is healthy for the stuck alert until its poll is due (`IdemWorkflowsStuck` measures time without progress, and a scheduled poll is progress until it is due).
- **A worker crash mid-poll:** lease reclaim puts a step that was claimed for a poll (it has a `provider_ref` and `poll` attempt) back to `AWAITING_CALLBACK` with a short deadline, **not** `IN_DOUBT`. Nothing was sent, so there is nothing to doubt and no reason to re-send the `POST`. The attempt is recorded `ABANDONED` with kind `poll`, which the pivot rule ignores. *(builder check Q5; my call)*
- This deliberately differs from §9.5, which had the *sweeper* poll: workers already own every provider call, leases and fencing. The sweeper stays database-only.
- Webhook and poll may race; both apply through the same compare-and-swap, so whichever lands second sees "terminal, same outcome" and does nothing.

### 4.6 Operability (part A)
- **Metrics:**
  - `idem_webhooks_received_total{result}` (accepted, duplicate, bad_signature, stale, malformed, store_error)
  - `idem_webhooks_applied_total{outcome}` (applied, noop, deferred, conflict, orphaned)
  - `idem_inbox_pending` and `idem_inbox_oldest_pending_age_seconds` (sweeper snapshot)
  - `idem_callback_polls_total{result}`
  - `idem_steps{…,status="AWAITING_CALLBACK"}` (already exported by status)
- **Alerts:**
  - `IdemInboxBacklog`: oldest pending row too old
  - `IdemWebhookSignatureFailures`: a sustained rate of bad signatures (a misconfigured secret, or an attack)
  - `IdemWebhookConflict`
  - `IdemCallbacksOverdue`: steps past their deadline with polls failing
- **Runbook:** sections for `callback_unresolved`, real webhook conflicts (`idem admin conflict`), stale and orphaned events, signature failures and secret rotation.

## 5. Part B — the shop learns the true outcome

### 5.1 `GET /workflows/{id}`
- Bearer-authenticated like `POST`. A client can only read its own workflows; anything else is `404` (no existence leak).
- Returns `workflow_id`, `status`, `reason`, `amount_cents`, `fee_cents`, `currency`, `created_at`, `updated_at`. *(As built in slice 2.4, owner-approved: two machine-readable fields instead of one `outcome` string such as `failed: card_declined`.)*
  - `status` is `processing`, `succeeded`, `failed` or `compensated`. `PENDING`, `RUNNING`, `COMPENSATING` and `NEEDS_ATTENTION` are all **`processing`**: none is an outcome yet, and parking reasons are internal.
  - `reason` comes only with `failed` (the charge's provider code if it is a plain token, e.g. `card_declined`, else `charge_failed`) and `compensated` (`payout_failed`). Raw provider text never reaches the shop.
  - The view is one SQL function, `workflow_view`, which the outbox also stores (§5.2), so the two cannot drift.
- No step or ledger internals, and no IDs other than the workflow's.
- For "my `POST` timed out, what happened?" the answer stays: retry the `POST` with the same key (it replays). The README says so.

### 5.2 The outbox
- **`outbox`** (new table): `id uuid` (v7, also the event ID the shop deduplicates on), `client_id`, `workflow_id`, `type` (`workflow.succeeded`, `workflow.failed`, `workflow.compensated`), `payload bytea`, `created_at`, `status` (`PENDING`, `DELIVERED`, `NO_ENDPOINT`, `DEAD`), `attempts`, `next_attempt_at`, `lease_owner`, `lease_expires_at`, `delivered_at`, `last_error`, and `UNIQUE (workflow_id, type)`.
- **Written in the same transaction as the workflow's transition to a terminal state,** inside `transitionWorkflow`, so every path (worker completion, webhook apply, poll, admin action) emits it and none can forget. One terminal transition ⇒ exactly one event (the unique constraint makes a bug loud).
- The payload is the same JSON the `GET` returns, frozen at the transition.

### 5.3 The relay (outbound webhooks)
- A new subcommand **`idem relay`**, its own compose service; any number may run.
- It claims due `PENDING` rows with a lease (`FOR UPDATE SKIP LOCKED`, lease TTL), and `POST`s them to the client's webhook URL:
  - headers `Idem-Event-Id: <outbox id>` and `Idem-Signature: t=…,v1=…` (HMAC-SHA256 with the client's secret, same scheme as inbound)
  - request timeout below the lease
- `2xx` → `DELIVERED`. Anything else, or a timeout → backoff with full jitter, `attempts++`. After `IDEM_RELAY_MAX_ATTEMPTS` (default 15, spanning roughly a day at the default cap) → `DEAD`, metric and alert. An admin command re-queues it.
- **At least once, by design:** a relay that crashes after the shop answered `2xx` but before marking `DELIVERED` sends again. The shop deduplicates on `Idem-Event-Id`. The README's "Receiving webhooks" section tells shops exactly that, and how to verify the signature.
- No ordering promise is needed: a workflow has exactly one terminal event.
- **Client configuration** stays environment-based, like M1 tokens: `IDEM_CLIENT_WEBHOOKS="demo=http://shop:8000/hooks"` and `IDEM_CLIENT_WEBHOOK_SECRETS="demo=…"`. URLs are operator-set only (never via the API), so no request can make idem call an arbitrary URL (SSRF). **A client with no URL configured** *(owner's decision, D19)*: its outbox rows are still written (I8 has no special case, and the history exists if a URL is added later). The relay marks them `NO_ENDPOINT` (not `DELIVERED`: nothing was delivered), with no alert, because polling `GET /workflows/{id}` is a legitimate integration. `idem admin outbox redeliver` can send them once a URL is configured. The relay logs at startup which clients have no URL.
- Failpoints: `relay_after_send` (before marking delivered), `relay_before_send`.
- *As built in slice 2.5 (owner-approved):*
  - Retry defaults `IDEM_RELAY_RETRY_BASE=30s`, `IDEM_RELAY_RETRY_CAP=6h` with full jitter: 15 attempts take about 16 hours on average, at most about 32.
  - A third header, `Idem-Event-Type`, lets a shop route without parsing the body.
  - Every change of an event's owner bumps `outbox.lease_epoch` (D7). The relay never follows a redirect and reads at most 4 KiB of an answer.
  - Events backfilled at the 2.4 upgrade are `SKIPPED` (migration 00016): announcing every historical outcome at once would flood shops; `redeliver` can still send one.

### 5.4 Fake shop
- `cmd/fakeshop`, test-only like the fake provider. It receives webhooks, verifies signatures, deduplicates by event ID, and records what it learned.
- It has faults: `down` (connection refused), `slow`, `5xx`, `ack_then_5xx`, `ack_after_timeout` (does the work, answers too late).
- `GET /_seen` is the shop's ground truth for invariant I9.

### 5.5 Operability (part B)
- **Metrics:**
  - `idem_outbox_pending`, `idem_outbox_oldest_pending_age_seconds`, `idem_outbox_dead`
  - `idem_relay_deliveries_total{result}`, `idem_relay_delivery_duration_seconds`
  - `idem_workflow_status_requests_total{result}`
- **Alerts:** `IdemOutboxBacklog`, `IdemOutboxDead`.
- **Admin:** `idem admin outbox list [--dead]` and `idem admin outbox redeliver <event-id> --reason …` (audited, version-guarded like 1.6).
- **Runbook:** a section for the shop being down, and for redelivery.

## 6. Part C — reconciliation

- **Production-style provider records.** The fake provider gains `GET /reports/balance_transactions?created_gte=&created_lt=`, paginated. Unlike `/_effects` (the test oracle), this is what a real provider offers, so the job could run against a real one. *(builder check Q4; my call)*
  - **One row per money movement:** a succeeded charge, a succeeded refund, a **paid** payout. Each has the provider ID, the type, a signed amount, `created` = the moment the money moved (settlement time for a payout), and metadata `workflow_id` and `idempotency_key`.
  - **A failed payout has no row,** because no money moved. This matches §4.1 (a failed payout is not an effect) and §4.2 (idem posts nothing until paid). The real-provider pattern of a debit at acceptance plus a reversal adds nothing here and is out of scope.
- **`idem reconcile`** subcommand: reconcile a time window (default: the last complete hour, ending `IDEM_RECON_LAG` ago).
  - **Not-yet-final work is never a discrepancy.** A workflow with any step `PENDING`, `RUNNING`, `IN_DOUBT` or `AWAITING_CALLBACK` is skipped and counted as `in_flight`. Its polling budget ends in `DEAD` (§4.5), which is then judged as below. So the lag only has to cover the provider report's own delay, not the callback deadline: default 5m. *(builder check Q4; my call)*
- **It compares** the provider's records with idem's ledger postings and steps, and classifies each difference:
  - `missing_in_ledger`: the provider moved money that we never recorded
  - `missing_at_provider`: we posted, the provider has no record
  - `amount_mismatch`
  - `duplicate_effect`: more than one charge, payout or refund for a workflow
  - `status_mismatch`: e.g. paid at the provider but `AWAITING_CALLBACK` past the lag
- **Parked workflows are reported, not skipped,** and flagged `parked`. For them a discrepancy is *evidence*: "the provider shows a payout for this workflow that the ledger doesn't have" is exactly what the operator needs to choose `confirm-step` over `compensate`. `idem admin list` shows a parked workflow's discrepancies next to it. *(builder check Q3; my call)*
- **Results** go to `recon_runs` and `recon_discrepancies` (with the workflow ID and the `parked` flag). The job changes **nothing** else.
- **Surfaces:** metric `idem_reconciliation_discrepancies{kind}` and `idem_reconciliation_last_success_timestamp_seconds`; alerts `IdemReconciliationDrift` and `IdemReconciliationStale`; `idem admin recon list`; a runbook section per kind.
- **Scheduling:** compose runs it every N minutes (a small loop service). In M4 it becomes a Kubernetes CronJob.
- **In chaos:** after settling, the harness runs `idem reconcile` over the whole campaign window. It requires **zero discrepancies on every workflow that is not parked**, and discrepancies on parked ones to agree with the oracle. That is I3 proven through the production path, not just the oracle. The claim stays honest: "every disagreement is either absent, or parked and shown to a human with the evidence".

## 7. Schema changes (summary)

New migrations, in slice order:
- `webhook_inbox` (§4.3)
- `steps`: `poll_count int NOT NULL DEFAULT 0`; a claim index covering `AWAITING_CALLBACK` past its deadline
- `dead_reason` check: add `callback_unresolved`
- `step_attempts`: `kind text NOT NULL DEFAULT 'send'` (`send` or `poll`)
- `outbox` (§5.2)
- `recon_runs`, `recon_discrepancies` (§6)
- `admin_actions.action`, if it is constrained: the new outbox actions

The columns `callback_deadline` and status `AWAITING_CALLBACK` already exist from M1.

## 8. Failure modes (additions to §12)

| ID | Failure | Mechanism | Expected | Injected by |
|---|---|---|---|---|
| F15 | Duplicate provider webhook | Inbox primary key | Applied once | `dup_webhook`; exercised in chaos (2.3), asserted by I10/I11 (2.3b) |
| F16 | Webhook before our `AWAITING_CALLBACK` commit | Row stays pending, retried | Applied once the step is ready | `fast_webhook`; exercised in chaos (2.3), asserted by I10/I11 (2.3b) |
| F17 | Contradictory / reordered webhook | Never flip; `CONFLICT` + alert | Loud, not wrong | `reorder`; exercised in chaos (2.3), asserted by I10/I11 (2.3b) |
| F18 | Webhook never arrives | Deadline → worker polls | Resolved by polling; bounded, then `callback_unresolved` | `drop_webhook`; exercised in chaos (2.3) |
| F19 | Forged or replayed webhook | Signature + timestamp | `400`, nothing stored | `bad_signature`, unit tests; exercised in chaos (2.3), asserted by I10/I11 (2.3b) |
| F28 | Crash after storing a webhook, before `200` | Provider retries; dedup | Applied once | failpoint `webhook_after_insert`; exercised in chaos (2.3) |
| F29 | Crash while applying a webhook | Transaction rolls back; row still pending | Applied once later | failpoint `inbox_before_commit`, kills; exercised in chaos (2.3) |
| F30 | Webhook and poll race | Same CAS apply | One applies, the other no-ops | `delay_webhook` + short deadline; exercised in chaos (2.3), asserted by I10/I11 (2.3b) |
| F31 | Shop down or slow | Relay backoff; `DEAD` after the budget | Delivered when it recovers; loud otherwise | fake shop `down`, `slow`, `5xx`; relay unit tests (refused connection, budget); chaos since 2.5 |
| F32 | Relay crash after the shop's `2xx`, before marking delivered | At least once + event ID | The shop sees it twice, processes it once | failpoint `relay_after_send`, `ack_after_timeout`, `ack_then_5xx`; chaos requires observed redeliveries since 2.5 |
| F33 | Outcome committed but the relay never runs | Outbox row durable | Delivered when a relay starts | 2.4: no relay exists; chaos kills workers at `before_commit`/`after_commit` and I8 finds every event, once. 2.5: kill all relays; 2.5: chaos event `kill_all_relays` |
| F34 | Provider record without our posting (or the reverse) | Reconciliation | Reported, nothing changed | inject by test (direct provider call) |

## 9. Invariants (additions to §13)

| ID | Invariant | Checked against |
|---|---|---|
| **I4** (extended; chaos since 2.3) | After quiescence: no step `AWAITING_CALLBACK`, no inbox row `PENDING`, no outbox row `PENDING` | DB |
| **I8** (checked by every chaos run and `idem check` since 2.4) | Every terminal workflow has exactly one outbox event, of the matching type, whose payload is its view; no non-terminal workflow has one | DB |
| **I9** (checked by every chaos run since 2.5) | The fake shop's deduplicated view equals idem's terminal workflows: same set, same outcomes. No event the shop saw contradicts the database | Fake shop `/_seen` × DB |
| **I10** (checked by every chaos run since 2.3; tightened in 2.3b) | After quiescence, checked event by event against the fake provider's delivery log: a true event ends `PROCESSED`; a stale (reordered) one ends `IGNORED` as stale, or `PROCESSED` when it reached a payout still waiting (applied with the provider's re-checked state, never its own claim; I3 and I7 judge the money); either may be `EVIDENCE` only on a workflow in `NEEDS_ATTENTION`. Orphan, malformed, unsupported or mismatch notes, and a true event dismissed as stale, are violations: the honest fake provider never causes them. `CONFLICT` appears only if the provider's state truly contradicts idem's, which the fake provider never does, so chaos expects none. | DB × fake provider delivery log (`/_webhooks`) |
| **I11** (2.3b) | Every event the provider saw acknowledged (2xx) is in the inbox; no event was accepted on a wrongly signed delivery alone; the inbox holds no event the provider never sent. Chaos also requires that webhooks completed payouts and (runs ≥ 1 min) that the provider delivered reordered events, because polling completes every payout even when webhooks are broken. | DB × fake provider delivery log |
| **Recon** | `idem reconcile` over the campaign window: zero discrepancies on non-parked workflows; parked workflows' discrepancies match the oracle | Reconciliation tables × oracle |

## 10. Delivery slices

Each slice ends like M1's: tests, an independent review, fixes, a PR and the docs (design doc, runbook, README). Each slice's failure work ships *with* it.

| Slice | Delivers | Failure work in the same slice |
|---|---|---|
| **2.1** Async payouts + polling | Fake provider `202` + settlement + `GET /payouts/{id}`; `ACCEPTED_ASYNC` → `AWAITING_CALLBACK`; worker polls past the deadline; `callback_unresolved`; I4 extended | F18 (no webhooks exist yet: polling alone must complete every payout), pivot rule on the async outcome |
| **2.2** Provider webhooks | Signed webhooks from the fake provider with its retries; `/webhooks/provider`; inbox; worker inbox loop; conflicts; metrics, alerts, runbook | F15, F16, F17, F19, F28, F29, F30; I10 |
| **2.3** Async chaos | Webhook faults and the new failpoints in chaos campaigns; in runs of a minute or more, the plan reserves a block of six back-to-back 8s windows (5s plus slack; one webhook fault each, once traffic flows) and the verdict requires every webhook fault to have had a real window and fired, and every armed failpoint to have fired. **Two API replicas** (owner's decision, 2026-11-10): misbehaving clients and provider webhooks are spread across both, and API restarts hit one replica at a time | All of A at process level; I6 (one workflow per key) and webhook dedup proven across replicas, since the API is stateless and Postgres enforces both |
| **2.4** Status + outbox | `GET /workflows/{id}`; outbox written in `transitionWorkflow`; I8 | F33 (rows survive with no relay) |
| **2.5** Relay + fake shop | `idem relay`, signing, backoff, `DEAD`; admin outbox commands; fake shop with faults; chaos with the shop; README "Receiving webhooks" | F31, F32; I9 |
| **2.6** Reconciliation | Provider balance-transactions report; `idem reconcile`; tables, metrics, alerts, admin, runbook; chaos requires zero discrepancies | F34; Recon invariant |

Slice 2.1 deliberately ships polling **before** webhooks: the system is already correct without them, and webhooks only make it faster. That is also the cleanest way to explain the design.

## 11. Left to the builder

- Exact column types, index definitions, metric bucket choices, default timings beyond those given, and log fields follow M1 conventions.
- How the worker splits time between the step loop and the inbox loop (one goroutine each is the obvious choice).

## 12. Changes to `docs/design.md` when M2 lands

- §9.1 and §11.3 say M1 writes outbox rows; as built it does not. M2 creates the outbox (§5.2).
- §9.5 and §11.1: polling moves from the sweeper to the workers (§4.5).
- §15: the M2 outline is replaced by a pointer to this document; generic workflow definitions move to M4.
- New decisions for §16:
  - **D12:** polling by workers, not the sweeper
  - **D13:** the outbox is written inside `transitionWorkflow`
  - **D14:** at-least-once outbound delivery with a stable event ID
  - **D15:** reconciliation only reports
  - **D16:** contradictory webhooks never flip a terminal state; they are re-checked against the provider, and only a real disagreement pages
  - **D17:** polls are attempts of kind `poll`, never doubt
  - **D18:** late provider news for a parked step is evidence for a human, never an automatic resolution
  - **D19:** outbox rows are written for every client; without a webhook URL they are marked `NO_ENDPOINT`, not delivered and not alerted
