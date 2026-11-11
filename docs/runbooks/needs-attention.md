# Runbook: workflows that need attention

idem parks a workflow in `NEEDS_ATTENTION` when it cannot finish it **without guessing about money**. A parked workflow is not an outage. It is the system refusing to do something that might be wrong (design principle: *stuck and loud beats wrong and quiet*). Your job is to find out what actually happened at the payment provider, then record that with one audited `idem admin` command.

**Never edit idem's tables by hand.** Every admin command checks the workflow is still parked, keeps the ledger and the saga consistent, and writes an `admin_actions` audit row. A hand-written `UPDATE` does none of that.

## 1. Triage

```bash
export IDEM_DATABASE_URL=postgres://…    # compose: "postgres://idem:${POSTGRES_PASSWORD}@localhost:5432/idem?sslmode=disable" (password in .env)
export IDEM_ADMIN_ACTOR=your.name        # recorded in the audit log (defaults to $USER)

idem admin list                          # every parked workflow, its version, step and reason
idem admin list --reason attempts_exhausted_in_doubt
idem admin list --json                   # with amounts, customer and merchant
```

| Column | Meaning |
|---|---|
| `VERSION` | what you decide on: pass it to any action as `--expect-version`. It changes whenever the workflow leaves `NEEDS_ATTENTION` and comes back |
| `STEP` | the step that parked the workflow: `charge`, `payout` or `refund` |
| `STATUS` | `DEAD` (gave up) or `FAILED_PERMANENT` (a refund the provider refused) |
| `REASON` | the `dead_reason`; for a refused refund, `refused`. The sections below are named after these reasons |
| `ATTEMPTS` | how many times it was sent |
| `POLLS` | how many times idem asked the provider about an accepted payout |
| `PROVIDER SAYS` | the provider's latest news about this step, received by webhook and re-checked with the provider (e.g. `provider says paid po_…`). **Use it**: it is usually the answer you would otherwise look up |
| `LAST ERROR` | the last error the worker saw |

Useful context for any workflow:

```sql
-- every attempt of every step, oldest first
SELECT s.name, s.generation, a.attempt_no, a.outcome, a.http_status, a.error, a.started_at
FROM step_attempts a JOIN steps s ON s.id = a.step_id
WHERE s.workflow_id = '<workflow-id>' ORDER BY a.started_at;

-- the idempotency keys idem sent (look these up at the provider)
SELECT name, generation, status, idempotency_key, provider_ref FROM steps WHERE workflow_id = '<workflow-id>';

-- what humans already did
SELECT at, actor, action, reason, details FROM admin_actions WHERE workflow_id = '<workflow-id>' ORDER BY at;
```

Worker logs carry `workflow_id`, `step_id`, `attempt` and `lease_epoch` on every line. The line `step is DEAD; workflow needs attention` has `dead_reason` and `money_may_have_moved`.

**Looking up a key at the provider.** Every request idem sends carries the step's `idempotency_key`. Ask the provider what happened to that key: in the fake provider, `GET /charges?idempotency_key=<key>` (likewise `/payouts` and `/refunds`); in a real provider, its idempotency or request-log lookup.

## 2. The actions

Every command below also takes **`--expect-version <VERSION>`** from `idem admin list` (omitted from the table for brevity). It ties your decision to the facts you checked. If someone else acted in between, the command changes nothing and exits `1`: it is refused as **not actionable** while their action is still under way, and as **stale** once the workflow has been parked again (for example after a retry that charged the customer). Either way: list again, re-check with the provider, and decide again.

| Command | Use it when | What it does |
|---|---|---|
| `idem admin retry-step <step> --reason …` | The cause was temporary. | Retries with the **same** key, which is always safe because the provider deduplicates. A step that may have moved money stays in doubt. |
| `idem admin confirm-step <step> --provider-ref <ref> --reason …` | You verified the money **did** move. | Marks the step succeeded, posts it to the ledger, and moves the saga on (e.g. after a charge, the payout runs). The reference must not already be recorded on another step. |
| `idem admin compensate <workflow> --reason … [--confirm-no-effect]` | Parked **payout**, and you verified the merchant was **not** paid. | Refunds the customer in full; the workflow ends `COMPENSATED`. |
| `idem admin fail <workflow> --reason … [--confirm-no-effect]` | Parked **charge**, and you verified the customer was **not** charged. | Closes the workflow as `FAILED`. |
| `idem admin new-generation <step> --reason … --confirm-no-effect` | You verified the old key moved **no** money and want to send the request again. | **Dangerous:** sends under a **new** key, which to the provider is a brand-new payment. The old step stays as history. |

Every command refuses to act on a workflow that isn't parked, on a step that isn't the parked one, or on a version that is no longer current. It also refuses any action that doesn't fit the step: `compensate` only applies to a payout, `fail` only to a charge. When refused, nothing changes and the exit code is `1`. A usage mistake (missing flag, bad ID, unknown command) exits `2`.

**`--confirm-no-effect` is required** on `compensate` and `fail` whenever the parked step **may have moved money**: its reason is anything but `attempts_exhausted_no_effect`, or any attempt was ambiguous. Without the flag the command exits `2` and changes nothing.
- **Why:** compensating a payout that actually paid the merchant pays out twice, and failing a charge that actually charged the customer loses their money.
- **When to pass it:** only after you have checked with the provider.

**`confirm-step` accepts:**
- a `DEAD` step
- a refused (`FAILED_PERMANENT`) step that had an ambiguous attempt (an earlier attempt may have succeeded)
- a refused refund (the customer may have been refunded outside idem)

## 3. By reason

### attempts_exhausted_no_effect

Every attempt was a definite "no effect" (rate limited or unreachable), so **no money moved**. The provider was overloaded or down for longer than the retries lasted.

1. Confirm the provider is healthy again (provider status, `idem_provider_request_duration_seconds`, worker logs).
2. Run `idem admin retry-step <step> --reason "provider recovered"`.
3. If you would rather give up on a parked *charge*, run `idem admin fail <workflow> --reason …`.

### attempts_exhausted_in_doubt

At least one attempt was ambiguous (timeout, `5xx`, a crashed worker), so **money may have moved**.

1. Look up the step's idempotency key at the provider.
2. **The provider shows a success:** run `idem admin confirm-step <step> --provider-ref <id from the provider> --reason …`.
3. **The provider has no record or shows a refusal:**
   - if the provider is healthy again, run `retry-step`; it's safe, because the same key cannot double-charge
   - otherwise, for a charge run `fail --confirm-no-effect`, and for a payout run `compensate --confirm-no-effect`

### key_expired_unknown

The step stayed in doubt so long that its key is near or past the provider's key retention, and the provider no longer knows it. idem will **never** retry under a new key on its own, because a forgotten key plus a new key could mean paying twice.

1. Search the provider's records for this payment *without* the key: by workflow ID in the request metadata, amount, customer or merchant, and time.
2. **Found:** run `confirm-step` with that reference.
3. **Verified not found:**
   - for a charge or payout: `new-generation --confirm-no-effect` to send again, or `fail --confirm-no-effect` (charge) / `compensate --confirm-no-effect` (payout) to stop
   - for a refund: `new-generation --confirm-no-effect`

### permanent_after_in_doubt

The **pivot safety rule**. A payout was refused (e.g. `422 invalid_account`) *after* an earlier attempt was ambiguous. A provider that didn't replay its first answer could have paid the merchant on that earlier attempt. Refunding the customer automatically might then pay out twice, so idem parks instead.

1. Ask the provider whether **any** payout for this workflow succeeded (search by workflow ID, amount and merchant, not just this key).
2. **Paid:** run `idem admin confirm-step <step> --provider-ref <po_…> --reason …`. The workflow ends `SUCCEEDED`.
3. **Not paid, and the merchant account is invalid:** run `idem admin compensate <workflow> --confirm-no-effect --reason "merchant account invalid; no payout made"`. The workflow ends `COMPENSATED`.
4. **Not paid, and the merchant fixed their account:** run `new-generation --confirm-no-effect` on the payout step.

In chaos runs these are the *expected* parkings. The fake provider always replays, so a human would find "not paid" and compensate.

### callback_unresolved

The provider **accepted** the payout (`202`, a `po_…` reference), but it was still `pending`, or could not be polled, for `IDEM_CALLBACK_MAX_WAIT` (default 72h) since acceptance, or after `IDEM_CALLBACK_MAX_POLLS` polls if that limit is set. **Money may have moved**, or may still move. Worker logs show each poll (`waiting for the provider's result`, with `poll_count` and `waited`).

1. Look up the step's `provider_ref` (`po_…`) at the provider (fake provider: `GET /payouts/<po_…>`).
2. **Paid:** run `idem admin confirm-step <step> --provider-ref <po_…> --expect-version <V> --reason …`. Use the step's **own** reference (`provider_ref`): the same key always returns the same payout, so any other reference is refused as a likely typo. The workflow ends `SUCCEEDED`.
3. **Failed:** run `idem admin compensate <workflow> --expect-version <V> --confirm-no-effect --reason …`. The workflow ends `COMPENSATED`.
4. **Still pending** (the provider is slow): run `idem admin retry-step <step> --expect-version <V> --reason …`. The same key is re-sent, the provider replays the same payout, and polling resumes. Never use `new-generation` while the old payout can still settle: that could pay twice.

### hook_failed

The provider **answered**, but the step's own bookkeeping kept failing: the ledger posting or the saga move that runs in the completion transaction. Every attempt rolled back. After `IDEM_HOOK_MAX_FAILURES` failures (default 3) the step is parked rather than retried forever. **The provider DID act**: if it reported success, money moved and **nothing is posted yet**. This is a bug in idem (or bad data), not a provider problem. `IdemMoneyMayHaveMoved` pages.

1. Read the error: `idem admin list` shows `last_error`, and the worker logs `step hook failed` with the error for each failure.
2. Fix the cause (deploy the fix, or correct the data), then hand the step back:
   - `idem admin retry-step <step> --expect-version <V> --reason …`. The same key is re-sent, the provider replays its answer, and the hook runs again. `retry-step` resets the failure count.
   - Or, if you have confirmed the outcome with the provider, `idem admin confirm-step <step> --provider-ref <ref> --expect-version <V> --reason …`, which runs the same success hook.
3. Never use `new-generation`: the provider already acted on this key, so a new key could act twice.

**`hook_failed_no_effect`** is the same failure after the provider **refused** the request (and no earlier attempt was in doubt): nothing moved and nothing is posted, so it does not page `IdemMoneyMayHaveMoved`. Fix the cause, then `retry-step`: the provider replays its refusal and the now-working failure hook runs (for a payout, the customer is refunded).

### Provider news about a parked workflow (`PROVIDER SAYS`)

When a webhook arrives for a parked step, idem re-checks it with the provider and shows it in the `PROVIDER SAYS` column. It never resolves the workflow by itself (money decisions made under doubt end with a human).
- `provider says paid po_…` → `confirm-step <step> --provider-ref po_… --expect-version <V> --reason …`.
- `provider says failed: …` → for a payout, `compensate <workflow> --expect-version <V> --confirm-no-effect --reason …`.

### refused (refund `FAILED_PERMANENT`)

The customer was charged and the provider **refused** the refund (e.g. `422 invalid_refund`): wrong charge reference, amount above what is left, or the charge already refunded elsewhere.

1. Check the refund's `refund_charge_ref` against the charge's `provider_ref`, and the charge's refund history at the provider.
2. **A refund for this charge exists at the provider:** either an earlier ambiguous attempt succeeded, or support refunded the customer directly. Run `idem admin confirm-step <refund-step> --provider-ref <re_…> --reason …`. That posts the reversal and the workflow ends `COMPENSATED`. **Never send a second refund.**
3. **No refund exists, and the refusal was caused by a fixable provider-side problem:** run `new-generation --confirm-no-effect` on the refund step. The new refund always targets the workflow's current successful charge.

## 4. Alerts

Rules: [`deploy/prometheus/alerts.yml`](../../deploy/prometheus/alerts.yml). Compose serves Prometheus on <http://localhost:9091> (alerts at `/alerts`).

### IdemWorkflowsNeedAttention

`idem_parked_workflows > 0` for 1 minute. At least one workflow is parked. Follow §1 and §3. Severity: page, because a customer is waiting on money.

### IdemMoneyMayHaveMoved

A parked workflow's reason is anything other than `attempts_exhausted_no_effect`, so the provider may have moved money that idem's ledger hasn't recorded. Handle these first, and check the provider before doing anything else.

### IdemWorkflowsStuck

An unsettled workflow (not parked) has made **no progress** for 15 minutes: no status change and no *finished* step attempt. Age alone doesn't count, so a workflow retried hours after it was parked doesn't page. A claim alone doesn't count either, and neither does an attempt abandoned by a crash, so a step that crash-loops still pages. Usual causes:
- the workers are down, or crash-loop on this step (check process restarts and worker logs; a step that is claimed and abandoned again and again also shows in `IdemStepsOverAttemptLimit`)
- the database is unreachable

A slow provider does **not** show here, because its attempts still finish; that is `IdemProviderSlow`.

The sweeper also logs `stuck workflows count=… longest_idle=…` with the longest-idle workflow's ID (`longest_idle_workflow_id`).

### IdemStepsOverAttemptLimit

An unfinished step was claimed more than once past its attempt limit. (One extra attempt is normal after a crash on the final attempt.) Steps should go `DEAD` at their limit, so this means a bug, or a step being claimed but failing before its outcome can be recorded. Look at that step's `step_attempts` rows and the worker error logs.

### IdemFencedCommitsSpike

Workers keep losing their lease before completing. Their results are discarded safely, but the work is wasted. Usual causes: GC pauses or CPU starvation longer than `IDEM_LEASE_TTL`, a database too slow to commit, or `IDEM_PROVIDER_TIMEOUT` too close to the lease. Raise the lease TTL, or give the workers more resources.

### IdemSweeperDown

No sweeper has been scrape-able for 2 minutes. Consequences:
- leases of crashed or paused workers are **not reclaimed**, so their steps sit `RUNNING`
- **every state alert above is blind**, because they all read the sweeper's gauges

Restart the sweeper first, then re-check the other alerts.

### IdemSweeperSnapshotStale

A sweeper is up, but has not refreshed its state gauges for 2 minutes. Its snapshot queries are failing; check the sweeper logs for `metrics snapshot` errors, which usually mean the database is unreachable or slow. Until this clears, treat the parked, stuck and over-limit numbers as unreliable.

### IdemProviderSlow

The provider's p99 latency is above 2s. Expect more timeouts, and so more in-doubt steps. Check the provider's status; consider lowering concurrency or raising `IDEM_PROVIDER_TIMEOUT` (it must stay below the lease TTL minus 2s).

### IdemWebhookConflict

**Page.** The provider's current state contradicts what idem recorded, for example "paid" for a payout idem recorded as failed, or after an operator compensated. **Money may have moved both ways** (the merchant paid *and* the customer refunded). idem never flips a decided result and never stops a refund half way, so the fix happens outside idem.
1. `idem admin conflict list` shows the event, the workflow and what each side says.
2. Confirm at the provider (the payout's `po_…` and its history), and recover the money (e.g. claw back from the merchant, or re-charge with the customer's consent).
3. `idem admin conflict resolve <event-id> --reason "what was done"` records it (audited) and clears the alert.

### IdemInboxBacklog

Provider webhooks are stored but not being applied (oldest pending > 10 minutes). Usually the workers are down or failing; check worker logs for `inbox event` errors. Payouts are not stuck meanwhile: polling still settles them, just more slowly. An event that never matches a payout is ignored as an orphan after `IDEM_INBOX_ORPHAN_AFTER` (see `IdemWebhookOrphans`). Processed and ignored events are deleted after `IDEM_INBOX_RETENTION` (default 30 days); evidence and conflicts are kept.

An event whose re-check with the provider keeps failing is deferred, with backoff, as long as its payout waits. That wait is bounded: after `IDEM_CALLBACK_MAX_WAIT` (72h) the payout parks as `callback_unresolved`, and the event then becomes evidence on the parked workflow.

### IdemWebhookOrphans

Provider webhooks arrived for payouts idem has no record of, and stayed unmatched for `IDEM_INBOX_ORPHAN_AFTER` (default 1h). They are ignored (`webhook_inbox` status `IGNORED`, note `orphan`) and never applied. Possible causes:
- a provider bug, or events meant for another environment sharing the endpoint (check the signing secret)
- a payout created outside idem
- rarely, a payout idem sent but never recorded; reconciliation (slice 2.6) will report it as `missing_in_ledger`

Find them with `SELECT provider_event_id, received_at, payload FROM webhook_inbox WHERE note = 'orphan' ORDER BY received_at DESC`.

**Other events idem ignores, without alerting** (also counted in `idem_webhooks_applied_total{outcome}`):
- `stale`: a late or reordered event that the provider's current state contradicts; idem was right
- `unsupported event type …`: an event type idem does not act on
- `mismatch`: an event whose amount or currency differs from the payout it names; logged as a warning, never applied
- `malformed`: an event that is not a valid payout event

### IdemDBPoolSaturated

Processes of one kind (`job`) keep waiting for a free database connection: over 100ms of waiting per second for 10 minutes. Requests and steps slow down even though the database itself may be fine. Look at `idem_db_pool_acquired_conns` against `idem_db_pool_max_conns` (each process opens at most 20), and at the database's own load. Try, in order:
1. **Shorter transactions:** a slow query or a lock wait holds a connection longer (check Postgres for slow queries and locks).
2. **More connections per process:** only if Postgres has headroom (`max_connections`, default 100).
3. **Fewer or more processes:** fewer, if the database is the limit; more, if each process is starved but the database is idle.
4. **PgBouncer** (transaction pooling) in front of Postgres, when many replicas multiply connections beyond what Postgres allows.

### IdemCallbacksOverdue

Waiting payouts are more than 5 minutes past their next poll: the workers are not keeping up, or are down. Webhooks may still settle them; if not, the stuck alert follows.

### IdemWebhookSignatureFailures

Provider webhooks keep failing verification (`bad_signature` or `stale`). Likely causes: the provider's secret changed and idem's did not, a server clock far off (5-minute tolerance), or someone forging requests. Nothing that fails verification is stored or acted on.

**Rotating the webhook secret without losing events:**
1. Set `IDEM_PROVIDER_WEBHOOK_SECRETS=<new>,<old>` on the API and restart it: both verify.
2. Switch the provider to the new secret.
3. When the failures stay at zero, set `IDEM_PROVIDER_WEBHOOK_SECRETS=<new>`.

### IdemOutboxBacklog

A workflow outcome has waited more than 15 minutes to reach its shop. The outcome itself is committed and safe: the event waits, durably, in the outbox. Find out which shop:

```bash
idem admin outbox list        # CLIENT, ATTEMPTS and LAST ERROR of every event not delivered
```

- **One client's events pile up, with errors** (`shop answered 503`, timeouts, refused connections): that shop is down or slow. The relay keeps retrying with backoff; nothing to do unless it stays down long enough for `IdemOutboxDead`. Tell the shop.
- **Every client's events pile up, with no errors and attempts at 0:** no relay is running. Check the `relay` processes (`idem_relay_deliveries_total` stops moving).
- **Several shops' events pile up behind one slow shop:** each relay delivers `IDEM_RELAY_CONCURRENCY` events at once, at most `IDEM_RELAY_PER_CLIENT` for one client, so a shop that hangs holds only its share. If healthy shops still wait, run more relays or raise `IDEM_RELAY_CONCURRENCY`.
- **A client you expected to be delivered to shows `NO_ENDPOINT`:** its ID in `IDEM_CLIENT_WEBHOOKS` does not match (the relay logs `configured clients with no workflow` at startup for a likely typo). Fix the variable, restart the relays, then `idem admin outbox redeliver-all --client <id> --status NO_ENDPOINT --reason "…"`.
- **Errors like `shop answered 401` or `400`:** the shop rejects the signature or the body. Check that both sides have the same secret (`IDEM_CLIENT_WEBHOOK_SECRETS`) and that the shop's clock is within 5 minutes.

Events of clients without a webhook URL are `NO_ENDPOINT`, never `PENDING`, so they never cause this alert.

### IdemOutboxDead

The relay gave up on an event after `IDEM_RELAY_MAX_ATTEMPTS` deliveries (default 15, about 16 hours on average). The shop never confirmed the outcome. Once the shop is back:

```bash
idem admin outbox list --dead
idem admin outbox redeliver <event-id> --reason "shop back after outage" --expect-version <VERSION from the list>
# many events of one client (after a long outage, or once a client has a URL):
idem admin outbox redeliver-all --client <id> --status DEAD --reason "shop back after outage"
```

The event gets a fresh budget and the relay sends it at once, with the same `Idem-Event-Id` (the shop ignores it if it had it after all). `redeliver` also sends:
- a `NO_ENDPOINT` event, after the client was given a URL;
- a `SKIPPED` event: an outcome from before the outbox existed, not announced at upgrade. Send one only if its shop asks;
- a `DELIVERED` event, if the shop lost it.

### IdemReconciliationDrift

The latest reconciliation found differences between the provider's record of money movements and idem's ledger, on workflows that are **not** parked. This is the alert that says the books disagree. Nothing was changed automatically (D15). Start with:

```bash
idem admin recon list         # WORKFLOW, KIND, TYPE, PROVIDER and LEDGER amounts, DETAIL
```

By kind:
- **`missing_in_ledger`**: the provider moved money idem never recorded. If the workflow is unknown to idem (no workflow ID, or one idem has no row for), the money moved outside idem: find who made the request (the provider's idempotency key and metadata). If idem knows the workflow, check its steps and the provider's object: the effect happened, so the ledger must get it, through `confirm-step` once the workflow is parked, never by hand.
- **`missing_at_provider`**: idem posted a movement the provider has no record of. Check the step's `provider_ref` at the provider; a provider-side reversal or a lost record needs the provider's support. Correct the ledger only with a new reversing transaction.
- **`amount_mismatch`**: both sides recorded the movement with different amounts. Compare the request idem sent (the step's input and fee) with the provider's object.
- **`duplicate_effect`**: two charges, payouts or refunds for one workflow. The customer or the merchant got money twice; recover it outside idem (refund the extra charge, claw back the extra payout) and record why.
- **`status_mismatch`**: money moved for a step idem recorded as definitely refused. The provider said no and then did it anyway (or a later attempt did): treat it as `missing_in_ledger` on a workflow whose saga already moved on, and decide the recovery with finance.

Differences on **parked** workflows do not page here (the parked alert already does); they appear next to the workflow in `idem admin list` (column `RECON`) as evidence for the operator's choice.

### IdemReconciliationStale

No reconciliation has succeeded for over 3 hours, so drift would go unnoticed. Check the `reconcile` process (compose: the `reconcile` service) and its logs: a run that fails (for example, the provider's report unreachable) is recorded `FAILED` with its error in `recon_runs` and reports nothing.

## 5. Worked example (compose)

```bash
make up
set -a; . ./.env; set +a     # IDEM_DEMO_TOKEN and the other local settings
# a payout that is refused after an ambiguous first attempt
curl -s -X PUT localhost:9090/_faults -d '{"rules":[{"endpoint":"payouts","mode":"error_before_effect","times":1}]}'
curl -s -X POST localhost:8080/workflows -H "Authorization: Bearer $IDEM_DEMO_TOKEN" \
  -H 'Idempotency-Key: rb-1' -d '{"amount_cents":10000,"currency":"EUR","customer_id":"cus_1","merchant_id":"mer_invalid_1"}'

docker compose run --rm --entrypoint /idem worker admin list
# WORKFLOW  VERSION  STEP    STATUS  REASON                    ATTEMPTS  POLLS  PARKED FOR  LAST ERROR
# 01a1…     3        payout  DEAD    permanent_after_in_doubt  2         0      12s         provider refused request: 422 invalid_account

curl -s localhost:9091/api/v1/alerts            # IdemWorkflowsNeedAttention, IdemMoneyMayHaveMoved firing

# the provider confirms no payout was made → refund the customer
docker compose run --rm -e IDEM_ADMIN_ACTOR=oncall --entrypoint /idem worker \
  admin compensate <workflow-id> --expect-version 3 --confirm-no-effect \
  --reason "provider: no payout for this workflow; merchant account invalid"

docker compose run --rm --entrypoint /idem worker check   # 0 violations, 0 need attention
```
