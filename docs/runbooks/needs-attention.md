# Runbook: workflows that need attention

idem parks a workflow in `NEEDS_ATTENTION` when it cannot finish it **without guessing about money**. A parked workflow is not an outage. It is the system refusing to do something that might be wrong (design principle: *stuck and loud beats wrong and quiet*). Your job is to find out what actually happened at the payment provider, then record that with one audited `idem admin` command.

**Never edit idem's tables by hand.** Every admin command checks the workflow is still parked, keeps the ledger and the saga consistent, and writes an `admin_actions` audit row. A hand-written `UPDATE` does none of that.

## 1. Triage

```bash
export IDEM_DATABASE_URL=postgres://…    # compose: postgres://idem:idem@localhost:5432/idem?sslmode=disable
export IDEM_ADMIN_ACTOR=your.name        # recorded in the audit log (defaults to $USER)

idem admin list                          # every parked workflow, its step and reason
idem admin list --reason attempts_exhausted_in_doubt
idem admin list --json                   # with amounts, customer and merchant
```

| Column | Meaning |
|---|---|
| `STEP` | the step that parked the workflow: `charge`, `payout` or `refund` |
| `STATUS` | `DEAD` (gave up) or `FAILED_PERMANENT` (a refund the provider refused) |
| `REASON` | the `dead_reason`; for a refused refund, `refused`. The sections below are named after these reasons |
| `ATTEMPTS` | how many times it was tried |
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

| Command | Use it when | What it does |
|---|---|---|
| `idem admin retry-step <step> --reason …` | The cause was temporary. | Retries with the **same** key, which is always safe because the provider deduplicates. A step that may have moved money stays in doubt. |
| `idem admin confirm-step <step> --provider-ref <ref> --reason …` | You verified the money **did** move. | Marks the step succeeded, posts it to the ledger, and moves the saga on (e.g. after a charge, the payout runs). |
| `idem admin compensate <workflow> --reason …` | Parked **payout**, and you verified the merchant was **not** paid. | Refunds the customer in full; the workflow ends `COMPENSATED`. |
| `idem admin fail <workflow> --reason …` | Parked **charge**, and you verified the customer was **not** charged. | Closes the workflow as `FAILED`. |
| `idem admin new-generation <step> --reason … --confirm-no-effect` | You verified the old key moved **no** money and want to send the request again. | **Dangerous:** sends under a **new** key, which to the provider is a brand-new payment. The old step stays as history. |

Every command refuses to act on a workflow that isn't parked, or on a step that isn't the parked one. It also refuses any action that doesn't fit the step: `compensate` only applies to a payout, `fail` only to a charge, `confirm-step` only to a `DEAD` step. When refused, nothing changes and the exit code is `1`.

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
   - otherwise, for a charge run `fail`, and for a payout run `compensate`

### key_expired_unknown

The step stayed in doubt so long that its key is near or past the provider's key retention, and the provider no longer knows it. idem will **never** retry under a new key on its own, because a forgotten key plus a new key could mean paying twice.

1. Search the provider's records for this payment *without* the key: by workflow ID in the request metadata, amount, customer or merchant, and time.
2. **Found:** run `confirm-step` with that reference.
3. **Verified not found:**
   - for a charge or payout: `new-generation --confirm-no-effect` to send again, or `fail` (charge) / `compensate` (payout) to stop
   - for a refund: `new-generation --confirm-no-effect`

### permanent_after_in_doubt

The **pivot safety rule**. A payout was refused (e.g. `422 invalid_account`) *after* an earlier attempt was ambiguous. A provider that didn't replay its first answer could have paid the merchant on that earlier attempt. Refunding the customer automatically might then pay out twice, so idem parks instead.

1. Ask the provider whether **any** payout for this workflow succeeded (search by workflow ID, amount and merchant, not just this key).
2. **Paid:** run `idem admin confirm-step <step> --provider-ref <po_…> --reason …`. The workflow ends `SUCCEEDED`.
3. **Not paid, and the merchant account is invalid:** run `idem admin compensate <workflow> --reason "merchant account invalid; no payout made"`. The workflow ends `COMPENSATED`.
4. **Not paid, and the merchant fixed their account:** run `new-generation --confirm-no-effect` on the payout step.

In chaos runs these are the *expected* parkings. The fake provider always replays, so a human would find "not paid" and compensate.

### refused (refund `FAILED_PERMANENT`)

The customer was charged and the provider **refused** the refund (e.g. `422 invalid_refund`): wrong charge reference, amount above what is left, or the charge already refunded elsewhere.

1. Check the refund's `refund_charge_ref` against the charge's `provider_ref`, and the charge's refund history at the provider.
2. **Already refunded elsewhere (e.g. by support):** record it outside idem and escalate. A second refund must not be sent.
3. **The refusal was caused by a fixable provider-side problem:** run `new-generation --confirm-no-effect` on the refund step.

## 4. Alerts

Rules: [`deploy/prometheus/alerts.yml`](../../deploy/prometheus/alerts.yml). Compose serves Prometheus on <http://localhost:9091> (alerts at `/alerts`).

### IdemWorkflowsNeedAttention

`idem_parked_workflows > 0` for 1 minute. At least one workflow is parked. Follow §1 and §3. Severity: page, because a customer is waiting on money.

### IdemMoneyMayHaveMoved

A parked workflow's reason is anything other than `attempts_exhausted_no_effect`, so the provider may have moved money that idem's ledger hasn't recorded. Handle these first, and check the provider before doing anything else.

### IdemWorkflowsStuck

The oldest unsettled workflow is older than 15 minutes while **not** parked. Usual causes:
- the workers are down or crash-looping (check process restarts and worker logs)
- the database is unreachable
- a provider is slow enough that backoff keeps pushing retries out

The sweeper also logs `stuck workflows count=… oldest=…` with the oldest workflow's ID.

### IdemStepsOverAttemptLimit

A step has more attempts than its limit. Steps should go `DEAD` at their limit, so this means a bug, or a step being claimed but failing before its outcome can be recorded. Look at that step's `step_attempts` rows and the worker error logs.

### IdemFencedCommitsSpike

Workers keep losing their lease before completing. Their results are discarded safely, but the work is wasted. Usual causes: GC pauses or CPU starvation longer than `IDEM_LEASE_TTL`, a database too slow to commit, or `IDEM_PROVIDER_TIMEOUT` too close to the lease. Raise the lease TTL, or give the workers more resources.

### IdemProviderSlow

The provider's p99 latency is above 2s. Expect more timeouts, and so more in-doubt steps. Check the provider's status; consider lowering concurrency or raising `IDEM_PROVIDER_TIMEOUT` (it must stay below the lease TTL minus 2s).

## 5. Worked example (compose)

```bash
make up
# a payout that is refused after an ambiguous first attempt
curl -s -X PUT localhost:9090/_faults -d '{"rules":[{"endpoint":"payouts","mode":"error_before_effect","times":1}]}'
curl -s -X POST localhost:8080/workflows -H 'Authorization: Bearer demo-token-0123456789' \
  -H 'Idempotency-Key: rb-1' -d '{"amount_cents":10000,"currency":"EUR","customer_id":"cus_1","merchant_id":"mer_invalid_1"}'

docker compose run --rm --entrypoint /idem worker admin list
# WORKFLOW  STEP    STATUS  REASON                    ATTEMPTS  PARKED FOR  LAST ERROR
# 01a1…     payout  DEAD    permanent_after_in_doubt  2         12s         provider refused request: 422 invalid_account

curl -s localhost:9091/api/v1/alerts            # IdemWorkflowsNeedAttention, IdemMoneyMayHaveMoved firing

# the provider confirms no payout was made → refund the customer
docker compose run --rm -e IDEM_ADMIN_ACTOR=oncall --entrypoint /idem worker \
  admin compensate <workflow-id> --reason "provider: no payout for this workflow; merchant account invalid"

docker compose run --rm --entrypoint /idem worker check   # 0 violations, 0 need attention
```
