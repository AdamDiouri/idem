# idem — Payments Workflow Engine — Design Doc (Milestone 1)

| | |
|---|---|
| **Status** | v1 — approved; all open questions resolved |
| **Date** | 2026-11-09 |
| **Scope** | Milestone 1: single-region, Go + Postgres engine, fake payment provider, fault-injection harness |
| **Name** | `idem` — Latin "the same", root of *idempotent* |

---

## 1. Summary

A small, durable workflow engine for one payment flow: **charge a customer → record it in a ledger → pay out a merchant**. Every step that touches money is designed around the question *"what if we crash or time out right here?"*.

The engine guarantees **effectively-once** outcomes: work may be *attempted* more than once, but its *effects* — money moved at the provider, entries in our ledger — happen at most once, and every workflow ends in a known, named state.

The project's real deliverable is not the happy path. It is the **failure-mode table** (§12), the **invariants** (§13), and the **fault-injection harness** (§14) that proves the guarantees hold while workers are killed, paused, and lied to by the network.

---

## 2. Background: why retries are dangerous with money

A worker sends `POST /payouts` to a payment provider and the request times out. Two possibilities, and the worker **cannot tell which one happened**:

1. The request never reached the provider → no money moved → we *must* retry.
2. The provider paid the merchant, but the response was lost → money moved → retrying naively **pays the merchant twice**.

No lock, queue, or database can resolve that uncertainty on our side — it's a property of networks. "Exactly-once execution" across a network boundary is therefore impossible. What *is* achievable:

> **at-least-once attempts + idempotent effects = effectively-once outcomes**

The provider deduplicates requests carrying the same **idempotency key**, so we are free to retry as long as we always send the same key for the same intended effect. Inside our own database we get true exactly-once for free, because a single transaction is atomic.

---

## 3. Goals, non-goals, anti-goals

### Goals (M1)
- **G1.** Run the `payment_payout` workflow (charge → payout, with ledger postings and refund compensation) to completion despite crashes, pauses, timeouts, and duplicate messages.
- **G2.** Accept workflow submissions idempotently: a client retrying `POST /workflows` never creates a second workflow.
- **G3.** Maintain a double-entry, append-only ledger (EUR, with platform fees) that is always balanced and always agrees with the provider.
- **G4.** Provide a fault-injection harness and an invariant checker that can run thousands of chaotic workflows and report violations.
- **G5.** Expose enough metrics and structured logs to answer "what is stuck, and why?" without reading the database by hand.

### Non-goals (M1)
- Kubernetes operator / CRDs (Milestone 4).
- A generic workflow definition language. M1 has one hard-coded workflow type behind a small interface.
- **Asynchronous payouts and provider webhooks (M2).** In M1 the provider answers payouts synchronously. The schema and state machine leave room for M2 (§9.5).
- Currencies other than EUR, FX, multiple providers.
- Sharding, multi-region, Postgres HA/failover.
- Publishing outbox events to an external broker (rows are written in M1; the relay is M2).
- Admin UI. Manual intervention is via a small CLI / documented SQL.
- Authentication beyond a static API token.

### Anti-goals (would make the project a failure even if it "works")
- **Claiming "exactly-once"** anywhere in code, docs, or talks.
- **Happy-path first, failure tests later.** Failure handling ships *with* each feature, not after.
- **Silent failure.** A workflow that quietly disappears or sits in a non-terminal state forever. *Stuck and loud beats wrong and quiet.*
- **Rebuilding Temporal.** Scope stays narrow: one domain, deeply correct.
- **Importing the hard parts.** The queue, leases, fencing, retries, and recovery are hand-built — that's the learning.

---

## 4. Guarantees and assumptions

### Guarantees
| ID | Guarantee | Mechanism |
|---|---|---|
| **GR1** | Each intended external effect (charge, payout, refund) happens **at most once** at the provider. | One stable idempotency key per intended effect, stored at step creation and never regenerated automatically (§9.2). |
| **GR2** | Each ledger posting is written **exactly once**. | Written in the same Postgres transaction as the step's success transition, guarded by a unique constraint (§10). |
| **GR3** | The ledger is **always balanced** (per transaction and globally). | Signed amounts + deferred constraint trigger (§10). |
| **GR4** | Every workflow reaches a **terminal or attention state** within bounded time, once the database is available and faults stop. | Leases, sweeper, retry limits, callback deadlines (§8, §11). |
| **GR5** | A payout is never requested unless the charge succeeded. | Steps are created sequentially by the previous step's success transaction. |

### Assumptions (if these break, the guarantees above may not hold)
| ID | Assumption |
|---|---|
| **A1** | The provider honours idempotency keys for at least `provider_key_retention` (configurable; Stripe: 24h). |
| **A2** | The provider supports looking up a request by idempotency key (used when a key is near or past retention). |
| **A3** | Postgres does not lose committed transactions (`synchronous_commit = on`, single primary). |
| **A4** | Lease times are read from the **database clock** only; worker clocks are never trusted for correctness. |
| **A5** | No component is malicious (crash/omission faults only — no Byzantine faults). |

---

## 5. Design principles — failure-first

1. **Failure is the normal path.** Every transition is designed by asking "what if the process dies right here?" between each write and each external call.
2. **Every failure lands in a named state** with an owner and an exit: `IN_DOUBT`, `AWAITING_CALLBACK`, `COMPENSATING`, `DEAD`, `NEEDS_ATTENTION`. No limbo.
3. **When unsure, assume the effect happened.** Ambiguous outcomes are `IN_DOUBT`, never "failed".
4. **Never mint a new idempotency key to "get unstuck".** A new key means a new payment. Only an explicit, audited human action can do that.
5. **Fencing protects our state; idempotency keys protect external effects.** You need both: fencing can't stop a paused worker's HTTP request, and idempotency keys can't stop a stale worker overwriting our rows.
6. **Durable before acknowledged.** Nothing (API request, webhook) is acknowledged with a 2xx until it is committed.
7. **The database is the arbiter.** Locks/leases reduce contention; correctness comes from transactions, unique constraints, and compare-and-swap updates.
8. **Prefer stuck and loud over wrong and quiet.** If facts conflict, stop and raise `NEEDS_ATTENTION` + an alert rather than guess.
9. **Every failure path has a test**, and most have a named failpoint (§14.2).

---

## 6. Fault model

What the system must tolerate in M1, and what it explicitly doesn't.

| Fault | Tolerated? | Notes |
|---|---|---|
| Worker crash at any point (`SIGKILL`) | ✅ | Lease expiry → sweeper → `IN_DOUBT`. |
| Worker pause longer than lease (GC, `SIGSTOP`, VM freeze) | ✅ | Fencing rejects its late commit; idempotency key makes its late HTTP call harmless. |
| Multiple workers / sweepers running concurrently | ✅ | `SKIP LOCKED` claims + CAS transitions. |
| Provider timeouts, 5xx, 429, connection resets | ✅ | Outcome classification (§9.3). |
| Provider performs effect, then response is lost | ✅ | The core case. Same key on retry → replayed response. |
| Webhooks duplicated, delayed, reordered, dropped | M2 | Inbox dedup, monotonic transitions, callback deadline + polling. |
| Forged / replayed webhooks | M2 | HMAC signature + timestamp tolerance. |
| Client retries / duplicate API submissions | ✅ | Inbound idempotency keys. |
| Postgres temporarily unavailable | ✅ | Workers back off; no provider call without a committed claim. |
| Clock skew between worker hosts | ✅ | All lease math uses DB `now()`. |
| Bug/panic inside a step handler | ✅ | Recovered, counted as an attempt, eventually `DEAD`. |
| Postgres losing committed data (async-replica failover, disk loss) | ❌ | Out of scope M1 (A3). Revisit with HA in later milestone. |
| Provider violating its idempotency contract | ❌ | Detected by reconciliation/invariants, not prevented. |
| Byzantine / malicious components | ❌ | A5. |
| Fake provider process restart (losing its in-memory key store) | ❌ | Test infrastructure, not system under test. |

---

## 7. Architecture (M1)

```
            ┌──────────────┐  POST /workflows (Idempotency-Key)
 clients ──▶│   API        │──────────────┐
            │  (idem api)  │◀── webhooks ─┼──────────────────────┐
            └──────┬───────┘              │                      │
                   │ txn                  │                      │
                   ▼                      │                      │
            ┌──────────────┐              │              ┌───────┴────────┐
            │   Postgres   │◀── claim / complete (txn) ──│  Fake provider │
            │  (state of   │              │              │  charges,      │
            │   record)    │              │              │  payouts,      │
            └──────▲───────┘   ┌──────────┴───┐          │  refunds,      │
                   │           │  Workers ×N  │──HTTP───▶│  fault inject, │
                   │           │  (idem       │ Idem-Key │  effect oracle │
                   │           │   worker)    │          └────────────────┘
                   │           └──────────────┘                  ▲
            ┌──────┴───────┐                                     │
            │  Sweeper     │  expired leases, callback           │
            │ (idem        │  deadlines, stuck workflows   ┌─────┴───────┐
            │  sweeper)    │                               │ Chaos       │
            └──────────────┘                               │ harness +   │
                                                           │ invariant   │
                                                           │ checker     │
                                                           └─────────────┘
```

- The webhook arrow (provider → API) arrives in **M2**; in M1 all provider interaction is request/response.
- **Local runtime:** `docker compose` in M1; `minikube` from M4.
- **One binary** `idem` with subcommands `api | worker | sweeper | all`. Each role is stateless; all state lives in Postgres. Running several of each must be safe.
- **`fakeprovider`** — a separate Go HTTP service imitating a Stripe-like provider, with fault injection and an "effects oracle" (§14.1).
- **`chaos`** — orchestrates processes, injects faults, then runs the invariant checker (§14).

### Repository layout (proposed)
```
cmd/idem/            # api, worker, sweeper subcommands
cmd/fakeprovider/
cmd/chaos/
internal/workflow/   # state machines, payment_payout definition
internal/store/      # Postgres access (hand-written SQL via pgx)
internal/ledger/
internal/provider/   # provider client + outcome classification
internal/webhook/    # inbox, signature verification (M2)
internal/failpoint/
internal/invariants/
migrations/
docs/design.md
docs/adr/
```

### Tech stack (confirmed 2026-11-09)
| Concern | Choice | Why |
|---|---|---|
| Language | Go 1.25 (`go 1.25` in `go.mod`) | Installed locally (1.25.6). |
| Database | Postgres 18 (docker compose) | D1. |
| Driver | `jackc/pgx/v5` + `pgxpool` | Postgres-native; exposes SQLSTATE error codes (needed to detect unique-violation vs. serialization failure). |
| Query layer | `sqlc` (generates typed Go from `.sql` files) | You still write every query by hand — locking clauses stay visible — but no manual row scanning. **No ORM.** |
| Migrations | `pressly/goose`, SQL files embedded with `embed.FS` | Plain SQL; runnable from the binary (`idem migrate`) or CLI. |
| HTTP | stdlib `net/http` (Go 1.22+ method/path routing) | No framework needed. |
| CLI | `spf13/cobra` | Subcommands + admin CLI; same library as kubectl/kubebuilder, useful again in M4. |
| Config | Env vars via `caarlos0/env` | 12-factor; maps directly onto K8s manifests later. |
| Logging | stdlib `log/slog` (JSON handler) | |
| Metrics | `prometheus/client_golang` | |
| IDs | `google/uuid`, UUIDv7 | Time-ordered → index-friendly primary keys. |
| Tests | stdlib `testing` + `stretchr/testify/require`, `testcontainers-go` (Postgres module) | Real Postgres in tests; never mock the database. |
| Lint | `golangci-lint` | |
| Task runner | `Makefile` | |

---

## 8. Workflow and step model

### 8.1 The `payment_payout` workflow

```
charge ──success──▶ payout ──success──▶ ✅ SUCCEEDED
  │                   │
  │ permanent fail    │ permanent fail
  ▼                   ▼
 ❌ FAILED          refund (compensation) ──success──▶ ↩ COMPENSATED
 (nothing to undo)    │
                      │ permanent fail / exhausted
                      ▼
                    🚨 NEEDS_ATTENTION
```

Ledger postings are **not** separate steps. Each is written inside the success transaction of the external step it describes (decision D2, §16). This removes the window "charge succeeded at the provider but the ledger wasn't updated".

| Step | External call | Async? | On success (same txn) | Compensable? |
|---|---|---|---|---|
| `charge` | `POST /charges` | No | Post charge entries; create `payout` step | Yes, via `refund` |
| `payout` | `POST /payouts` | **No in M1** (synchronous). M2: `202 pending` + webhook | Post payout entries; workflow `SUCCEEDED` | **No — pivot step.** Once paid, money is gone. |
| `refund` | `POST /refunds` | No | Post reversal entries; workflow `COMPENSATED` | No (must be retried hard) |

### 8.2 Workflow states
```
PENDING ─▶ RUNNING ─┬─▶ SUCCEEDED                     (terminal)
                    ├─▶ FAILED                        (terminal: charge declined)
                    ├─▶ COMPENSATING ─▶ COMPENSATED   (terminal)
                    └─▶ NEEDS_ATTENTION               (from any non-terminal state)
```
`NEEDS_ATTENTION` exits only via an explicit admin action (§11.4).

### 8.3 Step states
```
         claim                 success
PENDING ───────▶ RUNNING ─────────────────▶ SUCCEEDED
   ▲               │  │  │
   │ backoff       │  │  └─ 202 accepted ─▶ AWAITING_CALLBACK ─┬─ webhook/poll: paid ──▶ SUCCEEDED
   │ (no effect)   │  │                                         └─ webhook/poll: failed ─▶ FAILED_PERMANENT
   └───────────────┘  │
                      ├─ permanent error ─▶ FAILED_PERMANENT
                      │
                      └─ ambiguous outcome, or lease expired ─▶ IN_DOUBT ── claim ──▶ RUNNING
                                                                      (same key, or lookup)

any non-terminal, attempts exhausted ─▶ DEAD   (workflow → NEEDS_ATTENTION)
```
- `PENDING` and `IN_DOUBT` are both claimable. The difference is semantic: `IN_DOUBT` means *"we may already have caused an effect"*, which matters for metrics, alerting, and for what happens when attempts run out (a `DEAD` step that was `IN_DOUBT` may have moved money — the alert says so).
- `AWAITING_CALLBACK` is introduced in **M2** with async payouts; it is shown here so the M1 state machine is designed to accept it.
- `DEAD` is the dead-letter state. There is **no separate DLQ table** in M1; the dead-letter queue is `SELECT … WHERE status = 'DEAD'` (decision D4).

---

## 9. Executing an external step

### 9.1 The three-phase protocol

Every external step runs as **claim → call → complete**. The crash windows between phases are deliberate and each is covered in §12.

**Phase A — claim (one transaction)**
```sql
WITH next AS (
  SELECT id FROM steps
  WHERE status IN ('PENDING', 'IN_DOUBT') AND run_after <= now()
  ORDER BY run_after
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE steps s
SET status           = 'RUNNING',
    lease_owner      = $worker_id,
    lease_epoch      = s.lease_epoch + 1,
    lease_expires_at = now() + $lease_ttl,
    attempt_count    = s.attempt_count + 1,
    prior_status     = s.status            -- remember if we came from IN_DOUBT
FROM next
WHERE s.id = next.id
RETURNING s.*;
-- + INSERT INTO step_attempts (... outcome = NULL ...)
```
The worker keeps the returned `lease_epoch` — this is its **fencing token**.

**Phase B — call the provider (no transaction open)**
- Send the request with `Idempotency-Key: <step.idempotency_key>`.
- HTTP timeout `provider_call_timeout`. Config validation at startup enforces `provider_call_timeout + commit_budget < lease_ttl`, so a *healthy* worker always finishes inside its lease. Pauses are handled by fencing, not by this rule.
- If the step came from `IN_DOUBT` and the key is older than `provider_key_retention − safety_margin`, **look up by key** instead of re-sending (§9.4).

**Phase C — complete (one transaction, fenced)**
```sql
BEGIN;
UPDATE steps
SET status = $new_status, provider_ref = $ref, lease_owner = NULL, ...
WHERE id = $step_id AND lease_epoch = $my_epoch AND status = 'RUNNING';
-- 0 rows affected → we lost the lease. ROLLBACK, increment
-- idem_fenced_commits_total, log, and walk away. The new owner will
-- get the same result from the provider via the idempotency key.

-- if success:
INSERT INTO ledger_transactions ...;   -- UNIQUE (step_id, kind)
INSERT INTO ledger_entries ...;        -- balanced, checked at COMMIT
INSERT INTO steps (...next step...);   -- or update workflow to terminal
UPDATE workflows SET status = ..., version = version + 1 WHERE id = $wf;
INSERT INTO outbox ...;
UPDATE step_attempts SET outcome = ..., finished_at = now() WHERE ...;
COMMIT;
```
**Rule:** every change of step ownership — claim, sweeper reclaim, admin action — increments `lease_epoch`. One rule, no special cases.

### 9.2 Idempotency keys

**Outbound (engine → provider)**
- Derived when the step row is created: `hex(sha256(workflow_id ‖ ":" ‖ step_name ‖ ":" ‖ generation))`.
- **Stored** on the step row and read from there forever after. The stored value is authoritative; if the derivation formula ever changes, existing steps are unaffected.
- `generation` starts at 0 and is only incremented by an explicit admin action ("I have verified no effect occurred; try again as a new payment"). Never automatically.

**Inbound (client → engine API)**
- Client sends `Idempotency-Key` on `POST /workflows`, scoped per client.
- Engine stores `(client_id, key, request_fingerprint, response)` where `request_fingerprint = sha256(canonical JSON body)`.
- Same key + same fingerprint → replay the stored response byte-for-byte with its original status (`201`) and header `Idempotent-Replayed: true`. The response is stored as `bytea`, not `jsonb`, because `jsonb` normalises key order and whitespace.
- Only successful creations are stored; requests rejected by validation never reserve the key.
- Same key + different fingerprint → `422 Unprocessable Entity`. (A client bug — don't guess which request they meant.)
- Two concurrent requests with the same key → primary-key conflict; the loser waits briefly and returns the winner's stored response, or `409` if the winner hasn't committed yet.
- The key row, the workflow row, and the first step row are inserted **in one transaction**.
- Retention: forever in M1 (cheap at this scale). Expiry policy → M2.

**Inbound webhooks (provider → engine) — M2** — the place where cryptography actually earns its keep:
- `HMAC-SHA256(webhook_secret, timestamp ‖ "." ‖ body)` in a signature header; constant-time compare.
- Reject if `|now − timestamp| > 5 min` (replay protection).
- Dedup by the provider's `event_id` (§9.5).

### 9.3 Classifying provider outcomes

The central rule: **if the request might have reached the provider, the outcome is ambiguous.**

| Observation | Class | Next step state |
|---|---|---|
| `2xx` with final result | `SUCCESS` | `SUCCEEDED` |
| `202` pending (payouts, **M2**) | `ACCEPTED_ASYNC` | `AWAITING_CALLBACK`, `callback_deadline = now() + X` |
| `400`, `402`, `422` (declined, invalid account, validation) | `PERMANENT` | `FAILED_PERMANENT` |
| `409` (same key currently being processed at provider) | `RETRYABLE_IN_FLIGHT` | `IN_DOUBT`, short backoff |
| `429` | `RETRYABLE_NO_EFFECT` | `PENDING`, backoff honouring `Retry-After` |
| DNS failure / connection refused (nothing sent) | `RETRYABLE_NO_EFFECT` | `PENDING`, backoff |
| `5xx` | `AMBIGUOUS` | `IN_DOUBT`, backoff |
| Timeout / connection reset after connect | `AMBIGUOUS` | `IN_DOUBT`, backoff |
| Unparseable 2xx body | `AMBIGUOUS` | `IN_DOUBT` + error log (likely a bug) |
| Panic in step code | `AMBIGUOUS` | `IN_DOUBT` (we can't know how far it got) |

Retry policy: exponential backoff with **full jitter** — `delay = rand(0, min(cap, base · 2^attempt))`. Defaults: `base = 2s`, `cap = 5m`, `max_attempts = 8` for charge/payout, `20` for refund (compensation must try harder).

### 9.4 Resolving `IN_DOUBT`
1. **Key age < retention − margin:** re-send the identical request with the same key. The provider either performs it (first time) or replays the original response.
2. **Key age ≥ retention − margin:** `GET` by idempotency key.
   - Found → use its result.
   - Not found → **do not retry with a new key.** → `DEAD` → workflow `NEEDS_ATTENTION`, alert: *"payment may or may not have moved; human must verify with provider"*.

### 9.5 Async payouts and webhooks (Milestone 2)

> Deferred to M2. Specified here so M1's schema and state machine leave room for it.

- **Receive:** verify signature → `INSERT INTO webhook_inbox … ON CONFLICT (provider_event_id) DO NOTHING` → commit → respond `200`. Respond `200` for duplicates too (the provider should stop retrying). Respond `5xx` if the insert fails so the provider retries.
- **Process:** a worker loop claims unprocessed inbox rows (`SKIP LOCKED`), matches them to a step by `provider_ref` or idempotency key, and applies the transition in one transaction.
  - Step not yet `AWAITING_CALLBACK` (webhook beat our own commit): leave unprocessed, retry later.
  - Step already terminal with the **same** outcome: mark processed, no-op.
  - Step already terminal with a **different** outcome (paid then failed?): `NEEDS_ATTENTION` + alert. Never flip a terminal state.
- **Never arrives:** sweeper finds `AWAITING_CALLBACK` past `callback_deadline` → polls `GET /payouts/{id}` → applies result or extends deadline (bounded number of times, then `NEEDS_ATTENTION`).

---

## 10. Ledger

- **Double-entry, append-only.** Amounts are signed `bigint` in minor units (cents): debits positive, credits negative. Every ledger transaction must sum to zero.
- Enforced by a `DEFERRABLE INITIALLY DEFERRED` constraint trigger that checks the sum at `COMMIT`.
- **Immutability:** the application's DB role has no `UPDATE`/`DELETE` on ledger tables. Corrections are new reversing transactions.
- **Exactly-once posting:** `UNIQUE (step_id, kind)` on `ledger_transactions` — a second attempt to post for the same step fails loudly instead of double-counting.
- Balances are computed with `SUM()` in M1 (no cached balance columns to drift).
- **Currency:** EUR only in M1. Every entry still carries a `currency` column (constrained to `'EUR'`) so multi-currency later is a constraint change, not a schema rewrite.
- **Fees:** platform fee per workflow, configurable (default 1.5% + €0.25). Computed in **integer cents** — never floats — with banker's rounding (half-to-even). The fee is fixed at workflow creation and stored in `workflows.input`, so retries and config changes can't alter it mid-flight.

| Event | Debit (+) | Credit (−) |
|---|---|---|
| Charge succeeded | `provider_receivable` amount | `merchant_payable` (amount − fee), `platform_revenue` fee |
| Payout succeeded | `merchant_payable` (amount − fee) | `provider_receivable` (amount − fee) |
| Refund succeeded | `merchant_payable` (amount − fee), `platform_revenue` fee | `provider_receivable` amount |

After a fully successful workflow, `provider_receivable` holds exactly the fee and `merchant_payable` is back to zero for that workflow. After a compensated workflow, every account nets to zero.

---

## 11. Background processes

### 11.1 Sweeper (any number may run; all transitions are CAS)
| Condition | Action |
|---|---|
| `RUNNING` and `lease_expires_at < now()` | → `IN_DOUBT`, `lease_epoch + 1`, mark the open attempt `ABANDONED`. |
| `AWAITING_CALLBACK` and `callback_deadline < now()` | Poll provider (§9.5). **M2.** |
| Non-terminal workflow older than `stuck_threshold` | Raise metric/alert. Don't change state — just make it loud. |
| Inbox rows unprocessed for > threshold | Raise metric/alert. **M2.** |

### 11.2 Graceful shutdown (rolling deploys)
`SIGTERM` → stop claiming → let in-flight steps finish within `shutdown_grace` → exit. Anything not finished falls through to the lease-expiry path; that's correct, just slower.

### 11.3 Outbox
Workflow lifecycle events (`workflow.succeeded`, `workflow.compensated`, …) are written to `outbox` in the same transaction as the transition. M1 only writes them; a relay that publishes and marks `published_at` comes in M2.

### 11.4 Admin actions (CLI, audited)
- `idem admin list --status NEEDS_ATTENTION|DEAD`
- `idem admin retry-step <id>` — same key; resets attempts. Safe.
- `idem admin new-generation <step-id> --reason "..."` — new key. **Dangerous.** Requires a reason, writes an audit row.
- `idem admin resolve <workflow-id> --as SUCCEEDED|FAILED --reason "..."` — record a human's verified conclusion.

---

## 12. Failure-mode table

The heart of the design. Every row should eventually map to at least one automated test.

| # | Scenario | What happens | Result | Test |
|---|---|---|---|---|
| F1 | Client times out on `POST /workflows` and retries | Inbound key matches, same fingerprint | Same workflow ID returned; one workflow | integration |
| F2 | Client reuses key with a different body | Fingerprint mismatch | `422`; no workflow created | integration |
| F3 | Two identical submissions race | PK conflict on key row | One workflow; both callers get its ID | integration |
| F4 | Worker crashes after claim, before calling provider | Lease expires → sweeper → `IN_DOUBT` → re-claimed → same key sent | Effect happens once | failpoint `before_provider_call` |
| F5 | Worker crashes after provider succeeded, before commit | Same as F4; provider replays original response | Effect once; ledger posted once | failpoint `after_provider_call` |
| F6 | Worker crashes mid-commit | Transaction rolls back atomically → F5 path | Same as F5 | failpoint `before_commit` |
| F7 | Provider timeout, effect happened | `AMBIGUOUS` → `IN_DOUBT` → retry same key → replay | Effect once | provider fault `timeout_after_effect` |
| F8 | Provider timeout, effect didn't happen | Same path; retry performs it | Effect once | provider fault `timeout_before_effect` |
| F9 | Provider 5xx | `AMBIGUOUS` → `IN_DOUBT` | Effect at most once | provider fault `error_after_effect` |
| F10 | Card declined | `PERMANENT` | Workflow `FAILED`; no ledger entries | provider fault `decline` |
| F11 | Provider 429 | Backoff with `Retry-After` | Eventually proceeds | provider fault `rate_limit` |
| F12 | Worker paused past lease (`SIGSTOP`), resumes, commits | Sweeper bumped epoch; new owner finished. Stale commit matches 0 rows | Stale result discarded; `fenced_commits_total`++ | chaos: pause |
| F13 | Paused worker resumes and *sends* the provider call late | Same idempotency key → provider dedupes | No duplicate effect | chaos: pause + oracle |
| F14 | Postgres down mid-flight | No claims → no new calls; in-flight completes can't commit → lease path after recovery | No loss, no duplicates; delayed | chaos: stop DB |
| F15 | **(M2)** Duplicate webhook | Inbox `ON CONFLICT DO NOTHING` | Processed once | provider fault `dup_webhook` |
| F16 | **(M2)** Webhook arrives before our `AWAITING_CALLBACK` commit | Inbox row left unprocessed and retried | Applied once step is ready | provider fault `fast_webhook` |
| F17 | **(M2)** Webhooks out of order / contradictory | Terminal states never flip; conflict → `NEEDS_ATTENTION` | Loud, not wrong | provider fault `reorder` |
| F18 | **(M2)** Webhook never arrives | Callback deadline → poll | Resolved by polling | provider fault `drop_webhook` |
| F19 | **(M2)** Forged or replayed webhook | Signature / timestamp check fails | `401`; nothing stored | unit + integration |
| F20 | Payout permanently fails after successful charge | `refund` step created; reversal posted on success | Workflow `COMPENSATED`; ledger nets to 0 | provider fault `payout_fail` |
| F21 | Refund keeps failing | Up to 20 attempts → `DEAD` | `NEEDS_ATTENTION` + alert | provider fault |
| F22 | Retries exhausted on any step | `DEAD` | `NEEDS_ATTENTION`; alert says whether money may have moved | unit |
| F23 | `IN_DOUBT` key older than provider retention | Lookup by key; if unknown, `DEAD` | Never retried with a new key | short retention in test |
| F24 | Clock skew between workers | Leases use DB `now()` only | No effect | review + unit |
| F25 | Panic inside step handler | Recovered → `AMBIGUOUS` → `IN_DOUBT` | Retried; eventually `DEAD` if persistent | unit |
| F26 | Rolling deploy (SIGTERM) | Graceful drain; leftovers via lease path | No loss | chaos: restart all |
| F27 | Engine bug mints a fresh key on retry | Provider performs a second effect | **Caught by invariant I1** (oracle counts effects per workflow, not per key) | chaos |

---

## 13. Invariants (checked after every chaos run)

| ID | Invariant | Checked against |
|---|---|---|
| **I1** | Per workflow, the provider performed ≤ 1 charge, ≤ 1 payout, ≤ 1 refund. | Provider effects oracle (counts by `workflow_id` metadata, **not** by key) |
| **I2** | Every ledger transaction sums to 0; all entries sum to 0. | DB |
| **I3** | Ledger agrees with provider: each provider effect has exactly one posting and vice versa. | DB × oracle (reconciliation) |
| **I4** | After faults stop and a quiescence period, no workflow is `PENDING`/`RUNNING`/`COMPENSATING` and no step is `RUNNING`/`IN_DOUBT`/`AWAITING_CALLBACK`. | DB |
| **I5** | No payout exists for a workflow without a successful charge. | Oracle |
| **I6** | Every inbound idempotency key maps to exactly one workflow. | DB |
| **I7** | `SUCCEEDED` ⇒ charge+payout effects at provider; `COMPENSATED` ⇒ charge+refund, no payout; `FAILED` ⇒ no effects. | DB × oracle |

`NEEDS_ATTENTION` workflows are allowed by I4 but listed in the report — a run that produces many of them is a design smell even if no invariant breaks.

---

## 14. Testing strategy and fault injection

### 14.1 Fake provider
- Endpoints: `POST /charges`, `POST /payouts` (synchronous in M1, async in M2), `POST /refunds` (must reference a real charge; total refunds ≤ the charge), `GET /{resource}?idempotency_key=…` (`409` while the key is in flight). Keys are namespaced per resource. Deterministic refusals: customers `cus_decline*` (402), merchants `mer_invalid*` (422).
- Implements Stripe-like idempotency: same key + same body → replay; same key + different body → `422`; same key in flight → `409`. Keys expire after `provider_key_retention` (set to minutes in tests to exercise F23).
- Payouts answer synchronously in M1. **M2:** returns `202` and sends signed webhooks.
- **Fault injection** via `PUT /_faults`, seeded RNG, per-endpoint probabilities:
  `latency`, `timeout_before_effect`, `timeout_after_effect`, `error_after_effect`, `decline`, `rate_limit`, `payout_fail`. M2 adds webhook faults: `dup_webhook`, `fast_webhook`, `delay_webhook`, `reorder`, `drop_webhook`, `bad_signature`.
- **Effects oracle** `GET /_effects`: every real effect performed, with workflow metadata — the ground truth for I1, I3, I5, I7.
- In-memory state; restarts are out of the fault model.

### 14.2 Failpoints
`failpoint.Hit("after_provider_call")` calls compiled into the engine. Configured via env (`IDEM_FAILPOINTS=after_provider_call=exit@0.05`) with actions `exit`, `panic`, `sleep:<d>` and an optional `@probability`; unknown names are rejected at startup. Compiled out with a build tag in release builds (`make build-release`, tag `nofailpoint`). Dev/compose images keep them compiled in but inactive unless `IDEM_FAILPOINTS` is set, so chaos runs can use them.

Initial set: `after_claim`, `before_provider_call`, `after_provider_call`, `before_commit`, `after_commit`. (`sweeper_after_select` is not needed: lease reclaim is a single atomic statement with no window between select and update. M2 adds `webhook_after_insert`.)

### 14.3 Test layers
1. **Unit** — table-driven state-machine transitions, outcome classification, backoff, key derivation, signature verification.
2. **Integration** — real Postgres via `testcontainers-go`; each F-row with a deterministic setup.
3. **Chaos runs** (`cmd/chaos`):
   1. Start Postgres, fake provider, API, N workers, M sweepers as child processes.
   2. Submit K workflows, with clients randomly retrying and double-submitting.
   3. While running: randomly `SIGKILL`, `SIGSTOP`/`SIGCONT` workers, restart the DB, enable provider faults and failpoints.
   4. Stop faults; wait for quiescence (or timeout).
   5. Run the invariant checker; write a report (counts per terminal state, fenced commits, `IN_DOUBT` resolutions, violations with workflow IDs).
   - Runs are seeded. Honest caveat: OS scheduling isn't deterministic, so a seed reproduces the *fault plan*, not the exact interleaving. Fully deterministic simulation is a stretch goal.

**Order of work (failure-first):** the fake provider, failpoints, and invariant checker are built *alongside* the first step, not after the flow is complete (§15).

---

## 15. Milestone 1 delivery slices

Each slice ends with passing tests and a short write-up in `docs/journal/`.

| Slice | Delivers | Failure work in the same slice |
|---|---|---|
| **1.1** Skeleton | Repo, migrations, `docker compose` (Postgres), `POST /workflows` with inbound idempotency | F1–F3 tests |
| **1.2** One step, end to end | Claim/lease/fencing, `charge` against fake provider, ledger posting, fake provider + oracle, failpoint package, invariant checker v0 (I1, I2), **lease-expiry reclaim (pulled forward from 1.3)** | F4–F6, F7–F9 (fixed-delay retry), F10, F12, F13, F27 |
| **1.3** Ambiguity | Outcome classification (incl. no-effect 429 / refused connections, `Retry-After`, 409 in-flight), retries with full-jitter backoff, sticky `IN_DOUBT`, `DEAD`/`NEEDS_ATTENTION` with `dead_reason`, lookup near key retention, stuck-workflow reporting, bounded shutdown | F7–F9, F11, F22, F23, F25 |
| **1.4** Full flow | `payout` (synchronous, the pivot), `refund` compensation only on a *definite* payout failure, refund charge reference frozen in a typed column, CAS workflow transitions, fees, I3, I5, I7 | F20, F21 |
| **1.5** Chaos harness | `cmd/chaos`, process kill/pause/DB restart, report | F13, F14, F26; full invariant suite |
| **1.6** Operability | Metrics, structured logs, admin CLI, runbook for `NEEDS_ATTENTION` | — |

### Observability (M1 minimum)
- **Structured logs** (`log/slog`, JSON) with `workflow_id`, `step`, `attempt`, `lease_epoch`, `worker_id` on every line.
- **Prometheus metrics:** `steps{status}`, `in_doubt_steps`, `oldest_nonterminal_workflow_age_seconds`, `fenced_commits_total`, `step_attempts_total{step,outcome_class}`, `provider_request_duration_seconds{endpoint,outcome_class}`, `needs_attention_workflows`, `webhook_inbox_unprocessed`.
- OpenTelemetry tracing → Milestone 3.

### Later milestones (outline only)
- **M2** — **Async payouts + webhooks first** (inbox, HMAC signatures, `AWAITING_CALLBACK`, deadline polling; F15–F19); then generalise workflow definitions; outbox relay; key retention/expiry; admin tooling polish; settlement reconciliation job.
- **M3** — Prove it at scale: long chaos campaigns, toxiproxy network faults, load tests, tracing, SLOs + alerts, a published chaos report.
  - **Traffic generator (`cmd/loadgen`)** — simulates 100–500 external clients against the public API only (no database or internal shortcuts while generating load):
    - **Realistic misbehaviour, not just happy paths:** clients that time out and retry with the same key; double submissions; impatient retries with a *new* key (a real client bug the system must survive); declined customers; malformed payloads.
    - **Open workload model:** requests arrive at a target rate whether or not earlier ones finished (avoids coordinated omission hiding latency).
    - **Runs with provider faults enabled**, and ends with a correctness verdict: every submitted workflow reached a terminal or attention state, and `idem check` reports zero invariant violations.
    - **Reports** request rate, latency percentiles and error classes; pairs with M3 metrics/tracing to locate bottlenecks (claim contention, pool exhaustion, sweeper cost, hot keys).
    - Runs against docker compose first; packaged as its own Helm release in M4 for minikube. Numbers on a laptop are relative ("this change made claims 3x slower"), not capacity claims.
- **M4** — Kubernetes operator (kubebuilder, developed on minikube): `WorkflowDefinition`, `SchedulerCluster` (workers, migrations, scaling on queue depth, safe upgrades), `DeadLetterPolicy`. Workflow *instances* stay in Postgres, never in etcd.
- **M5 (stretch)** — etcd-backed lease implementation behind the same interface, compared under partitions; deterministic simulation testing; sharding.

---

## 16. Decisions (to become ADRs in `docs/adr/`)

| ID | Decision | Why | Alternative rejected |
|---|---|---|---|
| D1 | Postgres as the single state store | Core invariants need multi-row atomic transactions and unique constraints; matches real payment systems | Distributed KV (etcd/Redis): would require rebuilding transactions; moves the project away from the payments problem |
| D2 | Ledger posting inside the external step's success transaction | Removes the "provider succeeded, ledger not updated" window | Separate ledger step: adds a crash window and a step to recover |
| D3 | All lease timing from DB `now()` | Immune to worker clock skew | Worker-side timestamps |
| D4 | Dead-letter = `DEAD` status, not a separate table | One source of truth; no move-between-tables crash window | DLQ table |
| D5 | Ambiguous outcomes → `IN_DOUBT`, never `FAILED` | Assuming failure is how double payments happen | Treat timeouts as failures |
| D6 | Outbound keys stored at step creation; new keys only by audited admin action | Prevents silent double payment from code changes or bugs | Recompute key on each attempt |
| D7 | Every ownership change bumps `lease_epoch` | One fencing rule with no special cases | Bump only on claim |
| D8 | Hand-written SQL, `sqlc`-generated Go on `pgx/v5`, no ORM | Learning goal; locking clauses stay visible; type safety without row-scanning boilerplate | ORM (GORM, ent): hides transactions and locking — the very things this project is about |
| D9 | Payouts synchronous in M1; async + webhooks first thing in M2 | Reach a complete, chaos-tested flow sooner; M1 schema/state machine already leave room | Async in M1 |
| D10 | Single currency (EUR) in M1, `currency` column kept | Simplicity now, cheap to extend later | Multi-currency from day one |
| D11 | Platform fees kept; integer cents, half-to-even rounding, fixed at workflow creation | Realism; fee rounding is a classic source of ledger drift | No fees |

**Calls made by Claude while drafting (please check):** D2, D4, D7, D8; fee defaults and rounding rule; retry defaults; one hard-coded workflow type; static API token; in-memory fake provider; `generation` mechanism for deliberate new keys.

---

## 17. Open questions

None open.

**Resolved (2026-11-09)**
- Name: **`idem`**.
- Tooling: **stack in §7 confirmed** (D8).

**Resolved (2026-11-08)**
- Single currency: **EUR** (D10).
- Fees: **kept** (D11).
- Async payouts: **moved to M2** (D9).
- Go: **1.25** (1.25.6 installed).
- Local runtime: **docker compose** for M1, **minikube** for M4.

---

## 18. Glossary

| Term | Meaning |
|---|---|
| **Effectively-once** | Work may run more than once; its effects apply once. |
| **Idempotency key** | Identifier for one *intended* effect; the receiver performs it once and replays the result for repeats. |
| **Lease** | Time-limited ownership of a step by a worker. |
| **Fencing token** | Monotonic number (`lease_epoch`) that lets the database reject writes from a worker that lost its lease. |
| **`IN_DOUBT`** | We may have caused an external effect but don't know. |
| **Pivot step** | The point after which a saga can only move forward (here: payout). |
| **Compensation** | A new action that semantically undoes an earlier one (refund undoes charge). Not a rollback. |
| **Outbox** | Table of events written in the same transaction as state changes, published later, so events never disagree with state. |
| **Inbox** | Table of received messages, deduplicated by ID, processed asynchronously. |
| **Oracle** | Independent record of ground truth (here: the fake provider's effect log) used to check invariants. |

## 19. References
- Brandur Leach — *Implementing Stripe-like Idempotency Keys in Postgres*
- Stripe API docs — *Idempotent requests*
- Martin Kleppmann — *How to do distributed locking* (fencing tokens)
- Martin Kleppmann — *Designing Data-Intensive Applications*, ch. 7–9
- Pat Helland — *Life beyond Distributed Transactions*
- Garcia-Molina & Salem — *Sagas* (1987)
- Temporal / Restate / DBOS docs — durable execution concepts
- Jepsen analyses — how invariant-checking under faults is done in practice
