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
- Publishing outbox events to an external broker. (Outbox rows are written since M2 slice 2.4; delivery to shops by webhook is slice 2.5.)
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

- The webhook arrow (provider → API) is live since **M2 slice 2.2**: `POST /webhooks/provider` verifies and stores events in `webhook_inbox`, and an inbox processor in each worker applies them. Polling (slice 2.1) stays as the safety net.
- **Local runtime:** `docker compose` in M1; `minikube` from M4.
- **One binary** `idem` with subcommands `api | worker | sweeper` (plus `migrate`, `check` and `admin`). Each role runs as its own process; there is no all-in-one mode, because compose and Kubernetes already run one process per role. Each role is stateless; all state lives in Postgres. Running several of each must be safe.
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
docs/adr/            # one file per decision (§16)
docs/journal/        # one write-up per delivery slice (§15)
```

### Tech stack (confirmed 2026-11-09)
| Concern | Choice | Why |
|---|---|---|
| Language | Go 1.27 (`toolchain go1.27.4` in `go.mod`) | Moved from 1.25 when CI began scanning: 1.25 was out of upstream support, with unpatched reachable vulnerabilities. |
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
| `payout` | `POST /payouts` | **Yes (M2, slice 2.1):** `202 pending` → `AWAITING_CALLBACK` → workers poll after the callback deadline (webhooks: slice 2.2) | Post payout entries when **paid**; workflow `SUCCEEDED` | **No — pivot step.** Once paid, money is gone. |
| `refund` | `POST /refunds` | No | Post reversal entries; workflow `COMPENSATED` | No (must be retried hard) |

### 8.2 Workflow states

Enforced by the database since slice 2.5b (migration 00017, D23): a trigger refuses any other move with SQLSTATE `23514`, naming the workflow and both statuses. Writing a status again is always allowed.

```
PENDING ─▶ RUNNING ─┬─▶ SUCCEEDED                        (terminal)
              ▲     ├─▶ FAILED                           (terminal: charge declined, or an operator's `fail`)
              │     ├─▶ COMPENSATING ─┬─▶ COMPENSATED    (terminal)
              │     │       ▲         └─▶ NEEDS_ATTENTION
              │     └─▶ NEEDS_ATTENTION ──▶ FAILED (operator)
              │               │    └─────▶ COMPENSATING (operator: compensate; or retry-step,
              │               │                          new-generation, confirm-step of a refund)
              └───────────────┘ (operator: retry-step, new-generation, confirm-step of a charge or payout)
```

| From | To | Who |
|---|---|---|
| `PENDING` | `RUNNING` | the first claim of its step |
| `RUNNING` | `SUCCEEDED`, `FAILED`, `COMPENSATING` | step hooks (payout paid; charge refused; payout refused) |
| `RUNNING`, `COMPENSATING` | `NEEDS_ATTENTION` | a step parked (`DEAD`, or a refund refused) |
| `COMPENSATING` | `COMPENSATED` | the refund's hook |
| `NEEDS_ATTENTION` | `RUNNING`, `COMPENSATING`, `FAILED` | admin actions (§11.4) |

- **Terminal** (`SUCCEEDED`, `FAILED`, `COMPENSATED`) is defined once, by the SQL function `workflow_is_terminal`, and never left; each terminal workflow has exactly one outbox event (D13, I8). **Active** (`PENDING`, `RUNNING`, `COMPENSATING`: claimable, watched by the stuck alert) is `workflow_is_active`.
- `PENDING` never parks: a workflow is `RUNNING` by the time any of its steps can be `DEAD`.

### 8.3 Step states

Enforced by the database since slice 2.5b (migration 00018), the same way.

```
         claim                 success
PENDING ───────▶ RUNNING ─────────────────▶ SUCCEEDED   (final)
   ▲               │  │  │
   │ backoff       │  │  └─ 202 accepted ─▶ AWAITING_CALLBACK ─┬─ poll: claim ─▶ RUNNING
   │ (no effect)   │  │                                         └─ webhook: paid / failed ─▶ SUCCEEDED / FAILED_PERMANENT / DEAD
   └───────────────┘  │
                      ├─ permanent error ─▶ FAILED_PERMANENT
                      ├─ attempts exhausted, or parked ─▶ DEAD
                      └─ ambiguous outcome, or lease expired ─▶ IN_DOUBT ── claim ──▶ RUNNING
                                                                      (same key, or lookup)
DEAD, FAILED_PERMANENT ── operator: retry-step ─▶ PENDING / IN_DOUBT;  confirm-step ─▶ SUCCEEDED
```

| From | To | Who |
|---|---|---|
| `PENDING`, `IN_DOUBT`, `AWAITING_CALLBACK` | `RUNNING` | a claim (send, re-send, lookup or poll) |
| `RUNNING` | `PENDING`, `IN_DOUBT`, `AWAITING_CALLBACK`, `SUCCEEDED`, `FAILED_PERMANENT`, `DEAD` | the worker's completion, the sweeper's lease reclaim, the hook-failure limit |
| `AWAITING_CALLBACK` | `SUCCEEDED`, `FAILED_PERMANENT`, `DEAD` | a provider webhook, applied without a lease (fenced by `lease_epoch`) |
| `DEAD`, `FAILED_PERMANENT` | `PENDING`, `IN_DOUBT`, `SUCCEEDED` | admin `retry-step` and `confirm-step` |

- `PENDING` and `IN_DOUBT` are both claimable. The difference is semantic: `IN_DOUBT` means *"we may already have caused an effect"*, which matters for metrics, alerting, and for what happens when attempts run out (a `DEAD` step that was `IN_DOUBT` may have moved money — the alert says so).
- `AWAITING_CALLBACK` arrived in **M2 slice 2.1**. An accepted payout waits there until its `callback_deadline`, then becomes claimable for a **poll** (`GET /payouts/{id}`). Polls are attempts of kind `poll`, counted in `poll_count` and not in the send budget. They send nothing, so they never create doubt (D17), and a crashed poll goes back to waiting, not to `IN_DOUBT`. Polls back off from `IDEM_RETRY_BASE` up to `IDEM_CALLBACK_POLL_CAP` (1h). After `IDEM_CALLBACK_MAX_WAIT` (72h) since acceptance, or an optional `IDEM_CALLBACK_MAX_POLLS` per acceptance, the step is `DEAD` with `callback_unresolved` (money may have moved); an operator's retry re-sends and starts a fresh budget. A waiting step counts as healthy for the stuck alert until its poll is due.
- `DEAD` is the dead-letter state. There is **no separate DLQ table** in M1; the dead-letter queue is `SELECT … WHERE status = 'DEAD'` (decision D4).

### 8.4 State encoded in other columns

Some states live outside the `status` columns. They are deliberate (adding statuses would multiply the tables above), but they are states, and code that reads them must read them the same way:

| What | Encoded as | Read by |
|---|---|---|
| **Money may have moved** (sticky, cleared only by a new generation) | step `IN_DOUBT`, or an ambiguous / abandoned / dead send in its history (`step_send_doubt`): an accepted re-send makes the status `AWAITING_CALLBACK`, which hides the taint | one Go predicate, `ClaimedStep.MoneyMayHaveMoved` (the pivot rule, dead reasons, sticky doubt, lookup near key retention, hook failures); admin commands read the same facts in SQL |
| **Why a step is `RUNNING`**: a first send, a re-send after doubt, or a poll | `steps.prior_status` (`PENDING`, `IN_DOUBT`, `AWAITING_CALLBACK`) | the attempt kind, `decide` vs `decidePoll`, where the sweeper reclaims to (a poll goes back to waiting, never to doubt) |
| **How an `IN_DOUBT` step is resolved**: re-send, or look up by key | the key's age against `IDEM_PROVIDER_KEY_RETENTION − IDEM_KEY_RETENTION_MARGIN`; changes with time, without a write | `Worker.nearKeyRetention` |
| **Leases**: free, held, expired | `lease_expires_at` against `now()` on steps, inbox and outbox rows; for inbox and outbox also backing off (`next_attempt_at` in the future), all inside `PENDING` | the claim queries; the relay's per-client cap counts "`PENDING` with a live lease" |
| **`AWAITING_CALLBACK`**: waiting, or due for a poll | `callback_deadline` (and `run_after`) against `now()`; the poll budget restarts at each acceptance (the latest send attempt) | `ClaimStep`, `decidePoll` |
| **Current step or history** | a `DEAD` step is parked only if it is its workflow's newest step | `LockParkedStep`, the inbox's matching |
| **Hook failures** | `hook_failures` below or at `IDEM_HOOK_MAX_FAILURES` | `RecordHookFailure` (reset by admin) |
| **Inbound idempotency key**: free, reserved, committed | no row; a row whose inserting transaction is still open (its lock holds back others); a committed row with its response | `Submit` |

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
INSERT INTO outbox ...;               -- terminal transitions only, since M2 slice 2.4 (D13)
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
| `202` pending (payouts, **M2**) | `Accepted` | `AWAITING_CALLBACK`, `callback_deadline = now() + IDEM_CALLBACK_DEADLINE` |
| `400`, `402`, `422` (declined, invalid account, validation) | `PERMANENT` | `FAILED_PERMANENT` |
| `409` (same key currently being processed at provider) | `RETRYABLE_IN_FLIGHT` | `IN_DOUBT`; retried soon (from the in-flight delay), backing off with equal jitter so a busy provider can't burn the attempt budget |
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

> **Superseded by [the M2 design](superpowers/specs/2026-11-10-milestone-2-design.md) §4.** Polling shipped in slice 2.1 (done by workers, D12); webhooks arrive in slice 2.2. The original sketch is kept below for history.

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
- **Immutability, by privilege (built in slice 2.3b):** migrations run as the table owner; the API, workers and sweepers log in as a member of the `idem_app` role, which may `SELECT` and `INSERT` on the ledger tables but has no `UPDATE`, `DELETE` or `TRUNCATE` (migration 00013 defines `idem_app`; the login itself is infrastructure: compose creates it from `deploy/postgres/initdb`, an operator with `CREATE ROLE … LOGIN IN ROLE idem_app`). Row triggers also refuse `UPDATE`/`DELETE`, as a second layer that even the owner meets. Corrections are new reversing transactions.
- **No empty transactions:** a second deferred constraint trigger rejects a ledger transaction with no entries at `COMMIT` (it would otherwise pass the balance check and record nothing).
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
| `AWAITING_CALLBACK` and `callback_deadline < now()` | Nothing: **workers** claim it and poll (D12). A poll whose lease expires goes back to `AWAITING_CALLBACK`, not `IN_DOUBT`. |
| Non-terminal workflow older than `stuck_threshold` | Raise metric/alert. Don't change state — just make it loud. |
| Inbox rows unprocessed for > threshold | Raise metric/alert. **M2.** |

### 11.2 Graceful shutdown (rolling deploys)
`SIGTERM` → stop claiming → let in-flight steps finish within `shutdown_grace` → exit. Anything not finished falls through to the lease-expiry path; that's correct, just slower.

### 11.3 Outbox
> **Built in M2 slices 2.4 (the outbox) and 2.5 (the relay)**; see the [M2 design](superpowers/specs/2026-11-10-milestone-2-design.md) §5.2–5.3 (D13, D14, D19).

A workflow's outcome (`workflow.succeeded`, `workflow.failed`, `workflow.compensated`) is written to `outbox` by `transitionWorkflow`, in the same transaction as the terminal transition, so an event exists if and only if its outcome committed. The payload is the shop's view of the workflow, from the SQL function `workflow_view`, which `GET /workflows/{id}` also returns. The database enforces the pairing too: a deferred constraint trigger refuses to commit a terminal workflow without its event (so a binary from before the outbox, still draining during a rolling upgrade, cannot end a workflow silently), and `UNIQUE (workflow_id, type)` refuses a second event of the same type. Invariant I8 checks the whole picture (one event, right type, payload equal to the view) after every chaos run and in `idem check`.

The **relay** (`idem relay`, any number may run) delivers each `PENDING` event to its client's webhook URL: it claims the row under a lease fenced by `lease_epoch` (D7), `POST`s the frozen payload signed with the client's secret (`Idem-Signature`, the inbound scheme), and records `DELIVERED`, a retry with backoff, `DEAD` after `IDEM_RELAY_MAX_ATTEMPTS`, or `NO_ENDPOINT` for a client without a URL (D19). Delivery is at least once (D14): a relay that crashes after the shop answered sends again, with the same `Idem-Event-Id`. It never follows redirects and reads at most 4 KiB of an answer. Operators list and resend events with `idem admin outbox`. Events backfilled at the 2.4 upgrade are `SKIPPED`: not announced, but resendable. Invariant I9 checks in chaos that a misbehaving shop ends up knowing exactly idem's outcomes.

### 11.3b Reconciliation (M2 slice 2.6, D15)

`idem reconcile` compares the provider's own record of money movements (its balance-transactions report, never the test oracle) with the ledger and the steps, and records every difference in `recon_runs` and `recon_discrepancies`. It changes nothing else.

- **What is compared:** for each workflow and effect type, the provider's records against the postings, whose `provider_receivable` entry is signed exactly like the provider's amount (charge `+amount`, payout `−(amount − fee)`, refund `−amount`).
- **Kinds, one per workflow and type, by precedence:** `duplicate_effect` > `amount_mismatch` > `status_mismatch` (money moved for a step recorded `FAILED_PERMANENT`) > `missing_in_ledger` > `missing_at_provider`.
- **Which workflows:** those with a provider record or a posting created in the window; each compared on everything known about it, the report read `IDEM_RECON_MARGIN` beyond both edges (a payout settles before it is posted). Workflows not final yet are skipped; parked ones are reported and flagged.
- **Surfaces:** `idem admin recon list`, the `RECON` column of `idem admin list`, the sweeper's gauges, alerts `IdemReconciliationDrift` and `IdemReconciliationStale`; compose runs it every 10 minutes. Every chaos campaign ends with it (invariant RECON).

### 11.4 Admin actions (CLI, audited)
As built in slice 1.6.
- **Safeguards on every mutating action:**
  - it needs `--reason`, and `--expect-version`: the workflow version from `list`. The workflow's `version` goes up on every status change, so a workflow that left `NEEDS_ATTENTION` and came back (e.g. another operator's retry that then charged the customer) no longer matches, and the action is refused as stale. Checked under the row lock.
  - it runs in one transaction that checks the workflow is `NEEDS_ATTENTION` and the step is its parked (latest, `DEAD`/`FAILED_PERMANENT`) step, refusing otherwise
  - it bumps `lease_epoch` when handing work back to workers
  - it writes an `admin_actions` audit row
- **The commands:**
  - `idem admin list [--reason R] [--json]`: parked workflows with their version, step, reason, attempts and last error.
  - `idem admin retry-step <step-id>`: same key, so it is safe. The step goes back `IN_DOUBT` if money may have moved (sticky), else `PENDING`. Attempt history is kept and the budget extended.
  - `idem admin confirm-step <step-id> --provider-ref REF`: a human verified the effect happened. The step goes `SUCCEEDED` and the step's own success hook runs (ledger posting, saga advance).
  - `idem admin compensate <workflow-id>`: a parked **payout** whose merchant was verified not paid. Creates the refund and moves the workflow to `COMPENSATING`.
  - `idem admin fail <workflow-id>`: a parked **charge** whose customer was verified not charged. Moves the workflow to `FAILED`.
  - `idem admin new-generation <step-id> --confirm-no-effect`: a new key. **Dangerous**: a new payment as far as the provider is concerned. The old step stays as history.
- **Why not the original `resolve --as SUCCEEDED|FAILED`:** relabelling a workflow would leave its ledger and saga inconsistent, so `confirm-step`, `compensate` and `fail` replace it.
- **Exit codes:** `0` done, `1` refused or failed (not parked, stale, doesn't fit), `2` usage mistake (decided by error type, never by error text) or a missing `--confirm-no-effect`.
- **Concurrency:** regression tests race every action against the others on the same listing (exactly one wins, losers leave no trace), with workers running so the winner's result settles cleanly. Workers can't touch a parked workflow, so the worker-reparks-while-an-operator-acts case is covered by a sequential stale-version test.
- **Runbook:** `docs/runbooks/needs-attention.md` maps each `dead_reason` to an investigation and an action.

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
| F15 | **(M2, built 2.2)** Duplicate webhook | Inbox primary key; a second event for a decided step is a no-op | Processed once | provider fault `dup_webhook` |
| F16 | **(M2, built 2.2)** Webhook arrives before our `AWAITING_CALLBACK` commit | Matched by idempotency key; deferred until the step is waiting | Applied once step is ready | provider fault `fast_webhook` |
| F17 | **(M2, built 2.2)** Webhooks out of order / contradictory | Terminal states never flip; re-checked with the provider: stale → ignored, real → `CONFLICT` (pages); parked step → evidence for the operator | Loud, not wrong | provider fault `reorder` |
| F18 | **(M2)** Webhook never arrives | Callback deadline → poll | Resolved by polling | provider fault `drop_webhook` |
| F19 | **(M2, built 2.2)** Forged or replayed webhook | Signature / timestamp check fails | `400`; nothing stored | unit + integration |
| F20 | Payout permanently fails after successful charge | `refund` step created; reversal posted on success | Workflow `COMPENSATED`; ledger nets to 0 | provider fault `payout_fail` |
| F21 | Refund keeps failing | Up to 20 attempts → `DEAD` | `NEEDS_ATTENTION` + alert | provider fault |
| F22 | Retries exhausted on any step | `DEAD` | `NEEDS_ATTENTION`; alert says whether money may have moved | unit |
| F23 | `IN_DOUBT` key older than provider retention | Lookup by key; if unknown, `DEAD` | Never retried with a new key | short retention in test |
| F24 | Clock skew between workers | Leases use DB `now()` only | No effect | static test `TestLeaseQueriesUseDatabaseClock`: no query touching a lease or schedule takes a time from Go (2.3b) |
| F25 | Panic inside step handler | Recovered → `AMBIGUOUS` → `IN_DOUBT` | Retried; eventually `DEAD` if persistent | unit |
| F26 | Rolling deploy (SIGTERM) | Graceful drain; leftovers via lease path | No loss | chaos event `restart_all`: every worker, sweeper and API replica restarted at once during traffic (2.3b) |
| F27 | Engine bug mints a fresh key on retry | Provider performs a second effect | **Caught by invariant I1** (oracle counts effects per workflow, not per key) | chaos |

---

## 13. Invariants (checked after every chaos run)

| ID | Invariant | Checked against |
|---|---|---|
| **I1** | Per workflow, the provider performed ≤ 1 charge, ≤ 1 payout, ≤ 1 refund. | Provider effects oracle (counts by `workflow_id` metadata, **not** by key) |
| **I2** | Every ledger transaction sums to 0; all entries sum to 0. | DB |
| **I3** | Ledger agrees with provider: each provider effect has exactly one posting and vice versa. A `NEEDS_ATTENTION` workflow is exempt only while its outcome is genuinely unknown (a step parked in doubt); one parked because the provider definitively did nothing (`permanent_after_in_doubt`, `attempts_exhausted_no_effect`) must match exactly. | DB × oracle (reconciliation) |
| **I4** | After faults stop and a quiescence period, no workflow is `PENDING`/`RUNNING`/`COMPENSATING` and no step is `RUNNING`/`IN_DOUBT`/`AWAITING_CALLBACK`. | DB |
| **I5** | No payout exists for a workflow without a successful charge. | Oracle |
| **I6** | Every inbound idempotency key maps to exactly one workflow. | DB |
| **I7** | `SUCCEEDED` ⇒ charge+payout effects at provider; `COMPENSATED` ⇒ charge+refund of the same amount, no payout; `FAILED` ⇒ no effects; `NEEDS_ATTENTION` with the payout parked as `permanent_after_in_doubt` ⇒ the charge only. | DB × oracle |
| **I8–I11** | Milestone 2: outbox, shop view, webhook inbox, webhook acknowledgement. See the [M2 design](superpowers/specs/2026-11-10-milestone-2-design.md) §9. | DB × oracles |
| **RECON** | Milestone 2 (2.6): the campaign's closing `idem reconcile` reports no difference outside parked workflows, and exactly the differences between the oracle and the ledger for parked ones. | Reconciliation tables × oracle |

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
   - As built (slice 1.5): `make chaos CHAOS_ARGS="--seed N --duration 90s"`.
     - **Processes:** a throwaway Postgres container, the fake provider, **two API replicas** (since M2 slice 2.3; clients and webhooks rotate across them, restarts hit one at a time), 4 workers and 2 sweepers.
     - **M2 slice 2.3:** campaigns inject the six webhook faults (runs of a minute or more reserve a block of six back-to-back 8s windows, 5s plus slack for timing drift, one fault each, once traffic flows) and the `inbox_before_commit` and `webhook_after_insert` failpoints. In runs of a minute or more the verdict requires every webhook fault to have had a real window (effective, ≥5s on the executed timeline) and fired, every armed failpoint to have fired, and every replica to have served clients, accepted new webhooks and recognised duplicates.
     - **Supervision:** a supervisor restarts any process that crashes (modelling Kubernetes); the fake provider is never killed, because it is the oracle.
     - **Child timings:** short (4s lease, 1s provider timeout, 100ms–2s retries), so a short run exercises many recoveries.
     - **Clients misbehave:** polite, impatient, double-click, reformatted-retry and malformed. They must never see two workflows for one key.
     - **Settling:** after the campaign, faults are cleared and workers restart without failpoints, then liveness (I4) must hold within the settle timeout.
     - **Traffic:** arrivals are open-loop and spread over the whole campaign, so disruptions meet work in flight.
     - **Effective events:** each event records how much work was in flight when it fired (unsettled workflows plus client requests in progress); an event that met no work is not *effective*.
     - **A pass must not be vacuous:** it requires every disruption kind to have been effective (runs ≥ 60s), provider faults that actually fired (the fake provider counts hits), and failpoints that actually fired (failpoint exits are told apart from kills).
     - **Strict verdict:** events that fail to apply, accepted workflow IDs the database disagrees with, steps left `RUNNING`/`IN_DOUBT` (I4), and parking for any reason other than the pivot safety rule all fail the run.
     - **Artifacts:** each run writes `report.txt`, `report.json` and per-process logs to `chaos-runs/`.

**Order of work (failure-first):** the fake provider, failpoints, and invariant checker are built *alongside* the first step, not after the flow is complete (§15).

---

## 15. Milestone 1 delivery slices

Each slice ends with passing tests and a short write-up in [`docs/journal/`](journal/README.md) (entries up to slice 2.3b were written retrospectively on 2026-11-10).

| Slice | Delivers | Failure work in the same slice |
|---|---|---|
| **1.1** Skeleton | Repo, migrations, `docker compose` (Postgres), `POST /workflows` with inbound idempotency | F1–F3 tests |
| **1.2** One step, end to end | Claim/lease/fencing, `charge` against fake provider, ledger posting, fake provider + oracle, failpoint package, invariant checker v0 (I1, I2), **lease-expiry reclaim (pulled forward from 1.3)** | F4–F6, F7–F9 (fixed-delay retry), F10, F12, F13, F27 |
| **1.3** Ambiguity | Outcome classification (incl. no-effect 429 / refused connections, `Retry-After`, 409 in-flight), retries with full-jitter backoff, sticky `IN_DOUBT`, `DEAD`/`NEEDS_ATTENTION` with `dead_reason`, lookup near key retention, stuck-workflow reporting, bounded shutdown | F7–F9, F11, F22, F23, F25 |
| **1.4** Full flow | `payout` (synchronous, the pivot), `refund` compensation only on a *definite* payout failure, refund charge reference frozen in a typed column, CAS workflow transitions, fees, I3, I5, I7 | F20, F21 |
| **1.5** Chaos harness | `cmd/chaos`: real processes under a supervisor, seeded kill/pause/API and DB restart/provider-fault plan, exit failpoints, misbehaving clients, settle + verdict report; invariants I4 (liveness) and I6; pivot safety rule (a payout refused after an ambiguous attempt is parked, not refunded) | F4–F6 and F12–F14 at process level, F26; full invariant suite |
| **1.6** Operability | Audited admin actions (`list`, `retry-step`, `confirm-step`, `compensate`, `fail`, `new-generation`), Prometheus metrics on every process, Prometheus + alert rules in compose, runbook, chaos recovery counts | — (milestone 1 complete) |

### Observability (M1 minimum)
- **Structured logs** (`log/slog`, JSON) with `workflow_id`, `step`, `attempt`, `lease_epoch`, `worker_id` on every line.
- **Prometheus metrics** (as built). Labels never carry IDs, keys or error text.
  - **Workers:** `idem_step_attempts_total{step,outcome}` (including `fenced`), `idem_provider_request_duration_seconds{step,class}`, `idem_key_retention_lookups_total{step,found}`.
  - **Sweepers:** `idem_leases_reclaimed_total`, `idem_workflows{status}`, `idem_steps{step,status}`, `idem_unsettled_workflow_max_idle_seconds` (time without progress: a status change or a finished, non-abandoned step attempt. Age since creation paged as soon as an operator retried an old workflow; counting bare claims hid crash loops), `idem_steps_over_attempt_limit`, `idem_parked_workflows{reason}`.
  - **API:** `idem_workflow_submissions_total{result}`.
  - **Not yet:** `webhook_inbox_unprocessed` arrives with M2.
- **Ports:** the API serves `/metrics` on its own address; workers and sweepers on `IDEM_METRICS_ADDR` (default `:8090`, outside the 9100–9999 range Prometheus allocates to exporters).
- **Alerts:** `deploy/prometheus/alerts.yml`, each linking to the runbook.
- **Deferred to where they belong:**
  - *Sweeper snapshot cost* (whole-table counts every tick): measure with `cmd/loadgen` in M3, then index, sample or slow the interval with real numbers.
  - *`/metrics` authentication:* the endpoints reveal volumes, never IDs or secrets. Restrict them in M4 with a NetworkPolicy, or a separate internal-only port for the API.
- **Database pool:** each process exports pgx's pool statistics (`idem_db_pool_*`: connections in use, idle, maximum; acquisitions that waited and the total wait), so "is the pool a bottleneck?" is a graph, not a guess. Alert `IdemDBPoolSaturated`.
- **Profiling:** Go's `pprof` on the internal metrics port of workers and sweepers, and on the API's optional `IDEM_DEBUG_ADDR` (never on its public port).
- **Tracing → Milestone 3, alongside the load generator (D20).** Traces answer "where did the time go" for one payment, and only show bottlenecks under load. idem hands work off **through database rows**, not calls: the API commits a workflow that a worker claims later, and a webhook lands in the inbox that another worker applies later. So the W3C trace context (`traceparent`) is **stored in the rows that carry work**: `workflows`, `steps`, `webhook_inbox`, and the outbox. Whichever process continues the work starts a span linked to it, so one trace shows API → queue wait → claim → provider call → commit, and webhook → inbox wait → apply. Metrics carry exemplars linking latency buckets to traces.

### Later milestones (outline only)
- **M2** — **Nobody silently disagrees:** see [the M2 design](superpowers/specs/2026-11-10-milestone-2-design.md). Async payouts and polling (2.1), provider webhooks (2.2, 2.3), status + outbox + relay to shops (2.4, 2.5), reconciliation (2.6). Generic workflow definitions moved to M4.
- **M3** — Prove it at scale: long chaos campaigns, toxiproxy network faults, load tests, tracing (OpenTelemetry, trace context stored in work rows: D20), SLOs + alerts, a published chaos report.
  - **Traffic generator (`cmd/loadgen`)** — simulates 100–500 external clients against the public API only (no database or internal shortcuts while generating load):
    - **Realistic misbehaviour, not just happy paths:** clients that time out and retry with the same key; double submissions; impatient retries with a *new* key (a real client bug the system must survive); declined customers; malformed payloads.
    - **Open workload model:** requests arrive at a target rate whether or not earlier ones finished (avoids coordinated omission hiding latency).
    - **Runs with provider faults enabled**, and ends with a correctness verdict: every submitted workflow reached a terminal or attention state, and `idem check` reports zero invariant violations.
    - **Reports** request rate, latency percentiles and error classes; pairs with M3 metrics/tracing to locate bottlenecks (claim contention, pool exhaustion, sweeper cost, hot keys).
    - Runs against docker compose first; packaged as its own Helm release in M4 for minikube. Numbers on a laptop are relative ("this change made claims 3x slower"), not capacity claims.
- **M4** — Kubernetes operator (kubebuilder, developed on minikube): `WorkflowDefinition`, `SchedulerCluster` (workers, migrations, scaling on queue depth, safe upgrades), `DeadLetterPolicy`. Workflow *instances* stay in Postgres, never in etcd.
- **M5 (stretch)** — etcd-backed lease implementation behind the same interface, compared under partitions; deterministic simulation testing; sharding.

---

## 16. Decisions

Each decision has an architecture decision record in [`docs/adr/`](adr/README.md) with its context, consequences and status. The table is the summary.

| ID | Decision | Why | Alternative rejected |
|---|---|---|---|
| [D1](adr/0001-postgres-single-state-store.md) | Postgres as the single state store | Core invariants need multi-row atomic transactions and unique constraints; matches real payment systems | Distributed KV (etcd/Redis): would require rebuilding transactions; moves the project away from the payments problem |
| [D2](adr/0002-ledger-posting-in-success-transaction.md) | Ledger posting inside the external step's success transaction | Removes the "provider succeeded, ledger not updated" window | Separate ledger step: adds a crash window and a step to recover |
| [D3](adr/0003-lease-timing-from-database-clock.md) | All lease timing from DB `now()` | Immune to worker clock skew | Worker-side timestamps |
| [D4](adr/0004-dead-letter-is-a-status.md) | Dead-letter = `DEAD` status, not a separate table | One source of truth; no move-between-tables crash window | DLQ table |
| [D5](adr/0005-ambiguous-outcomes-are-in-doubt.md) | Ambiguous outcomes → `IN_DOUBT`, never `FAILED` | Assuming failure is how double payments happen | Treat timeouts as failures |
| [D6](adr/0006-outbound-keys-stored-at-step-creation.md) | Outbound keys stored at step creation; new keys only by audited admin action | Prevents silent double payment from code changes or bugs | Recompute key on each attempt |
| [D7](adr/0007-every-ownership-change-bumps-lease-epoch.md) | Every ownership change bumps `lease_epoch` | One fencing rule with no special cases | Bump only on claim |
| [D8](adr/0008-hand-written-sql-with-sqlc.md) | Hand-written SQL, `sqlc`-generated Go on `pgx/v5`, no ORM | Learning goal; locking clauses stay visible; type safety without row-scanning boilerplate | ORM (GORM, ent): hides transactions and locking — the very things this project is about |
| [D9](adr/0009-sync-payouts-first.md) | Payouts synchronous in M1; async + webhooks first thing in M2 | Reach a complete, chaos-tested flow sooner; M1 schema/state machine already leave room | Async in M1 |
| [D10](adr/0010-single-currency.md) | Single currency (EUR) in M1, `currency` column kept | Simplicity now, cheap to extend later | Multi-currency from day one |
| [D11](adr/0011-platform-fees-in-integer-cents.md) | Platform fees kept; integer cents, half-to-even rounding, fixed at workflow creation | Realism; fee rounding is a classic source of ledger drift | No fees |
| [D12](adr/0012-workers-poll-waiting-payouts.md) | (M2) Workers poll waiting payouts, not the sweeper | Workers already own every provider call, leases and fencing; the sweeper stays database-only | Sweeper polls |
| [D13](adr/0013-outbox-written-in-transition.md) | (M2, built 2.4) The outbox is written inside `transitionWorkflow`, in the transaction of the terminal transition | Every path that ends a workflow emits exactly one event, and none can forget | Publish from each code path (dual write) |
| [D14](adr/0014-at-least-once-outbound-delivery.md) | (M2, built 2.5) Outbound delivery is at least once, with a stable event ID the shop deduplicates on | Exactly-once over HTTP is impossible without the receiver; this is how real providers do it | At most once; claiming exactly once |
| [D15](adr/0015-reconciliation-only-reports.md) | (M2, built 2.6) Reconciliation only reports | When ledger and provider disagree, idem cannot know which is right; humans resolve with evidence | Auto-correct the ledger |
| [D16](adr/0016-contradictory-news-never-flips-terminal-state.md) | (M2) Contradictory provider news never flips a terminal state; it is re-checked with the provider, and only a real disagreement pages | Late and reordered webhooks are normal; flipping on them is how money moves twice | Last event wins |
| [D17](adr/0017-polls-never-create-doubt.md) | (M2) Polls are attempts of kind `poll` and never create doubt; the pivot rule reads the history of send attempts | A `GET` cannot move money; counting a crashed poll as doubt would park clean failures | Treat every attempt alike |
| [D18](adr/0018-news-about-parked-steps-is-evidence.md) | (M2) Provider news about a parked step is evidence for a human, never an automatic resolution | Money decisions made under doubt end with a human, who sees the answer | Auto-resolve parked steps |
| [D19](adr/0019-outbox-rows-for-every-client.md) | (M2, built 2.5) Outbox rows are written for every client; without a webhook URL they are marked `NO_ENDPOINT`, not delivered and not alerted | No special case in I8; polling `GET /workflows/{id}` is a legitimate integration | Skip those rows; mark them delivered |
| [D20](adr/0020-trace-context-stored-in-work-rows.md) | (M3) Tracing with the trace context stored in the rows that hand work between processes | Work moves through database rows, not RPC calls, so headers alone cannot carry a trace; storing it keeps one payment one trace | Trace each process separately; header-only propagation |
| [D21](adr/0021-append-only-by-privilege-logins-are-infrastructure.md) | (2.3b) The ledger and audit trail are append-only by privilege (role `idem_app`); database logins are infrastructure, never created by idem | The owner could drop the triggers; an application should not manage credentials | Triggers only; `idem migrate` creates the login |
| [D22](adr/0022-toolchain-pinned-in-go-mod.md) | The Go toolchain is pinned in `go.mod` and kept on a supported release; govulncheck runs in CI | Go 1.25 had left support with reachable, unpatched vulnerabilities; one version everywhere | Whatever Go is installed locally |
| [D23](adr/0023-state-transitions-enforced-by-the-database.md) | (2.5b) Workflow and step transitions are enforced by the database; terminal and active defined once in SQL | Moves lived only in Go call sites and the docs had drifted; an illegal move now fails loudly | Checks in Go only; a state-machine library |

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
- Go: **1.25** (1.25.6 installed). Moved to 1.27 on 2026-11-10 (§ stack table).
- Local runtime: **docker compose** for M1, **minikube** for M4.

---

## 18. Glossary

Moved to [`docs/glossary.md`](glossary.md), which defines every term used here (workflow, step, ledger, idempotency key, lease, outbox, webhook, invariant, …) in plain language, grouped by area, with where each lives in the code.

## 19. References
- Brandur Leach — *Implementing Stripe-like Idempotency Keys in Postgres*
- Stripe API docs — *Idempotent requests*
- Martin Kleppmann — *How to do distributed locking* (fencing tokens)
- Martin Kleppmann — *Designing Data-Intensive Applications*, ch. 7–9
- Pat Helland — *Life beyond Distributed Transactions*
- Garcia-Molina & Salem — *Sagas* (1987)
- Temporal / Restate / DBOS docs — durable execution concepts
- Jepsen analyses — how invariant-checking under faults is done in practice
