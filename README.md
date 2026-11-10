# idem

An effectively-once payments workflow engine: **charge → ledger → payout**, designed failure-first.
Work may be attempted more than once; its effects happen at most once.

See [`docs/design.md`](docs/design.md) for the design, guarantees, fault model and failure-mode table. Also:
- [decision records](docs/adr/README.md): why each design choice was made, and what was rejected;
- [journal](docs/journal/README.md): one write-up per delivery slice, covering what shipped and what broke along the way.

**Status:** Milestone 1 complete. The full saga (charge, payout, or refund on a definite payout failure) runs with leases, fencing, retries, dead letters and invariant checks. A chaos harness proves it under crashes and faults, and operators get audited admin actions, Prometheus metrics, alerts and a runbook. Milestone 2 is in progress ([design](docs/superpowers/specs/2026-11-10-milestone-2-design.md)). Done so far: asynchronous payouts with polling (2.1), signed provider webhooks (2.2), chaos for the async path (2.3), and hardening after an external review (2.3b).

## Prerequisites

- Go 1.21 or later to start with: `go.mod` pins the exact toolchain (Go 1.27.4), which the go command downloads if it is not installed
- Docker (for `docker compose` and for the integration tests, which run a real Postgres 18)
- [`golangci-lint`](https://golangci-lint.run/) v2 for `make lint`

## Run it

```bash
make up   # postgres → migrate → api (:8080), worker, sweeper, fakeprovider (:9090)
```

**Local settings and secrets** live in `.env`, which is never committed. The first `make up` creates it from [`.env.example`](.env.example); edit it to change the Postgres password, the demo API token or the webhook secret. Compose refuses to start if a secret is missing. Load it into your shell to use the examples below:

```bash
set -a; . ./.env; set +a     # exports IDEM_DEMO_TOKEN, POSTGRES_PASSWORD, ...
```

After changing `POSTGRES_PASSWORD`, run `make reset` (it deletes the local database) before `make up`: Postgres only applies the password when it first creates its volume.

| Service | Command | Role |
|---|---|---|
| `api` | `idem api` | Accepts workflows idempotently |
| `worker` | `idem worker` | Claims steps under a lease, calls the provider, posts the ledger |
| `sweeper` | `idem sweeper` | Moves steps with expired leases to `IN_DOUBT` (crashed/paused workers); exports state gauges |
| `prometheus` | `prom/prometheus` | Scrapes every process; evaluates the alert rules (<http://localhost:9091>) |
| `fakeprovider` | `/fakeprovider` | Stripe-like provider with fault injection and an effects oracle |

Submit a workflow:

```bash
curl -i -X POST localhost:8080/workflows \
  -H "Authorization: Bearer $IDEM_DEMO_TOKEN" \
  -H 'Idempotency-Key: order-42' \
  -d '{"amount_cents":10000,"currency":"EUR","customer_id":"cus_1","merchant_id":"mer_1"}'
```

- First call: `201 Created` with the workflow (fee `175` = 1.5% + €0.25).
- Same call again: `201`, identical body, header `Idempotent-Replayed: true`.
- Same key, different body: `422 idempotency_key_reused`.

Health: `GET /healthz` (process alive), `GET /readyz` (database reachable, else `503`).

A few seconds later the workflow has run its saga:

```
charge ──ok──▶ payout ──ok──▶ SUCCEEDED
  │ declined      │ definitively refused
  ▼               ▼
FAILED          refund ──ok──▶ COMPENSATED
                  │ refused / exhausted
                  ▼
           NEEDS_ATTENTION   (also: any step DEAD, e.g. an ambiguous payout — never auto-refunded)
```

Magic test IDs in the fake provider: customers starting with `cus_decline` are declined; merchants starting with `mer_invalid` have an invalid account, so the payout fails and the customer is refunded.

Payouts are **asynchronous**, like real ones. The provider answers `202 pending`, and the payout settles `FAKEPROVIDER_PAYOUT_SETTLE` later (default `2s`). The payout step waits in `AWAITING_CALLBACK`, and workers poll `GET /payouts/{id}` after `IDEM_CALLBACK_DEADLINE`. The `payout_fail` fault makes an accepted payout settle `failed`. With `FAKEPROVIDER_WEBHOOK_URL` and `FAKEPROVIDER_WEBHOOK_SECRET`, the fake provider also announces each settled payout by **signed webhook**; idem's API verifies it (`IDEM_PROVIDER_WEBHOOK_SECRETS`: current secret first, then the previous one while rotating; the endpoint is off without one), stores it in an inbox, and workers apply it. Compose wires this up, so a payout completes about 2 seconds after it is accepted.

`FAKEPROVIDER_HANG` (default `30s`) sets how long timeout faults hang; chaos uses `3s`.

**Rotating the webhook secret without losing events:** set `IDEM_PROVIDER_WEBHOOK_SECRETS=<new>,<old>` on the API and restart it (both verify), switch the provider to the new secret, and once no `bad_signature` failures remain, drop the old one.

Watch a compensation:

```bash
curl -s -X POST localhost:8080/workflows -H "Authorization: Bearer $IDEM_DEMO_TOKEN" \
  -H 'Idempotency-Key: comp-1' -d '{"amount_cents":10000,"currency":"EUR","customer_id":"cus_1","merchant_id":"mer_invalid_1"}'
docker compose exec postgres psql -U idem -c "select name, status from steps order by created_at"
# charge SUCCEEDED, payout FAILED_PERMANENT, refund SUCCEEDED; workflow COMPENSATED; its ledger nets to zero
```

Check the books at any time:

```bash
docker compose exec postgres psql -U idem -c "select name, status from steps"   # charge SUCCEEDED, payout PENDING
curl -s localhost:9090/_effects                                                    # one charge
docker compose run --rm --entrypoint /idem worker check                            # 0 violations
```

### Break it on purpose

Make the provider take the money and then hang, so the worker's call times out (the classic double-charge trap):

```bash
curl -X PUT localhost:9090/_faults \
  -d '{"rules":[{"endpoint":"charges","mode":"timeout_after_effect","times":1}]}'
```

Submit another workflow and wait ~15s. Its charge step shows attempt 1 `IN_DOUBT` (timeout) and attempt 2 `SUCCEEDED`: the retry reused the idempotency key, the provider replayed the original response, and `idem check` still reports one charge per workflow. Other modes: `timeout_before_effect`, `error_before_effect`, `error_after_effect`, `rate_limit` (429, provably no effect), `decline`. Customers whose ID starts with `cus_decline` are always declined.

Retries back off exponentially with jitter. When a step runs out of attempts it becomes `DEAD` (the dead-letter state), its workflow becomes `NEEDS_ATTENTION`, and the worker logs an ERROR saying whether money may have moved. To see it quickly, make every charge fail and lower a fresh step's attempt limit:

```bash
curl -X PUT localhost:9090/_faults -d '{"rules":[{"endpoint":"charges","mode":"error_before_effect"}]}'
# submit a workflow, then:
docker compose exec postgres psql -U idem -c "update steps set max_attempts = 2 where status in ('PENDING','IN_DOUBT')"
docker compose exec postgres psql -U idem -c "select name, status, dead_reason from steps"   # charge DEAD, attempts_exhausted_in_doubt
```

A `500` is ambiguous, so the reason says money may have moved. With `rate_limit` it would be `attempts_exhausted_no_effect`. The sweeper also logs `workflow stuck` for workflows that have not progressed for `IDEM_STUCK_AFTER`.

Failpoints crash or pause a process at a named point, e.g. `IDEM_FAILPOINTS=after_provider_call=exit@0.2` on the worker:
- **worker:** `after_claim`, `before_provider_call`, `after_provider_call`, `before_commit`, `after_commit`, and `inbox_before_commit` (applying a provider webhook)
- **api:** `webhook_after_insert` (a webhook stored but not yet acknowledged)

```bash
make down
```

## Operate it

When idem cannot finish a workflow without guessing about money, it parks it in `NEEDS_ATTENTION` and alerts. The [runbook](docs/runbooks/needs-attention.md) explains every parking reason and which command resolves it.

```bash
export IDEM_DATABASE_URL="postgres://idem_app_user:${IDEM_APP_DB_PASSWORD}@localhost:5432/idem?sslmode=disable"   # after loading .env
idem admin list                       # what is parked, why, and its VERSION
# every action below also takes --expect-version <VERSION from list>
idem admin retry-step <step-id> --reason "provider recovered"  # same key: always safe
idem admin confirm-step <step-id> --provider-ref po_… --reason "provider shows the payout"
idem admin compensate <workflow-id> --reason "merchant not paid; refund customer" [--confirm-no-effect]
idem admin fail <workflow-id> --reason "customer was never charged" [--confirm-no-effect]
idem admin new-generation <step-id> --reason … --confirm-no-effect   # new key: verify first
```

(With compose: `docker compose run --rm --entrypoint /idem worker admin list`.)

Every change needs `--reason` and `--expect-version` (the `VERSION` column of `list`), refuses anything that isn't parked or changed since you listed it, and is recorded in `admin_actions`. `compensate` and `fail` also need `--confirm-no-effect` when money may have moved. That's because:
- refunding a customer whose merchant was paid pays out twice
- failing a charge that happened loses the customer's money

**Database pool:** every process exports its connection pool (`idem_db_pool_*`): connections in use, idle and maximum, and how often and how long requests waited for one. `IdemDBPoolSaturated` fires on sustained waiting.

**Profiling (pprof):** workers and sweepers serve Go's profiler next to `/metrics` on their internal port; compose publishes them on localhost only (worker `:8090`, sweeper `:8091`). The API serves it only on `IDEM_DEBUG_ADDR`, never on its public port.

```bash
go tool pprof -http=:0 'http://localhost:8090/debug/pprof/profile?seconds=10'   # worker CPU for 10s
go tool pprof -http=:0 http://localhost:8090/debug/pprof/heap                    # worker memory
curl -s 'http://localhost:8090/debug/pprof/goroutine?debug=1' | head            # what every goroutine is doing
```

**Metrics:**
- `/metrics` on the API (`:8080`) and on workers and sweepers (`IDEM_METRICS_ADDR`, default `:8090`).
- Prometheus in compose: <http://localhost:9091>.
- Alert rules: [`deploy/prometheus/alerts.yml`](deploy/prometheus/alerts.yml):
  - `IdemWorkflowsNeedAttention`
  - `IdemMoneyMayHaveMoved`
  - `IdemWorkflowsStuck`
  - `IdemStepsOverAttemptLimit`
  - `IdemFencedCommitsSpike`
  - `IdemProviderSlow`
  - `IdemSweeperDown`
  - `IdemSweeperSnapshotStale` (every state alert depends on a live sweeper)

## Run a chaos campaign

The chaos harness runs the real binaries as separate processes and does its best to break them:
- **Infrastructure:** a throwaway Postgres container, the fake provider, **two API replicas** (`--apis`), 4 workers and 2 sweepers.
- **Traffic:** misbehaving clients (impatient retries, double-clicks, reformatted retries, malformed bodies). Every request goes to the next replica in turn, so one payment's retries usually reach both.
- **Webhooks:** the provider signs a webhook for every settled payout and rotates deliveries across the replicas, so its retries reach the *other* replica.
- **Disruptions:** a seeded plan of `kill -9`, `SIGSTOP` beyond the lease, API restarts (one replica at a time), **`restart_all`** (every worker, sweeper and API replica gets `SIGTERM` at once and drains: a rolling deploy done badly, F26), database restarts, provider faults, and the six **webhook faults** (duplicated, sent before the `202`, delayed, a stale contradiction, dropped, badly signed). In runs of a minute or more, the plan reserves a **webhook-fault block**: six back-to-back 8-second windows (5 seconds plus slack for timing drift), one fault each and nothing else, starting once traffic flows.
- **Crash failpoints** make processes `exit` mid-step, including `inbox_before_commit` on workers and `webhook_after_insert` on the API. A supervisor restarts them like Kubernetes would.
- **Least privilege:** migrations run as the database owner; every idem process logs in as the restricted application user, so a missing grant fails the campaign.

Afterwards it clears every fault and lets the system settle: every workflow terminal or parked, and every webhook the provider queued delivered or given up. Then it checks every invariant.

```bash
make chaos CHAOS_ARGS="--seed 1 --duration 90s"   # needs Docker
```

Example result (seed 1, 90s, 1000 workflows):

```
chaos run seed 1: PASSED
events executed: 33 (failed to apply: 0, skipped: 1); effective = met work in flight
  kill_worker 16 (effective 16)   pause_worker 4 (4)   restart_api 1 (1)
  restart_postgres 5 (5)          set_provider_faults 6 (6)   clear_provider_faults 1 (1)
provider fault hits: … webhooks/bad_signature:22 webhooks/delay_webhook:17 webhooks/drop_webhook:2 …
api replicas: 2 (requests api-0=… api-1=…; webhooks accepted api-0=… api-1=…, duplicates api-0=… api-1=…)
failpoints fired: after_claim=22 after_provider_call=35 before_commit=40 inbox_before_commit=19 webhook_after_insert=25
webhooks: … applied, … duplicates, … deferred, … stale delivered (… ignored as stale), 0 in flight at the verdict
outcomes: COMPENSATED=123 FAILED=44 NEEDS_ATTENTION=7 SUCCEEDED=710
violations: 0
```

**How to read the result:**
- **`PASSED` requires all of:**
  - zero invariant violations (I1–I11). The webhook inbox is checked event by event against the provider's delivery log: every acknowledged event stored, none accepted on a forged signature alone, none the provider never sent, an "applied" event leaving a webhook attempt on its step, true events processed, and stale ones ignored as stale (or, on a payout still waiting, applied with what the provider says on a re-check, never with their own claim). Any webhook `CONFLICT` is a violation, since the fake provider never truly contradicts itself
  - every workflow terminal or parked, with no step left `RUNNING`/`IN_DOUBT`
  - no idempotency key that ever returned two different workflows
  - every workflow a client was told about exists in the database under the same ID
  - no event that failed to apply, and no webhook delivery still in flight at the verdict
  - parking only for expected reasons
- **A pass must be earned, not vacuous:**
  - every kind of disruption must have met work in flight (an *effective* event)
  - provider faults must actually have hit requests
  - webhooks must actually have completed payouts (polling completes every payout even when webhooks are broken), and, in runs of a minute or more, the provider must have delivered reordered (stale) events (I10 judges how each was handled)
  - every webhook fault that was in force for at least 5 seconds while work was in flight (measured on the executed timeline) must actually have fired; in runs of a minute or more, **every** webhook fault must have had such a window and fired
  - every armed failpoint must actually have fired (failpoints name themselves when they do)
  - with several API replicas, every replica must have answered clients and accepted new webhooks, and, in runs of a minute or more, recognised duplicates (evidence of deduplication across replicas)
  - a binary built with `-tags nofailpoint` refuses `IDEM_FAILPOINTS` instead of running silently
- **Skipped events:** a kill or pause aimed at a worker that was already restarting is *skipped*. That is harmless, and it is neither counted nor treated as a failure.
- **`NEEDS_ATTENTION`** entries list the step and reason. In a healthy run they are all `payout permanent_after_in_doubt`: payouts refused after an ambiguous attempt, parked for a human by the pivot safety rule instead of being refunded on a guess.
- **Exit codes:** `0` passed, `1` failed, `2` could not run or was interrupted.
- **Artifacts:** `chaos-runs/<time>-seed<N>/` holds `report.txt`, `report.json` (every event with its in-flight count) and every process's log.
- **Reproducing a run:** `bin/chaos --seed N …` (printed in the report) replays the same fault plan. OS scheduling still differs between runs.
- **Cleanup:** Ctrl-C stops a campaign at any point and still removes its container, its data volume and its processes. Container names are unique per run, so concurrent campaigns don't collide.

## Develop

```bash
make test       # go test -race ./... (starts Postgres containers)
make lint
make generate        # regenerate sqlc code after editing internal/store/queries/*.sql
make build-release   # binary with failpoints compiled out
go test -tags chaos ./internal/chaos/   # 30s chaos campaign as a test (after make build)
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `IDEM_DATABASE_URL` | (required) | Postgres connection URL. For `migrate`: the table owner. For everything else: a login in the restricted `idem_app` role, which cannot rewrite the ledger. idem never creates logins: after `idem migrate`, run `CREATE ROLE idem_app_user LOGIN PASSWORD '…' IN ROLE idem_app` (compose does it for you) |
| `IDEM_API_TOKENS` | (required for `api`) | `client=token,...`; tokens at least 16 characters, unique |
| `IDEM_HTTP_ADDR` | `:8080` | |
| `IDEM_IDEMPOTENCY_LOCK_TIMEOUT` | `5s` | Max wait on a concurrent duplicate request before `409`; must be below the request timeout |
| `IDEM_REQUEST_TIMEOUT` | `10s` | Deadline for all work in one request before `503 request_timeout`; must be below the write timeout |
| `IDEM_HTTP_WRITE_TIMEOUT` | `15s` | |
| `IDEM_SHUTDOWN_GRACE` (API) | `20s` | How long the API drains in-flight requests after SIGTERM; must be greater than the write timeout |
| `IDEM_FEE_RATE_BPS` | `150` | Fee rate in basis points (0 to 10000) |
| `IDEM_FEE_FIXED_CENTS` | `25` | Fixed fee in cents |
| `IDEM_PROVIDER_WEBHOOK_SECRETS` | (none: endpoint off) | Secrets that verify `POST /webhooks/provider`: the current one first, then the previous one while rotating; each at least 16 characters, spaces trimmed |
| `IDEM_FAILPOINTS` | (none) | On the API: `webhook_after_insert=…` (see failpoints above) |
| `IDEM_DEBUG_ADDR` | `off` | On the API: serves Go profiling (`/debug/pprof/`) on this internal-only address; never on the public port |
| `IDEM_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

Worker, sweeper and `check` (no API tokens needed):

| Variable | Default | Notes |
|---|---|---|
| `IDEM_PROVIDER_URL` | (required) | e.g. `http://fakeprovider:9090` |
| `IDEM_PROVIDER_TIMEOUT` | `10s` | Provider call timeout; timeout + 2s must be below the lease TTL |
| `IDEM_LEASE_TTL` | `30s` | How long a worker owns a claimed step |
| `IDEM_POLL_INTERVAL` | `500ms` | Idle wait between claims |
| `IDEM_RETRY_BASE` | `2s` | First retry delay upper bound; doubles per attempt (full jitter) |
| `IDEM_RETRY_CAP` | `5m` | Maximum retry delay; also caps a provider's `Retry-After` |
| `IDEM_PROVIDER_KEY_RETENTION` | `24h` | How long the provider remembers idempotency keys |
| `IDEM_KEY_RETENTION_MARGIN` | `1h` | In-doubt steps older than retention − margin are resolved by lookup, never by a new key |
| `IDEM_SHUTDOWN_GRACE` (worker) | `15s` | How long an in-flight step may finish after SIGTERM; at least provider timeout + 2s |
| `IDEM_STUCK_AFTER` | `15m` | Sweeper logs non-terminal workflows unchanged for this long |
| `IDEM_SWEEP_INTERVAL` | `5s` | How often the sweeper reclaims expired leases |
| `IDEM_CALLBACK_DEADLINE` | `10m` | How long an accepted payout waits for its result before workers poll it |
| `IDEM_CALLBACK_POLL_CAP` | `1h` | Polls back off from `IDEM_RETRY_BASE` up to this; at least `IDEM_RETRY_BASE` |
| `IDEM_CALLBACK_MAX_WAIT` | `72h` | Total wait since the provider accepted a payout before it is `callback_unresolved` (bank payouts take hours or days); longer than the deadline |
| `IDEM_CALLBACK_MAX_POLLS` | `0` | Optional limit on polls per acceptance; `0` means none |
| `IDEM_HOOK_MAX_FAILURES` | `3` | After this many failures of a step's bookkeeping hook (ledger posting, saga move), the step is `DEAD` with `hook_failed` (see the runbook) |
| `IDEM_INBOX_ORPHAN_AFTER` | `1h` | A provider webhook that matches no payout this long is ignored as an orphan |
| `IDEM_INBOX_RETENTION` | `720h` | The sweeper deletes processed and ignored provider events older than this; evidence and conflicts are kept |
| `IDEM_WORKER_ID` | `hostname-pid` | |
| `IDEM_METRICS_ADDR` | `:8090` | Where worker/sweeper serve `/metrics`; `off` disables |
| `IDEM_FAILPOINTS` | (none) | `name=exit\|panic\|sleep:<d>[@probability],...` |
