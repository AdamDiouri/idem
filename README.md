# idem

An effectively-once payments workflow engine: **charge → ledger → payout**, designed failure-first.
Work may be attempted more than once; its effects happen at most once.

See [`docs/design.md`](docs/design.md) for the design, guarantees, fault model and failure-mode table.

**Status:** Milestone 1 complete. The full saga (charge, payout, or refund on a definite payout failure) runs with leases, fencing, retries, dead letters and invariant checks. A chaos harness proves it under crashes and faults, and operators get audited admin actions, Prometheus metrics, alerts and a runbook. Next: milestone 2 (async payouts with webhooks, generic workflow definitions).

## Prerequisites

- Go 1.25 (dependencies are pinned to versions that support it; the Makefile sets `GOTOOLCHAIN=local`)
- Docker (for `docker compose` and for the integration tests, which run a real Postgres 18)
- [`golangci-lint`](https://golangci-lint.run/) v2 for `make lint`

## Run it

```bash
make up   # postgres → migrate → api (:8080), worker, sweeper, fakeprovider (:9090)
```

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
  -H 'Authorization: Bearer demo-token-0123456789' \
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

Magic test IDs in the fake provider: customers starting with `cus_decline` are declined; merchants starting with `mer_invalid` have an invalid account, so the payout fails and the customer is refunded. Watch a compensation:

```bash
curl -s -X POST localhost:8080/workflows -H 'Authorization: Bearer demo-token-0123456789' \
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

Failpoints crash or pause a worker at a named point (`after_claim`, `before_provider_call`, `after_provider_call`, `before_commit`, `after_commit`), e.g. `IDEM_FAILPOINTS=after_provider_call=exit@0.2` on the worker.

```bash
make down
```

## Operate it

When idem cannot finish a workflow without guessing about money, it parks it in `NEEDS_ATTENTION` and alerts. The [runbook](docs/runbooks/needs-attention.md) explains every parking reason and which command resolves it.

```bash
export IDEM_DATABASE_URL=postgres://idem:idem@localhost:5432/idem?sslmode=disable
idem admin list                                              # what is parked, and why
idem admin retry-step <step-id> --reason "provider recovered"  # same key: always safe
idem admin confirm-step <step-id> --provider-ref po_… --reason "provider shows the payout"
idem admin compensate <workflow-id> --reason "merchant not paid; refund customer" [--confirm-no-effect]
idem admin fail <workflow-id> --reason "customer was never charged" [--confirm-no-effect]
idem admin new-generation <step-id> --reason … --confirm-no-effect   # new key: verify first
```

(With compose: `docker compose run --rm --entrypoint /idem worker admin list`.)

Every change needs `--reason`, refuses anything that isn't parked, and is recorded in `admin_actions`. `compensate` and `fail` also need `--confirm-no-effect` when money may have moved. That's because:
- refunding a customer whose merchant was paid pays out twice
- failing a charge that happened loses the customer's money

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
- **Infrastructure:** a throwaway Postgres container, the fake provider, the API, 4 workers and 2 sweepers.
- **Traffic:** about 300 workflows from misbehaving clients (impatient retries, double-clicks, reformatted retries, malformed bodies).
- **Disruptions:** a seeded plan of `kill -9`, `SIGSTOP` beyond the lease, API and database restarts, and provider faults. Workers also carry crash failpoints that make them `exit` mid-step, and a supervisor restarts them like Kubernetes would.

Afterwards it clears every fault, lets the system settle, and checks every invariant.

```bash
make chaos CHAOS_ARGS="--seed 1 --duration 90s"   # needs Docker
```

Example result (seed 1, 90s, 1000 workflows):

```
chaos run seed 1: PASSED
events executed: 33 (failed to apply: 0, skipped: 1); effective = met work in flight
  kill_worker 16 (effective 16)   pause_worker 4 (4)   restart_api 1 (1)
  restart_postgres 5 (5)          set_provider_faults 6 (6)   clear_provider_faults 1 (1)
provider fault hits: 59 across charges/payouts/refunds
failpoint exits: 99
clients: submitted 1000, accepted 884, malformed rejected 116, retried attempts 178
outcomes: COMPENSATED=74 FAILED=66 NEEDS_ATTENTION=5 SUCCEEDED=739
settled in: 3.619s
violations: 0
```

**How to read the result:**
- **`PASSED` requires all of:**
  - zero invariant violations (I1–I7)
  - every workflow terminal or parked, with no step left `RUNNING`/`IN_DOUBT`
  - no idempotency key that ever returned two different workflows
  - every workflow a client was told about exists in the database under the same ID
  - no event that failed to apply
  - parking only for expected reasons
- **A pass must be earned, not vacuous:**
  - every kind of disruption must have met work in flight (an *effective* event)
  - provider faults must actually have hit requests
  - armed failpoints must actually have fired
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
| `IDEM_DATABASE_URL` | (required) | Postgres connection URL |
| `IDEM_API_TOKENS` | (required for `api`) | `client=token,...`; tokens at least 16 characters, unique |
| `IDEM_HTTP_ADDR` | `:8080` | |
| `IDEM_IDEMPOTENCY_LOCK_TIMEOUT` | `5s` | Max wait on a concurrent duplicate request before `409`; must be below the request timeout |
| `IDEM_REQUEST_TIMEOUT` | `10s` | Deadline for all work in one request before `503 request_timeout`; must be below the write timeout |
| `IDEM_HTTP_WRITE_TIMEOUT` | `15s` | |
| `IDEM_FEE_RATE_BPS` | `150` | Fee rate in basis points (0 to 10000) |
| `IDEM_FEE_FIXED_CENTS` | `25` | Fixed fee in cents |
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
| `IDEM_SHUTDOWN_GRACE` | `15s` | How long an in-flight step may finish after SIGTERM; at least provider timeout + 2s |
| `IDEM_STUCK_AFTER` | `15m` | Sweeper logs non-terminal workflows unchanged for this long |
| `IDEM_SWEEP_INTERVAL` | `5s` | How often the sweeper reclaims expired leases |
| `IDEM_WORKER_ID` | `hostname-pid` | |
| `IDEM_METRICS_ADDR` | `:8090` | Where worker/sweeper serve `/metrics`; `off` disables |
| `IDEM_FAILPOINTS` | (none) | `name=exit\|panic\|sleep:<d>[@probability],...` |
