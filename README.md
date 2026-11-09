# idem

An effectively-once payments workflow engine: **charge → ledger → payout**, designed failure-first.
Work may be attempted more than once; its effects happen at most once.

See [`docs/design.md`](docs/design.md) for the design, guarantees, fault model and failure-mode table.

**Status:** Milestone 1, slice 1.2: idempotent submission, plus workers that run the `charge` step end to end against a fake payment provider. Crashes and pauses at every step boundary are tested to never charge twice. Payout, retries with backoff and the chaos harness come next.

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
| `sweeper` | `idem sweeper` | Moves steps with expired leases to `IN_DOUBT` (crashed/paused workers) |
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

A second later the worker has charged the customer and posted the ledger:

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

Submit another workflow and wait ~15s. Its charge step shows attempt 1 `IN_DOUBT` (timeout) and attempt 2 `SUCCEEDED`: the retry reused the idempotency key, the provider replayed the original response, and `idem check` still reports one charge per workflow. Other modes: `timeout_before_effect`, `error_before_effect`, `error_after_effect`, `decline`. Customers whose ID starts with `cus_decline` are always declined.

Failpoints crash or pause a worker at a named point (`after_claim`, `before_provider_call`, `after_provider_call`, `before_commit`, `after_commit`), e.g. `IDEM_FAILPOINTS=after_provider_call=exit@0.2` on the worker.

```bash
make down
```

## Develop

```bash
make test       # go test -race ./... (starts Postgres containers)
make lint
make generate        # regenerate sqlc code after editing internal/store/queries/*.sql
make build-release   # binary with failpoints compiled out
```

If you created the compose volume before slice 1.2, reset it with `docker compose down -v`.

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
| `IDEM_RETRY_DELAY` | `2s` | Delay before retrying an `IN_DOUBT` step (fixed until slice 1.3 adds backoff) |
| `IDEM_SWEEP_INTERVAL` | `5s` | How often the sweeper reclaims expired leases |
| `IDEM_WORKER_ID` | `hostname-pid` | |
| `IDEM_FAILPOINTS` | (none) | `name=exit\|panic\|sleep:<d>[@probability],...` |
