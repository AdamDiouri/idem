# idem

An effectively-once payments workflow engine: **charge → ledger → payout**, designed failure-first.
Work may be attempted more than once; its effects happen at most once.

See [`docs/design.md`](docs/design.md) for the design, guarantees, fault model and failure-mode table.

**Status:** Milestone 1, slice 1.1: idempotent workflow submission. Workers, the fake provider and the chaos harness come next.

## Prerequisites

- Go 1.25 (dependencies are pinned to versions that support it; the Makefile sets `GOTOOLCHAIN=local`)
- Docker (for `docker compose` and for the integration tests, which run a real Postgres 18)
- [`golangci-lint`](https://golangci-lint.run/) v2 for `make lint`

## Run it

```bash
make up   # postgres → migrate → api on :8080
```

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

```bash
make down
```

## Develop

```bash
make test       # go test -race ./... (starts Postgres containers)
make lint
make generate   # regenerate sqlc code after editing internal/store/queries/*.sql
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
