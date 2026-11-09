-- +goose Up
CREATE TABLE workflows (
  id          uuid        PRIMARY KEY,
  client_id   text        NOT NULL,
  type        text        NOT NULL CHECK (type IN ('payment_payout')),
  status      text        NOT NULL CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED',
                                                     'COMPENSATING','COMPENSATED','NEEDS_ATTENTION')),
  input       jsonb       NOT NULL,
  version     bigint      NOT NULL DEFAULT 0,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE steps (
  id                 uuid        PRIMARY KEY,
  workflow_id        uuid        NOT NULL REFERENCES workflows(id),
  name               text        NOT NULL CHECK (name IN ('charge','payout','refund')),
  generation         int         NOT NULL DEFAULT 0 CHECK (generation >= 0),
  status             text        NOT NULL CHECK (status IN ('PENDING','RUNNING','IN_DOUBT','AWAITING_CALLBACK',
                                                            'SUCCEEDED','FAILED_PERMANENT','DEAD')),
  prior_status       text,
  idempotency_key    text        NOT NULL UNIQUE,
  attempt_count      int         NOT NULL DEFAULT 0,
  max_attempts       int         NOT NULL CHECK (max_attempts > 0),
  run_after          timestamptz NOT NULL DEFAULT now(),
  lease_owner        text,
  lease_epoch        bigint      NOT NULL DEFAULT 0,
  lease_expires_at   timestamptz,
  callback_deadline  timestamptz,
  provider_ref       text,
  last_error         text,
  first_attempted_at timestamptz,
  created_at         timestamptz NOT NULL DEFAULT now(),
  UNIQUE (workflow_id, name, generation)
);
CREATE INDEX steps_claimable ON steps (run_after) WHERE status IN ('PENDING','IN_DOUBT');

CREATE TABLE api_idempotency_keys (
  client_id           text        NOT NULL,
  key                 text        NOT NULL,
  request_fingerprint text        NOT NULL,
  workflow_id         uuid        REFERENCES workflows(id),
  response_body       jsonb,
  created_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (client_id, key)
);

-- +goose Down
DROP TABLE api_idempotency_keys;
DROP TABLE steps;
DROP TABLE workflows;
