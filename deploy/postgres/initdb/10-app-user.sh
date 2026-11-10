#!/usr/bin/env bash
# Creates idem_app_user, the restricted login the API, workers and sweepers use
# (design §10). Postgres runs this once, when it first creates its volume;
# `make app-user` runs it again on an existing one (it is idempotent and
# applies a changed IDEM_APP_DB_PASSWORD).
#
# Logins are infrastructure, not schema: migration 00013 defines what the
# group role idem_app may do; this script only creates a login that is a
# member of it. The group role is created here too (the migration creates it
# only if missing) so that the login can join it before migrations run.
set -euo pipefail
: "${IDEM_APP_DB_PASSWORD:?IDEM_APP_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v pw="$IDEM_APP_DB_PASSWORD" <<'SQL'
SELECT 'CREATE ROLE idem_app NOLOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'idem_app') \gexec
SELECT 'CREATE ROLE idem_app_user LOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'idem_app_user') \gexec
SELECT format('ALTER ROLE idem_app_user WITH LOGIN PASSWORD %L', :'pw') \gexec
GRANT idem_app TO idem_app_user;
SQL
echo "idem_app_user ready (member of idem_app)"
