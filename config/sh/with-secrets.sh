#!/bin/sh
# FILE: config/sh/with-secrets.sh
# DATE: 2026-02-21
# PURPOSE: Unified runtime launcher for all services: load /run/serenity-secrets/runtime.env, map DB_* to POSTGRES_*, then exec target command.

set -eu

SECRETS_DIR="${SECRETS_DIR:-/run/serenity-secrets}"
SECRETS_ENV_FILE="${SECRETS_ENV_FILE:-$SECRETS_DIR/runtime.env}"

if [ ! -f "$SECRETS_ENV_FILE" ]; then
  echo "ERROR: secrets env file not found: $SECRETS_ENV_FILE" >&2
  exit 1
fi

if [ "$#" -eq 0 ]; then
  echo "ERROR: no command passed to with-secrets.sh" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
. "$SECRETS_ENV_FILE"
set +a

if [ -n "${DB_NAME:-}" ] && [ -z "${POSTGRES_DB:-}" ]; then
  POSTGRES_DB="$DB_NAME"
fi
if [ -n "${DB_USER:-}" ] && [ -z "${POSTGRES_USER:-}" ]; then
  POSTGRES_USER="$DB_USER"
fi
if [ -n "${DB_PASSWORD:-}" ] && [ -z "${POSTGRES_PASSWORD:-}" ]; then
  POSTGRES_PASSWORD="$DB_PASSWORD"
fi

export POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD

exec "$@"
