#!/usr/bin/env bash
# FILE: config/sh/pg_backup.sh
# PURPOSE: Daily backup of Postgres DB (mailer-db → mailersys) into ~/backup (keep last 5 daily backups, overwrite today's)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
HOST_LOG_DIR="${PG_BACKUP_HOST_LOG_DIR:-$ROOT_DIR/logs/postgres}"
SYS_LOG_DIR="${PG_BACKUP_SYS_LOG_DIR:-/serenity-logs/postgres}"
HOST_LOG_FILE="$HOST_LOG_DIR/pg_backup.log"
SYS_LOG_FILE="$SYS_LOG_DIR/pg_backup.log"

mkdir -p "$HOST_LOG_DIR"
log_targets=("$HOST_LOG_FILE")
if [ -d /serenity-logs ] && mkdir -p "$SYS_LOG_DIR" 2>/dev/null; then
  log_targets+=("$SYS_LOG_FILE")
fi

exec > >(tee -a "${log_targets[@]}") 2>&1

BACKUP_DIR="${HOME}/backup"
CONTAINER="mailer-db"
DB_NAME="mailersys"
DB_USER="mailersys_user"
KEEP_LAST=5

mkdir -p "$BACKUP_DIR"

# one backup per day (overwrite today's)
TS="$(date +%F)"
OUT="$BACKUP_DIR/${DB_NAME}_${TS}.sql.gz"

echo "INFO: pg_backup start ts=$TS container=$CONTAINER db=$DB_NAME"
docker exec "$CONTAINER" pg_dump -U "$DB_USER" -d "$DB_NAME" | gzip > "$OUT"
echo "INFO: backup written: $OUT"

# keep only last 5 backups (by filename date)
ls -1t "$BACKUP_DIR/${DB_NAME}_"*.sql.gz 2>/dev/null | tail -n +$((KEEP_LAST+1)) | xargs -r rm -f
echo "INFO: keep_last=$KEEP_LAST completed"
