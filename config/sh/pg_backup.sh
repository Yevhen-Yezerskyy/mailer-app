#!/usr/bin/env bash
# FILE: config/sh/pg_backup.sh
# PURPOSE: Daily backup of Postgres DB (mailer-db → mailersys) into ~/backup (keep last 5 daily backups, overwrite today's)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
LOG_DIR="${PG_BACKUP_LOG_DIR:-/home/eee/mailer-app/logs/postgres}"
LOG_FILE="$LOG_DIR/pg_backup.log"

mkdir -p "$LOG_DIR"

exec > >(tee -a "$LOG_FILE") 2>&1

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
