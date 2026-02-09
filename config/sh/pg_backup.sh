#!/usr/bin/env bash
# FILE: config/sh/pg_backup.sh
# PURPOSE: Daily backup of Postgres DB (mailer-db → mailersys) into ~/backup (keep last 5 daily backups, overwrite today's)

set -euo pipefail

BACKUP_DIR="${HOME}/backup"
CONTAINER="mailer-db"
DB_NAME="mailersys"
DB_USER="mailersys_user"
KEEP_LAST=5

mkdir -p "$BACKUP_DIR"

# one backup per day (overwrite today's)
TS="$(date +%F)"
OUT="$BACKUP_DIR/${DB_NAME}_${TS}.sql.gz"

docker exec -t "$CONTAINER" pg_dump -U "$DB_USER" -d "$DB_NAME" | gzip > "$OUT"

# keep only last 5 backups (by filename date)
ls -1t "$BACKUP_DIR/${DB_NAME}_"*.sql.gz 2>/dev/null | tail -n +$((KEEP_LAST+1)) | xargs -r rm -f
