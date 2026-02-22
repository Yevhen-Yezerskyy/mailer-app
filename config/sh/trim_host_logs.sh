#!/bin/sh
# FILE: config/sh/trim_host_logs.sh
# DATE: 2026-02-22
# PURPOSE: Trim oversized host logs in /host-logs (24 MB max -> keep 16 MB tail).

set -eu

HOST_ROOT="/host-logs"
MAX_MB=24
CUT_MB=8

MAX_BYTES=$((MAX_MB * 1024 * 1024))
KEEP_BYTES=$(((MAX_MB - CUT_MB) * 1024 * 1024))

trim_file() {
  file="$1"
  size="$(wc -c < "$file" 2>/dev/null || echo 0)"
  [ "${size:-0}" -gt "$MAX_BYTES" ] || return 0

  tmp="${file}.trim.$$"
  if tail -c "$KEEP_BYTES" "$file" > "$tmp" 2>/dev/null; then
    cat "$tmp" > "$file"
    echo "[LOG-TRIM] file=$file before=${size} after=${KEEP_BYTES}"
  fi
  rm -f "$tmp"
}

[ -d "$HOST_ROOT" ] || exit 0

find "$HOST_ROOT" -type f \( -name "*.log" -o -name "*.jsonl" \) | while IFS= read -r file; do
  [ -f "$file" ] || continue
  trim_file "$file"
done
