#!/bin/sh
# FILE: config/sh/init_set_env.sh
# DATE: 2026-02-22
# PURPOSE: Initialize shared absolute log dirs/permissions and prepare runtime secrets env for all services.

set -eu
umask 077

mkdir -p \
  /home/eee/mailer-app/logs \
  /home/eee/mailer-app/logs/django-dev \
  /home/eee/mailer-app/logs/django-prod \
  /home/eee/mailer-app/logs/nginx \
  /home/eee/mailer-app/logs/postgres \
  /home/eee/mailer-app/logs/certbot \
  /home/eee/mailer-app/logs/gpt \
  /home/eee/mailer-app/logs/crawler \
  /home/eee/mailer-app/logs/processing \
  /host-logs/smrel \
  /serenity-logs/smrel

chmod 777 \
  /home/eee/mailer-app/logs \
  /home/eee/mailer-app/logs/django-dev \
  /home/eee/mailer-app/logs/django-prod \
  /home/eee/mailer-app/logs/nginx \
  /home/eee/mailer-app/logs/postgres \
  /home/eee/mailer-app/logs/certbot \
  /home/eee/mailer-app/logs/gpt \
  /home/eee/mailer-app/logs/crawler \
  /home/eee/mailer-app/logs/processing \
  /host-logs/smrel \
  /serenity-logs/smrel

python -m config.load_keys --seal-only
python -m config.load_keys --load-only --print-export > /run/serenity-secrets/runtime.env
chmod 644 /run/serenity-secrets/runtime.env

# host logs: hourly trim loop (24 MB max per file, keep 16 MB tail)
(
  while :; do
    /usr/local/bin/trim_host_logs.sh || true
    sleep 3600
  done
) &

exec sleep infinity
