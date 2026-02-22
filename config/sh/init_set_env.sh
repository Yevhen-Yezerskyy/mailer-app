#!/bin/sh
# FILE: config/sh/init_set_env.sh
# DATE: 2026-02-22
# PURPOSE: Initialize log dirs/permissions and prepare runtime secrets env for all services.

set -eu
umask 077

mkdir -p \
  /host-logs \
  /host-logs/django-dev \
  /host-logs/django-prod \
  /host-logs/nginx \
  /host-logs/postgres \
  /serenity-logs \
  /serenity-logs/nginx

chmod 777 \
  /host-logs \
  /host-logs/django-dev \
  /host-logs/django-prod \
  /host-logs/nginx \
  /host-logs/postgres \
  /serenity-logs \
  /serenity-logs/nginx

python -m config.load_keys --seal-only
python -m config.load_keys --load-only --print-export > /run/serenity-secrets/runtime.env
chmod 644 /run/serenity-secrets/runtime.env

exec sleep infinity
