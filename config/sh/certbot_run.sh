#!/bin/sh
# FILE: config/sh/certbot_run.sh
# DATE: 2026-02-21
# PURPOSE: Certbot entrypoint with two modes:
# - init: issue/ensure certificates using standalone challenge before nginx starts.
# - renew: verify certs and run periodic renew via webroot while nginx is running.

set -eu

MODE="${CERTBOT_MODE:-renew}"
CONF_DIR="${CERTBOT_NGINX_CONF_DIR:-/etc/nginx/conf.d}"
WEBROOT="${CERTBOT_WEBROOT:-/var/www/certbot}"
LE_DIR="${CERTBOT_LE_DIR:-/etc/letsencrypt}"
EMAIL="${CERTBOT_EMAIL:?CERTBOT_EMAIL is required}"
RENEW_INTERVAL_SECONDS="${CERTBOT_RENEW_INTERVAL_SECONDS:-43200}"
STAGING="${CERTBOT_STAGING:-0}"

mkdir -p "$WEBROOT"

reload_nginx_if_shared_pid() {
  cmdline=""
  if [ -r /proc/1/cmdline ]; then
    cmdline="$(tr '\000' ' ' </proc/1/cmdline || true)"
  fi
  case "$cmdline" in
    *nginx*)
      if kill -HUP 1 2>/dev/null; then
        echo "INFO: nginx reload sent (HUP pid 1)"
      else
        echo "WARN: nginx reload failed" >&2
      fi
      ;;
    *)
      echo "INFO: nginx reload skipped (pid namespace not shared)"
      ;;
  esac
}

tmp_pairs="$(mktemp)"
tmp_certs="$(mktemp)"
cleanup() {
  rm -f "$tmp_pairs" "$tmp_certs"
}
trap cleanup EXIT INT TERM

for conf in "$CONF_DIR"/*.conf; do
  [ -f "$conf" ] || continue
  awk '
    function flush_block(    i,n,a,d){
      if (cert == "") {
        names = ""
        return
      }
      n = split(names, a, /[[:space:]]+/)
      for (i = 1; i <= n; i++) {
        d = a[i]
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", d)
        if (d == "" || d == "_" || d ~ /^~/) continue
        print cert "\t" d
      }
      cert = ""
      names = ""
      collecting = 0
      line = ""
    }

    {
      sub(/[[:space:]]*#.*/, "", $0)
    }

    /^[[:space:]]*server[[:space:]]*\{/ {
      in_server = 1
      cert = ""
      names = ""
      collecting = 0
      line = ""
      next
    }

    in_server && /^[[:space:]]*}/ {
      flush_block()
      in_server = 0
      next
    }

    in_server && /^[[:space:]]*ssl_certificate[[:space:]]+/ {
      line2 = $0
      if (line2 ~ /\/etc\/letsencrypt\/live\/[^\/]+\/fullchain\.pem;/) {
        sub(/^.*\/etc\/letsencrypt\/live\//, "", line2)
        sub(/\/fullchain\.pem;.*$/, "", line2)
        cert = line2
      }
      next
    }

    in_server {
      if (!collecting && $0 ~ /^[[:space:]]*server_name[[:space:]]+/) {
        line = $0
        collecting = 1
      } else if (collecting) {
        line = line " " $0
      }

      if (collecting && index($0, ";") > 0) {
        sub(/^[[:space:]]*server_name[[:space:]]+/, "", line)
        sub(/;.*/, "", line)
        gsub(/[[:space:]]+/, " ", line)
        names = names " " line
        collecting = 0
        line = ""
      }
    }
  ' "$conf" >> "$tmp_pairs"
done

sort -u "$tmp_pairs" -o "$tmp_pairs"
awk -F '\t' '{print $1}' "$tmp_pairs" | sort -u > "$tmp_certs"

if [ ! -s "$tmp_certs" ]; then
  echo "ERROR: no cert definitions found in $CONF_DIR/*.conf"
  exit 1
fi

while IFS= read -r cert_name; do
  [ -n "$cert_name" ] || continue
  domains="$(awk -F '\t' -v c="$cert_name" '$1 == c {print $2}' "$tmp_pairs" | sort -u)"
  if [ -z "$domains" ]; then
    domains="$cert_name"
  fi

  set -- certbot certonly \
    --non-interactive \
    --agree-tos \
    --email "$EMAIL" \
    --keep-until-expiring \
    --expand \
    --cert-name "$cert_name"

  if [ "$MODE" = "init" ]; then
    set -- "$@" --standalone --preferred-challenges http
  else
    set -- "$@" --webroot -w "$WEBROOT"
  fi

  if [ "$STAGING" = "1" ]; then
    set -- "$@" --test-cert
  fi

  has_cert_name=0
  for d in $domains; do
    [ "$d" = "$cert_name" ] && has_cert_name=1
    set -- "$@" -d "$d"
  done
  [ "$has_cert_name" -eq 1 ] || set -- "$@" -d "$cert_name"

  "$@"
done < "$tmp_certs"

reload_nginx_if_shared_pid

if [ "$MODE" = "init" ]; then
  certbot certificates
  exit 0
fi

while IFS= read -r cert_name; do
  [ -s "$LE_DIR/live/$cert_name/fullchain.pem" ] || {
    echo "ERROR: missing fullchain for $cert_name"
    exit 1
  }
  [ -s "$LE_DIR/live/$cert_name/privkey.pem" ] || {
    echo "ERROR: missing privkey for $cert_name"
    exit 1
  }
done < "$tmp_certs"

certbot certificates

while :; do
  sleep "$RENEW_INTERVAL_SECONDS"
  certbot renew --webroot -w "$WEBROOT" --non-interactive --quiet
  reload_nginx_if_shared_pid
done
