#!/bin/sh
# FILE: config/sh/crawler-net-guard.sh
# DATE: 2026-04-11
# PURPOSE: Lock crawler container egress down to loopback SOCKS, DB, Docker DNS, and SSH to tunnel hosts.

set -eu

find_bin() {
  for candidate in "$@"; do
    if [ -n "$candidate" ] && [ -x "$candidate" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
    if command -v "$candidate" >/dev/null 2>&1; then
      command -v "$candidate"
      return 0
    fi
  done
  return 1
}

IPTABLES_BIN="${IPTABLES_BIN:-}"
IP6TABLES_BIN="${IP6TABLES_BIN:-}"

if [ -n "$IPTABLES_BIN" ]; then
  IPTABLES_RESOLVED="$IPTABLES_BIN"
else
  IPTABLES_RESOLVED="$(find_bin iptables /usr/sbin/iptables /sbin/iptables)" || {
    echo "ERROR: iptables not found" >&2
    exit 1
  }
fi

if [ -n "$IP6TABLES_BIN" ]; then
  IP6TABLES_RESOLVED="$IP6TABLES_BIN"
else
  IP6TABLES_RESOLVED="$(find_bin ip6tables /usr/sbin/ip6tables /sbin/ip6tables || true)"
fi

TMP_RULES="$(mktemp)"
cleanup() {
  rm -f "$TMP_RULES"
}
trap cleanup EXIT INT TERM

python - <<'PY' > "$TMP_RULES"
import json
import os
import socket


def emit(family: int, addr: str, port: int) -> None:
    if not addr:
        return
    print(f"{family}\t{addr}\t{int(port)}")


def resolve_all(host: str, port: int) -> None:
    host_s = str(host or "").strip()
    if not host_s:
        return
    try:
        infos = socket.getaddrinfo(host_s, int(port), type=socket.SOCK_STREAM)
    except Exception:
        return
    seen: set[tuple[int, str, int]] = set()
    for family, _socktype, _proto, _canonname, sockaddr in infos:
        if family not in (socket.AF_INET, socket.AF_INET6):
            continue
        addr = str(sockaddr[0] or "").strip()
        key = (family, addr, int(port))
        if not addr or key in seen:
            continue
        seen.add(key)
        emit(4 if family == socket.AF_INET else 6, addr, int(port))


resolve_all(os.environ.get("DB_HOST", ""), int(os.environ.get("DB_PORT", "5432") or 5432))

raw = os.environ.get("CORE_CRAWLER_TUNNELS_11880_JSON", "").strip()
if raw:
    try:
        payload = json.loads(raw)
    except Exception:
        payload = {}
    for row in list(payload.get("tunnels") or []):
        host = str((row or {}).get("host") or "").strip()
        ssh_port = int((row or {}).get("ssh_port") or payload.get("ssh_port") or 22)
        resolve_all(host, ssh_port)
PY

"$IPTABLES_RESOLVED" -F OUTPUT
"$IPTABLES_RESOLVED" -P OUTPUT DROP
"$IPTABLES_RESOLVED" -A OUTPUT -o lo -j ACCEPT
"$IPTABLES_RESOLVED" -A OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
"$IPTABLES_RESOLVED" -A OUTPUT -d 127.0.0.11 -p udp --dport 53 -j ACCEPT
"$IPTABLES_RESOLVED" -A OUTPUT -d 127.0.0.11 -p tcp --dport 53 -j ACCEPT

if [ -n "$IP6TABLES_RESOLVED" ]; then
  "$IP6TABLES_RESOLVED" -F OUTPUT
  "$IP6TABLES_RESOLVED" -P OUTPUT DROP
  "$IP6TABLES_RESOLVED" -A OUTPUT -o lo -j ACCEPT
  "$IP6TABLES_RESOLVED" -A OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
fi

while IFS="$(printf '\t')" read -r family addr port; do
  if [ -z "${family:-}" ] || [ -z "${addr:-}" ] || [ -z "${port:-}" ]; then
    continue
  fi
  if [ "$family" = "4" ]; then
    "$IPTABLES_RESOLVED" -A OUTPUT -d "$addr" -p tcp --dport "$port" -j ACCEPT
    continue
  fi
  if [ "$family" = "6" ] && [ -n "$IP6TABLES_RESOLVED" ]; then
    "$IP6TABLES_RESOLVED" -A OUTPUT -d "$addr" -p tcp --dport "$port" -j ACCEPT
  fi
done < "$TMP_RULES"

echo "[crawler-net-guard] OUTPUT locked down" >&2
