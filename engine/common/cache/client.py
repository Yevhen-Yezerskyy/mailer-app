# FILE: engine/common/cache/client.py  (обновлено — 2026-01-02)
# Смысл: клиент для dev IPC-кеша. Если демона нет/ошибка — считаем локально и продолжаем.
# (новое — 2026-01-02) исправление производительности:
# - Анти-шторм: если сокет/демон недоступен, временно "глушим" RPC на короткий backoff, чтобы не долбить CPU/FS

from __future__ import annotations

import hashlib
import pickle
import socket
import struct
import time
from pathlib import Path
from typing import Any, Callable, Optional


DEFAULT_TTL_SEC = 7 * 24 * 60 * 60  # неделя
DEFAULT_VERSION = "dev"

# лимиты (зафиксировано)
MAX_VALUE_BYTES = 128 * 1024

# лимиты входа (защита от "случайно закешировал весь мир")
MAX_QUERY_BYTES = 32 * 1024
MAX_REQUEST_BYTES = 256 * 1024
RPC_TIMEOUT_SEC = 0.25

# анти-шторм по недоступности демона
_RPC_FAIL_BACKOFF_SEC = 0.5
_RPC_DOWN_UNTIL = 0.0


def _module_dir() -> Path:
    return Path(__file__).resolve().parent


def _sock_path() -> str:
    return str(_module_dir() / "cache.sock")


def _fn_fingerprint(fn: Callable[..., Any]) -> str:
    return f"{getattr(fn, '__module__', '')}:{getattr(fn, '__qualname__', getattr(fn, '__name__', 'fn'))}"


def _safe_pickle(obj: Any) -> bytes:
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def _hash_query(query: Any) -> str:
    raw = _safe_pickle(query)
    if len(raw) > MAX_QUERY_BYTES:
        raise ValueError(f"query_too_big: {len(raw)} > {MAX_QUERY_BYTES}")
    return hashlib.sha1(raw).hexdigest()


def _make_key(query: Any, fn: Callable[[Any], Any], version: str) -> str:
    qh = _hash_query(query)
    raw = (_fn_fingerprint(fn) + "|" + str(version) + "|" + qh).encode("utf-8", errors="replace")
    return hashlib.sha1(raw).hexdigest()


def _recv_exact(conn: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    return buf


def _rpc(req: dict[str, Any]) -> Optional[dict[str, Any]]:
    global _RPC_DOWN_UNTIL

    now = time.monotonic()
    if now < _RPC_DOWN_UNTIL:
        return None

    try:
        data = _safe_pickle(req)
        if len(data) > MAX_REQUEST_BYTES:
            return None

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(RPC_TIMEOUT_SEC)
            s.connect(_sock_path())
            s.sendall(struct.pack("!I", len(data)) + data)

            hdr = _recv_exact(s, 4)
            (ln,) = struct.unpack("!I", hdr)
            if ln > MAX_REQUEST_BYTES:
                return None
            payload = _recv_exact(s, ln)
            obj = pickle.loads(payload)
            return obj if isinstance(obj, dict) else None
    except Exception:
        _RPC_DOWN_UNTIL = time.monotonic() + _RPC_FAIL_BACKOFF_SEC
        return None


class CacheClient:
    # -------------------- cache --------------------

    def get(self, key: str, ttl_sec: int) -> Optional[bytes]:
        resp = _rpc({"op": "GET", "key": key, "ttl_sec": int(ttl_sec)})
        if not resp or not resp.get("ok"):
            return None
        if not resp.get("hit"):
            return None

        payload = resp.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return None
        if len(payload) > MAX_VALUE_BYTES:
            return None
        return bytes(payload)

    def set(self, key: str, payload: bytes, ttl_sec: int) -> bool:
        if len(payload) > MAX_VALUE_BYTES:
            return False
        resp = _rpc({"op": "SET", "key": key, "ttl_sec": int(ttl_sec), "payload": payload})
        return bool(resp and resp.get("ok") and resp.get("stored"))

    def stats(self) -> Optional[dict[str, Any]]:
        resp = _rpc({"op": "STATS"})
        return resp if resp and resp.get("ok") else None

    # -------------------- locks (lease) --------------------

    def lock_try(self, key: str, *, ttl_sec: float, owner: str) -> Optional[dict[str, Any]]:
        resp = _rpc({"op": "LOCK_TRY", "key": str(key), "ttl_sec": float(ttl_sec), "owner": str(owner)})
        return resp if resp and resp.get("ok") else None

    def lock_renew(self, key: str, *, ttl_sec: float, token: str) -> bool:
        resp = _rpc({"op": "LOCK_RENEW", "key": str(key), "ttl_sec": float(ttl_sec), "token": str(token)})
        return bool(resp and resp.get("ok") and resp.get("renewed") is True)

    def lock_release(self, key: str, *, token: str) -> bool:
        resp = _rpc({"op": "LOCK_RELEASE", "key": str(key), "token": str(token)})
        return bool(resp and resp.get("ok") and resp.get("released") is True)

    def lock_status(self, key: str) -> Optional[dict[str, Any]]:
        resp = _rpc({"op": "LOCK_STATUS", "key": str(key)})
        return resp if resp and resp.get("ok") else None


CLIENT = CacheClient()


def memo(
    query: Any,
    fn: Callable[[Any], Any],
    *,
    ttl: int = DEFAULT_TTL_SEC,
    version: str = DEFAULT_VERSION,
    update: bool = False,
) -> Any:
    ttl_sec = int(ttl) if ttl is not None else DEFAULT_TTL_SEC

    try:
        key = _make_key(query, fn, version)
    except Exception:
        return fn(query)

    if not update:
        payload = CLIENT.get(key, ttl_sec=ttl_sec)
        if payload is not None:
            try:
                return pickle.loads(payload)
            except Exception:
                pass

    value = fn(query)

    try:
        out = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        return value

    CLIENT.set(key, out, ttl_sec=ttl_sec)
    return value
