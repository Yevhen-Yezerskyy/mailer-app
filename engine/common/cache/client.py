# FILE: engine/common/cache/client.py  (обновлено — 2026-01-05)
# PURPOSE: Redis-cache client via UNIX-socket (старый интерфейс НЕ ЛОМАЕМ) + быстрые bulk/stream-хелперы.
#          Старое (без изменений по API): CLIENT.get/set/stats/lock_* и memo()
#          Новое (опционально): get_many/set_many/delete_many + memo_many_iter (yield всегда (query, value))
#          ВАЖНО: sliding TTL убран (GET вместо GETEX) ради скорости и меньшей нагрузки.

from __future__ import annotations

import errno
import hashlib
import os
import pickle
import socket
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

DEFAULT_TTL_SEC = 7 * 24 * 60 * 60
DEFAULT_VERSION = "dev"

MAX_VALUE_BYTES = 128 * 1024
MAX_QUERY_BYTES = 32 * 1024

RPC_TIMEOUT_SEC = 1.0

_RPC_FAIL_BACKOFF_SEC = 0.5
_RPC_TIMEOUT_BACKOFF_SEC = 0.05
_RPC_SOFT_BACKOFF_SEC = 0.05
_RPC_DOWN_UNTIL = 0.0

POOL_SIZE = 10
KEY_SIZE_CAP = 50_000

# socket IO
_RECV_CHUNK = 64 * 1024
_CRLF = b"\r\n"

_Resp = Union[None, int, bytes, str, list[Any]]


def _project_root() -> Path:
    # engine/common/cache/client.py -> cache -> common -> engine -> PROJECT_ROOT
    return Path(__file__).resolve().parents[3]


def _redis_sock_path() -> str:
    env = (os.environ.get("REDIS_SOCKET") or "").strip()
    if env:
        return env
    return str(_project_root() / "run" / "redis" / "redis.sock")


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


class _LRUKeySizes:
    def __init__(self, cap: int) -> None:
        self._cap = int(cap)
        self._od: "OrderedDict[str, int]" = OrderedDict()
        self._mu = threading.Lock()

    def set(self, key: str, size: int) -> None:
        if not key:
            return
        try:
            size_i = int(size)
        except Exception:
            return
        if size_i <= 0:
            return
        with self._mu:
            self._od[key] = size_i
            self._od.move_to_end(key, last=True)
            while len(self._od) > self._cap:
                self._od.popitem(last=False)


_KEY_SIZES = _LRUKeySizes(KEY_SIZE_CAP)


# -------------------- RESP helpers --------------------
def _b(x: Union[str, bytes, int]) -> bytes:
    if isinstance(x, bytes):
        return x
    if isinstance(x, int):
        return str(x).encode("utf-8")
    return x.encode("utf-8")


def _encode_cmd(*parts: Union[str, bytes, int]) -> bytes:
    # RESP Array of Bulk Strings
    out = bytearray()
    out += b"*" + str(len(parts)).encode("utf-8") + _CRLF
    for p in parts:
        pb = _b(p)
        out += b"$" + str(len(pb)).encode("utf-8") + _CRLF
        out += pb + _CRLF
    return bytes(out)


class _RedisIOError(RuntimeError):
    pass


class _RedisConn:
    def __init__(self) -> None:
        self._sock: Optional[socket.socket] = None
        self._mu = threading.Lock()
        self._buf = bytearray()

    def _connect(self) -> socket.socket:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(RPC_TIMEOUT_SEC)
        s.connect(_redis_sock_path())
        return s

    def _ensure(self) -> socket.socket:
        if self._sock is None:
            self._sock = self._connect()
            self._buf.clear()
        return self._sock

    def close(self) -> None:
        try:
            if self._sock is not None:
                self._sock.close()
        except Exception:
            pass
        self._sock = None
        self._buf.clear()

    def _fill(self, s: socket.socket, need: int = 1) -> None:
        while len(self._buf) < need:
            chunk = s.recv(_RECV_CHUNK)
            if not chunk:
                raise _RedisIOError("closed")
            self._buf += chunk

    def _read_exact(self, s: socket.socket, n: int) -> bytes:
        if n <= 0:
            return b""
        self._fill(s, n)
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out

    def _read_until_crlf(self, s: socket.socket) -> bytes:
        while True:
            idx = self._buf.find(_CRLF)
            if idx != -1:
                out = bytes(self._buf[:idx])
                del self._buf[: idx + 2]
                return out
            self._fill(s, len(self._buf) + 1)

    def _read_reply(self, s: socket.socket) -> _Resp:
        t = self._read_exact(s, 1)
        if t == b"+":
            return self._read_until_crlf(s).decode("utf-8", errors="replace")
        if t == b"-":
            raise _RedisIOError(self._read_until_crlf(s).decode("utf-8", errors="replace"))
        if t == b":":
            line = self._read_until_crlf(s)
            try:
                return int(line)
            except Exception:
                raise _RedisIOError("bad_int")
        if t == b"$":
            line = self._read_until_crlf(s)
            try:
                ln = int(line)
            except Exception:
                raise _RedisIOError("bad_bulk_len")
            if ln == -1:
                return None
            data = self._read_exact(s, ln)
            _ = self._read_exact(s, 2)  # CRLF
            return data
        if t == b"*":
            line = self._read_until_crlf(s)
            try:
                n = int(line)
            except Exception:
                raise _RedisIOError("bad_array_len")
            if n == -1:
                return None
            arr: list[Any] = []
            for _ in range(n):
                arr.append(self._read_reply(s))
            return arr
        raise _RedisIOError("bad_prefix")

    def call(self, *parts: Union[str, bytes, int]) -> _Resp:
        with self._mu:
            s = self._ensure()
            s.sendall(_encode_cmd(*parts))
            return self._read_reply(s)

    def call_many(self, cmds: Sequence[Tuple[Union[str, bytes, int], ...]]) -> List[_Resp]:
        with self._mu:
            s = self._ensure()
            blob = b"".join(_encode_cmd(*c) for c in cmds)
            s.sendall(blob)
            out: List[_Resp] = []
            for _ in range(len(cmds)):
                out.append(self._read_reply(s))
            return out


class _ConnPool:
    def __init__(self, size: int) -> None:
        self._size = int(size)
        self._mu = threading.Lock()
        self._free: list[_RedisConn] = []
        self._created = 0

    def acquire(self) -> _RedisConn:
        with self._mu:
            if self._free:
                return self._free.pop()
            if self._created < self._size:
                self._created += 1
                return _RedisConn()
        return _RedisConn()

    def release(self, c: _RedisConn) -> None:
        with self._mu:
            if len(self._free) >= self._size:
                c.close()
                return
            self._free.append(c)

    def drop(self, c: _RedisConn) -> None:
        c.close()


_POOL = _ConnPool(POOL_SIZE)


def _is_down_oserror(e: OSError) -> bool:
    return e.errno in (errno.ENOENT, errno.ECONNREFUSED, errno.ECONNRESET, errno.ENOTCONN, errno.EPIPE)


def _redis_call(*parts: Union[str, bytes, int]) -> Optional[_Resp]:
    global _RPC_DOWN_UNTIL

    now = time.monotonic()
    if now < _RPC_DOWN_UNTIL:
        return None

    c = _POOL.acquire()
    try:
        r = c.call(*parts)
        _POOL.release(c)
        return r
    except socket.timeout:
        _RPC_DOWN_UNTIL = time.monotonic() + _RPC_TIMEOUT_BACKOFF_SEC
        _POOL.drop(c)
        return None
    except FileNotFoundError:
        _RPC_DOWN_UNTIL = time.monotonic() + _RPC_FAIL_BACKOFF_SEC
        _POOL.drop(c)
        return None
    except (BrokenPipeError, ConnectionError, _RedisIOError):
        _RPC_DOWN_UNTIL = time.monotonic() + _RPC_FAIL_BACKOFF_SEC
        _POOL.drop(c)
        return None
    except OSError as e:
        _RPC_DOWN_UNTIL = time.monotonic() + (_RPC_FAIL_BACKOFF_SEC if _is_down_oserror(e) else _RPC_SOFT_BACKOFF_SEC)
        _POOL.drop(c)
        return None
    except Exception:
        _RPC_DOWN_UNTIL = time.monotonic() + _RPC_SOFT_BACKOFF_SEC
        _POOL.drop(c)
        return None


def _redis_call_many(cmds: Sequence[Tuple[Union[str, bytes, int], ...]]) -> Optional[List[_Resp]]:
    global _RPC_DOWN_UNTIL

    if not cmds:
        return []

    now = time.monotonic()
    if now < _RPC_DOWN_UNTIL:
        return None

    c = _POOL.acquire()
    try:
        r = c.call_many(cmds)
        _POOL.release(c)
        return r
    except socket.timeout:
        _RPC_DOWN_UNTIL = time.monotonic() + _RPC_TIMEOUT_BACKOFF_SEC
        _POOL.drop(c)
        return None
    except FileNotFoundError:
        _RPC_DOWN_UNTIL = time.monotonic() + _RPC_FAIL_BACKOFF_SEC
        _POOL.drop(c)
        return None
    except (BrokenPipeError, ConnectionError, _RedisIOError):
        _RPC_DOWN_UNTIL = time.monotonic() + _RPC_FAIL_BACKOFF_SEC
        _POOL.drop(c)
        return None
    except OSError as e:
        _RPC_DOWN_UNTIL = time.monotonic() + (_RPC_FAIL_BACKOFF_SEC if _is_down_oserror(e) else _RPC_SOFT_BACKOFF_SEC)
        _POOL.drop(c)
        return None
    except Exception:
        _RPC_DOWN_UNTIL = time.monotonic() + _RPC_SOFT_BACKOFF_SEC
        _POOL.drop(c)
        return None


_LUA_RENEW = b"if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end"
_LUA_RELEASE = b"if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"


def _chunked_pairs(items: Sequence[Tuple[Any, Any]], n: int) -> Iterator[Sequence[Tuple[Any, Any]]]:
    if n <= 0:
        n = 200
    for i in range(0, len(items), n):
        yield items[i : i + n]


class CacheClient:
    # ---------------- OLD API (DO NOT BREAK) ----------------

    def get(self, key: str, ttl_sec: int) -> Optional[bytes]:
        # ttl_sec is kept for backward compatibility; not used for sliding TTL anymore.
        r = _redis_call("GET", key)
        if r is None:
            return None
        if not isinstance(r, (bytes, bytearray)):
            return None

        payload = bytes(r)
        if len(payload) > MAX_VALUE_BYTES:
            return None

        _KEY_SIZES.set(key, len(payload))
        return payload

    def set(self, key: str, payload: bytes, ttl_sec: int) -> bool:
        if not isinstance(payload, (bytes, bytearray)):
            return False
        pb = bytes(payload)
        if len(pb) > MAX_VALUE_BYTES:
            return False

        try:
            ttl_i = int(ttl_sec)
        except Exception:
            ttl_i = DEFAULT_TTL_SEC
        if ttl_i <= 0:
            ttl_i = DEFAULT_TTL_SEC

        r = _redis_call("SET", key, pb, "EX", ttl_i)
        if r is None:
            return False
        ok = isinstance(r, str) and r.upper() == "OK"
        if ok:
            _KEY_SIZES.set(key, len(pb))
        return ok

    def stats(self) -> Optional[dict[str, Any]]:
        dbsize = _redis_call("DBSIZE")
        info = _redis_call("INFO", "memory")
        if dbsize is None:
            return None

        items = int(dbsize) if isinstance(dbsize, int) else None
        used = None
        if isinstance(info, (bytes, bytearray)):
            try:
                txt = bytes(info).decode("utf-8", errors="replace")
                for line in txt.splitlines():
                    if line.startswith("used_memory:"):
                        used = int(line.split(":", 1)[1].strip())
                        break
            except Exception:
                used = None

        return {"ok": True, "items": items, "used_memory": used, "socket": _redis_sock_path()}

    def lock_try(self, key: str, *, ttl_sec: float, owner: str) -> Optional[dict[str, Any]]:
        try:
            ttl_ms = int(float(ttl_sec) * 1000)
        except Exception:
            ttl_ms = 1000
        if ttl_ms <= 0:
            ttl_ms = 1000

        token = os.urandom(16).hex()
        lock_key = f"lock:{key}"

        r = _redis_call("SET", lock_key, token, "NX", "PX", ttl_ms)
        if r is None:
            return None
        if isinstance(r, str) and r.upper() == "OK":
            return {"ok": True, "acquired": True, "owner": str(owner), "token": token, "expire_ms": ttl_ms}
        return {"ok": True, "acquired": False}

    def lock_renew(self, key: str, *, ttl_sec: float, token: str) -> bool:
        try:
            ttl_ms = int(float(ttl_sec) * 1000)
        except Exception:
            ttl_ms = 1000
        if ttl_ms <= 0:
            ttl_ms = 1000

        lock_key = f"lock:{key}"
        r = _redis_call("EVAL", _LUA_RENEW, 1, lock_key, str(token), ttl_ms)
        return bool(isinstance(r, int) and r == 1)

    def lock_release(self, key: str, *, token: str) -> bool:
        lock_key = f"lock:{key}"
        r = _redis_call("EVAL", _LUA_RELEASE, 1, lock_key, str(token))
        return bool(isinstance(r, int) and r == 1)

    def lock_status(self, key: str) -> Optional[dict[str, Any]]:
        lock_key = f"lock:{key}"
        r = _redis_call("GET", lock_key)
        if r is None:
            return None
        return {"ok": True, "held": isinstance(r, (bytes, bytearray))}

    # ---------------- NEW OPTIONAL FAST API ----------------

    def get_many(self, keys: Sequence[str], ttl_sec: int) -> List[Optional[bytes]]:
        # MGET, no TTL-touch. ttl_sec kept for symmetry/backward usage patterns.
        if not keys:
            return []

        r = _redis_call("MGET", *list(keys))
        if r is None or not isinstance(r, list):
            return [None] * len(keys)

        out: List[Optional[bytes]] = []
        for i, v in enumerate(r[: len(keys)]):
            if isinstance(v, (bytes, bytearray)):
                b = bytes(v)
                if len(b) <= MAX_VALUE_BYTES:
                    _KEY_SIZES.set(str(keys[i]), len(b))
                    out.append(b)
                else:
                    out.append(None)
            else:
                out.append(None)

        if len(out) < len(keys):
            out.extend([None] * (len(keys) - len(out)))
        return out

    def set_many(self, items: Sequence[Tuple[str, bytes]], ttl_sec: int) -> int:
        # Pipeline SET EX ... ; returns OK count
        if not items:
            return 0

        try:
            ttl_i = int(ttl_sec)
        except Exception:
            ttl_i = DEFAULT_TTL_SEC
        if ttl_i <= 0:
            ttl_i = DEFAULT_TTL_SEC

        kept: List[Tuple[str, bytes]] = []
        cmds: List[Tuple[Union[str, bytes, int], ...]] = []

        for k, payload in items:
            if not isinstance(payload, (bytes, bytearray)):
                continue
            pb = bytes(payload)
            if len(pb) > MAX_VALUE_BYTES:
                continue
            kept.append((str(k), pb))
            cmds.append(("SET", str(k), pb, "EX", ttl_i))

        if not cmds:
            return 0

        rr = _redis_call_many(cmds)
        if rr is None:
            return 0

        ok = 0
        for (k, pb), r in zip(kept, rr):
            if isinstance(r, str) and r.upper() == "OK":
                ok += 1
                _KEY_SIZES.set(k, len(pb))
        return ok

    def delete_many(self, keys: Sequence[str]) -> int:
        if not keys:
            return 0
        r = _redis_call("DEL", *list(keys))
        return int(r) if isinstance(r, int) else 0


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


def memo_many_iter(
    queries: Sequence[Any],
    fn: Callable[[Any], Any],
    *,
    ttl: int = DEFAULT_TTL_SEC,
    version: str = DEFAULT_VERSION,
    update: bool = False,
    chunk: int = 200,
) -> Iterator[Tuple[Any, Any]]:
    """
    Быстрый stream-memo на батчах.
    Yield ВСЕГДА: (query, value).
    Порядок НЕ гарантируется.
    Miss -> fn(query) -> set(key, pickle(value)).
    """
    if not queries:
        return
        yield  # pragma: no cover

    ttl_sec = int(ttl) if ttl is not None else DEFAULT_TTL_SEC
    chunk_i = int(chunk) if chunk is not None else 200
    if chunk_i <= 0:
        chunk_i = 200

    # key building: bad query -> compute directly (same semantics as memo fallback)
    good: List[Tuple[Any, str]] = []
    for q in queries:
        try:
            k = _make_key(q, fn, version)
            good.append((q, k))
        except Exception:
            yield (q, fn(q))

    if not good:
        return

    _MISS = object()

    for part in _chunked_pairs(good, chunk_i):
        part_qk = list(part)
        keys = [k for (_q, k) in part_qk]

        hits: Dict[str, Any] = {}
        misses: List[Tuple[Any, str]] = []

        if update:
            misses = part_qk
        else:
            payloads = CLIENT.get_many(keys, ttl_sec=ttl_sec)
            for (q, k), payload in zip(part_qk, payloads):
                if payload is None:
                    misses.append((q, k))
                    continue
                try:
                    hits[k] = pickle.loads(payload)  # may be None (valid value)
                except Exception:
                    misses.append((q, k))

        # yield hits (unordered ok)
        for q, k in part_qk:
            v = hits.get(k, _MISS)
            if v is not _MISS:
                yield (q, v)

        # compute misses, store, yield
        to_set: List[Tuple[str, bytes]] = []
        for q, k in misses:
            v = fn(q)
            try:
                pb = pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)
                if len(pb) <= MAX_VALUE_BYTES:
                    to_set.append((k, pb))
            except Exception:
                pass
            yield (q, v)

        if to_set:
            CLIENT.set_many(to_set, ttl_sec=ttl_sec)
