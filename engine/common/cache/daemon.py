# FILE: engine/common/cache/daemon.py
# DATE: 2026-01-05
# CHANGE: демон не засираем BrokenPipe и быстрее дренируем accept:
# - accept() в цикле (дренируем очередь) за один select
# - BrokenPipe/ConnectionError/socket.timeout при recv/send НЕ считаем "ошибкой" (клиент мог тайм-аутнуться)
# - conn timeout чуть увеличен (под RPC_TIMEOUT_SEC клиента)
# - остальная логика/протокол без изменений

from __future__ import annotations

import heapq
import os
import pickle
import selectors
import signal
import socket
import struct
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Tuple


# фиксированные лимиты (задача)
MAX_VALUE_BYTES = 128 * 1024
MAX_CACHE_BYTES = 50 * 1024 * 1024
GC_TARGET_RATIO = 0.60  # чистим до 60%
DEFAULT_TTL_SEC = 7 * 24 * 60 * 60

# защита протокола
MAX_REQUEST_BYTES = 256 * 1024

# dev поведение
WATCHDOG_STALL_SEC = 60
ALIVE_EVERY_SEC = 10

# чтобы watchdog не срабатывал в idle — ограничиваем max timeout selector
MAX_IDLE_WAIT_SEC = 2.0

# таймаут на обработку одного соединения (должен быть >= client RPC_TIMEOUT_SEC)
CONN_TIMEOUT_SEC = 2.0


@dataclass
class _Entry:
    payload: bytes
    size: int
    expire_at: float
    last_access: float


@dataclass
class _Lease:
    owner: str
    token: str
    expire_at: float


class CacheDaemon:
    def __init__(self) -> None:
        self.dir = Path(__file__).resolve().parent
        self.sock_path = self.dir / "cache.sock"
        self.dump_path = self.dir / "cache.dump"

        self.data: dict[str, _Entry] = {}
        self.total_bytes = 0

        # volatile locks (lease): key -> _Lease
        self.locks: dict[str, _Lease] = {}

        # heaps: (expire_at, kind, key, version)
        # kind: 0=cache, 1=lock
        self._exp_heap: list[Tuple[float, int, str, float]] = []

        self._stop = False
        self._last_heartbeat = time.monotonic()
        self._last_alive = 0.0

        # счетчики на период (для alive-лога)
        self._evicted = 0
        self._expired = 0
        self._errors = 0

    def _now(self) -> float:
        return time.monotonic()

    def _beat(self) -> None:
        self._last_heartbeat = self._now()

    def _log(self, msg: str) -> None:
        print(msg)

    def _safe_load_pickle(self, raw: bytes) -> Any:
        return pickle.loads(raw)

    def _safe_dump_pickle(self, obj: Any) -> bytes:
        return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

    def _push_expire(self, *, kind: int, key: str, expire_at: float) -> None:
        # version = expire_at: lazy-invalidations (сверяем с текущим expire_at записи)
        heapq.heappush(self._exp_heap, (float(expire_at), int(kind), str(key), float(expire_at)))

    def _drop(self, key: str) -> None:
        e = self.data.pop(key, None)
        if e:
            self.total_bytes -= e.size

    def _drop_lock(self, key: str) -> None:
        self.locks.pop(key, None)

    def _expire_pop(self) -> None:
        """Проверяет heap и выкидывает всё протухшее (lazy)."""
        now = self._now()
        h = self._exp_heap

        while h:
            exp_at, kind, key, ver = h[0]
            if exp_at > now:
                break
            heapq.heappop(h)

            if kind == 0:
                cur = self.data.get(key)
                if not cur:
                    continue
                # stale heap-item?
                if float(cur.expire_at) != float(ver):
                    continue
                if now >= cur.expire_at:
                    self._drop(key)
                    self._expired += 1
            else:
                cur = self.locks.get(key)
                if not cur:
                    continue
                if float(cur.expire_at) != float(ver):
                    continue
                if now >= cur.expire_at:
                    self._drop_lock(key)

    def _next_deadline(self) -> Optional[float]:
        """Следующее событие: ближайший expire, либо alive-log."""
        now = self._now()
        nxt: Optional[float] = None

        if self._exp_heap:
            nxt = float(self._exp_heap[0][0])

        alive_at = (self._last_alive + ALIVE_EVERY_SEC) if self._last_alive else (now + ALIVE_EVERY_SEC)
        if nxt is None:
            return alive_at
        return min(nxt, alive_at)

    def _gc(self) -> None:
        try:
            # сперва выкинем протухшее по heap
            self._expire_pop()

            if self.total_bytes <= MAX_CACHE_BYTES:
                return

            target = int(MAX_CACHE_BYTES * GC_TARGET_RATIO)

            # eviction по размеру (крупные сначала), а при равных — по expire_at (раньше умрет), затем по last_access (старее)
            items = sorted(
                self.data.items(),
                key=lambda kv: (-kv[1].size, kv[1].expire_at, kv[1].last_access),
            )

            for k, _e in items:
                if self.total_bytes <= target:
                    break
                self._drop(k)
                self._evicted += 1
        except Exception:
            self._errors += 1

    def _alive_log(self) -> None:
        now = self._now()
        if self._last_alive and (now - self._last_alive) < ALIVE_EVERY_SEC:
            return
        self._last_alive = now

        mb = self.total_bytes / 1024 / 1024
        limit = MAX_CACHE_BYTES / 1024 / 1024
        items = len(self.data)
        locks = len(self.locks)

        ev, ex, er = self._evicted, self._expired, self._errors
        self._evicted = 0
        self._expired = 0
        self._errors = 0

        self._log(
            f"[cache][DEV] alive | items={items} locks={locks} | mem={mb:.2f}MB/{limit:.0f}MB | evicted={ev} expired={ex} errors={er}"
        )

    def _write_dump(self) -> None:
        try:
            snap = {
                "v": 1,
                "ts": time.time(),
                "total_bytes": self.total_bytes,
                # только cache bytes+мета (locks не дампим)
                "items": {
                    k: {
                        "payload": e.payload,
                        "size": e.size,
                        "expire_at": e.expire_at,
                        "last_access": e.last_access,
                    }
                    for k, e in self.data.items()
                },
            }
            raw = self._safe_dump_pickle(snap)
            tmp = self.dump_path.with_suffix(".dump.tmp")
            tmp.write_bytes(raw)
            os.replace(tmp, self.dump_path)
        except Exception:
            self._errors += 1

    def _try_restore_dump(self) -> None:
        try:
            if not self.dump_path.exists():
                return
            raw = self.dump_path.read_bytes()
            snap = self._safe_load_pickle(raw)
            if not isinstance(snap, dict) or snap.get("v") != 1:
                return

            items = snap.get("items")
            if not isinstance(items, dict):
                return

            now = self._now()
            restored = 0
            total = 0
            data: dict[str, _Entry] = {}

            for k, v in items.items():
                if not isinstance(k, str) or not isinstance(v, dict):
                    continue
                payload = v.get("payload")
                size = v.get("size")
                expire_at = v.get("expire_at")
                last_access = v.get("last_access")

                if not isinstance(payload, (bytes, bytearray)):
                    continue
                payload = bytes(payload)

                if not isinstance(size, int):
                    continue
                if size != len(payload):
                    continue
                if size <= 0 or size > MAX_VALUE_BYTES:
                    continue

                if not isinstance(expire_at, (int, float)):
                    continue
                if not isinstance(last_access, (int, float)):
                    continue

                if now >= float(expire_at):
                    continue

                e = _Entry(payload=payload, size=size, expire_at=float(expire_at), last_access=float(last_access))
                data[k] = e
                total += size
                restored += 1

            self.data = data
            self.total_bytes = total

            # locks никогда не восстанавливаем
            self.locks = {}

            # rebuild heap
            self._exp_heap = []
            for k, e in self.data.items():
                self._push_expire(kind=0, key=k, expire_at=e.expire_at)

            if self.total_bytes > MAX_CACHE_BYTES:
                self._gc()

            self._log(f"[cache][DEV] restore: restored_items={restored}, mem={self.total_bytes/1024/1024:.2f}MB")
        except Exception:
            self._errors += 1
        finally:
            try:
                self.dump_path.unlink(missing_ok=True)
            except Exception:
                pass

    def _watchdog_loop(self) -> None:
        while not self._stop:
            time.sleep(2)
            now = self._now()
            if (now - self._last_heartbeat) > WATCHDOG_STALL_SEC:
                self._log(f"[cache][DEV] WATCHDOG: stalled > {WATCHDOG_STALL_SEC}s, exiting")
                os._exit(2)

    def _recv_exact(self, conn: socket.socket, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("closed")
            buf += chunk
        return buf

    def _recv_msg(self, conn: socket.socket) -> Any:
        hdr = self._recv_exact(conn, 4)
        (ln,) = struct.unpack("!I", hdr)
        if ln <= 0 or ln > MAX_REQUEST_BYTES:
            raise ValueError("bad_len")
        raw = self._recv_exact(conn, ln)
        return self._safe_load_pickle(raw)

    def _send_msg(self, conn: socket.socket, obj: Any) -> None:
        raw = self._safe_dump_pickle(obj)
        conn.sendall(struct.pack("!I", len(raw)) + raw)

    def _new_token(self) -> str:
        return os.urandom(16).hex()

    def _handle(self, req: Any) -> dict[str, Any]:
        try:
            if not isinstance(req, dict):
                return {"ok": False, "err": "bad_req"}

            op = req.get("op")

            if op == "STATS":
                self._expire_pop()
                return {
                    "ok": True,
                    "items": len(self.data),
                    "locks": len(self.locks),
                    "total_bytes": self.total_bytes,
                    "max_bytes": MAX_CACHE_BYTES,
                }

            key = req.get("key")
            if not isinstance(key, str) or not key:
                return {"ok": False, "err": "bad_key"}

            ttl_sec = req.get("ttl_sec", DEFAULT_TTL_SEC)
            try:
                ttl = float(ttl_sec)
            except Exception:
                ttl = 0.0
            if ttl <= 0:
                return {"ok": False, "err": "bad_ttl"}

            now = self._now()

            # -------------------- LOCKS (lease) --------------------
            if op in ("LOCK_TRY", "LOCK_RENEW", "LOCK_RELEASE", "LOCK_STATUS"):
                self._expire_pop()

                if op == "LOCK_STATUS":
                    cur = self.locks.get(key)
                    if not cur or now >= cur.expire_at:
                        if cur:
                            self._drop_lock(key)
                        return {"ok": True, "held": False}
                    return {"ok": True, "held": True, "owner": cur.owner, "token": cur.token, "expire_at": cur.expire_at}

                if op == "LOCK_TRY":
                    owner = req.get("owner")
                    if not isinstance(owner, str) or not owner:
                        return {"ok": False, "err": "bad_owner"}

                    cur = self.locks.get(key)
                    if cur and now < cur.expire_at:
                        return {"ok": True, "acquired": False, "owner": cur.owner, "token": cur.token, "expire_at": cur.expire_at}

                    token = self._new_token()
                    expire_at = now + ttl
                    self.locks[key] = _Lease(owner=owner, token=token, expire_at=expire_at)
                    self._push_expire(kind=1, key=key, expire_at=expire_at)
                    return {"ok": True, "acquired": True, "owner": owner, "token": token, "expire_at": expire_at}

                if op == "LOCK_RENEW":
                    token = req.get("token")
                    if not isinstance(token, str) or not token:
                        return {"ok": False, "err": "bad_token"}

                    cur = self.locks.get(key)
                    if not cur or now >= cur.expire_at:
                        if cur:
                            self._drop_lock(key)
                        return {"ok": True, "renewed": False, "reason": "not_held"}

                    if cur.token != token:
                        return {"ok": True, "renewed": False, "reason": "token_mismatch", "owner": cur.owner, "expire_at": cur.expire_at}

                    cur.expire_at = now + ttl
                    self._push_expire(kind=1, key=key, expire_at=cur.expire_at)
                    return {"ok": True, "renewed": True, "expire_at": cur.expire_at}

                if op == "LOCK_RELEASE":
                    token = req.get("token")
                    if not isinstance(token, str) or not token:
                        return {"ok": False, "err": "bad_token"}

                    cur = self.locks.get(key)
                    if not cur or now >= cur.expire_at:
                        if cur:
                            self._drop_lock(key)
                        return {"ok": True, "released": False, "reason": "not_held"}

                    if cur.token != token:
                        return {"ok": True, "released": False, "reason": "token_mismatch", "owner": cur.owner, "expire_at": cur.expire_at}

                    self._drop_lock(key)
                    return {"ok": True, "released": True}

                return {"ok": False, "err": "unknown_op"}

            # -------------------- CACHE --------------------
            self._expire_pop()

            if op == "GET":
                e = self.data.get(key)
                if not e:
                    return {"ok": True, "hit": False}

                if now >= e.expire_at:
                    self._drop(key)
                    self._expired += 1
                    return {"ok": True, "hit": False}

                # sliding TTL
                e.last_access = now
                e.expire_at = now + ttl
                self._push_expire(kind=0, key=key, expire_at=e.expire_at)
                return {"ok": True, "hit": True, "payload": e.payload}

            if op == "SET":
                payload = req.get("payload")
                if not isinstance(payload, (bytes, bytearray)):
                    return {"ok": False, "err": "bad_payload"}
                payload = bytes(payload)
                size = len(payload)

                if size <= 0 or size > MAX_VALUE_BYTES:
                    return {"ok": True, "stored": False, "reason": "too_big"}

                expire_at = now + ttl

                old = self.data.get(key)
                if old:
                    self.total_bytes -= old.size

                self.data[key] = _Entry(payload=payload, size=size, expire_at=expire_at, last_access=now)
                self.total_bytes += size
                self._push_expire(kind=0, key=key, expire_at=expire_at)

                if self.total_bytes > MAX_CACHE_BYTES:
                    self._gc()

                return {"ok": True, "stored": True}

            return {"ok": False, "err": "unknown_op"}
        except Exception:
            self._errors += 1
            return {"ok": False, "err": "server_error"}

    def _on_signal(self, signum: int, _frame: Any) -> None:
        self._log(f"[cache][DEV] signal={signum}, shutting down (dump.)")
        self._stop = True

    def _handle_one_conn(self, conn: socket.socket) -> None:
        with conn:
            try:
                conn.settimeout(CONN_TIMEOUT_SEC)
                req = self._recv_msg(conn)
                resp = self._handle(req)
                self._send_msg(conn, resp)
            except (BrokenPipeError, ConnectionError, socket.timeout):
                # клиент мог закрыть/тайм-аутнуться -> это НОРМА, не считаем ошибкой и не логируем
                return
            except Exception as e:
                self._errors += 1
                try:
                    self._log(f"[cache][DEV] error: {type(e).__name__}: {e}")
                except Exception:
                    pass
                try:
                    self._send_msg(conn, {"ok": False, "err": "io_error"})
                except Exception:
                    pass

    def serve_forever(self) -> None:
        self._log(
            "[cache][DEV] starting (DEV MODE) | "
            f"watchdog={WATCHDOG_STALL_SEC}s | alive_log={ALIVE_EVERY_SEC}s | "
            f"max_obj={MAX_VALUE_BYTES//1024}KB | max_mem={MAX_CACHE_BYTES//1024//1024}MB | "
            f"gc_to={int(GC_TARGET_RATIO*100)}% | ttl_default=7d | locks=lease"
        )
        self._log("[cache][DEV] NOTE: for production this must be replaced/disabled")

        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

        self._try_restore_dump()

        try:
            if self.sock_path.exists():
                self.sock_path.unlink()
        except Exception:
            pass

        t = threading.Thread(target=self._watchdog_loop, daemon=True)
        t.start()

        sel = selectors.DefaultSelector()

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.bind(str(self.sock_path))
            os.chmod(str(self.sock_path), 0o666)
            s.listen(128)
            s.setblocking(False)

            sel.register(s, selectors.EVENT_READ)

            try:
                while not self._stop:
                    self._beat()

                    # housekeeping by deadlines (expire heap + alive)
                    self._expire_pop()
                    self._alive_log()

                    deadline = self._next_deadline()
                    now = self._now()
                    if deadline is None:
                        timeout = MAX_IDLE_WAIT_SEC
                    else:
                        timeout = max(0.0, min(float(deadline - now), MAX_IDLE_WAIT_SEC))

                    events = sel.select(timeout)
                    for key, _mask in events:
                        if key.fileobj is not s:
                            continue

                        # дренируем очередь accept за один select
                        while True:
                            try:
                                conn, _addr = s.accept()
                            except BlockingIOError:
                                break
                            except Exception:
                                self._errors += 1
                                break

                            self._handle_one_conn(conn)

            finally:
                try:
                    sel.unregister(s)
                except Exception:
                    pass
                sel.close()

        try:
            self._write_dump()
        finally:
            try:
                if self.sock_path.exists():
                    self.sock_path.unlink()
            except Exception:
                pass
            self._log("[cache][DEV] stopped")


def main() -> None:
    CacheDaemon().serve_forever()


if __name__ == "__main__":
    main()
