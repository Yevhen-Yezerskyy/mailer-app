# FILE: engine/common/cache/daemon.py  (обновлено — 2025-12-27)
# Смысл: dev cache-daemon (RAM) по UNIX-socket рядом с модулем: TTL sliding, лимиты 128KB/obj и 50MB total,
# GC до 60%, dump/restore рядом, watchdog 60s, alive-лог каждые 10s (evicted/expired/errors).
# (новое — 2025-12-27)
# - Добавлены lease-lock'и (координация воркеров): LOCK_TRY / LOCK_RENEW / LOCK_RELEASE / LOCK_STATUS
# - Locks НЕ участвуют в dump/restore и НЕ учитываются в memory GC: это volatile coordination (TTL-only)

from __future__ import annotations

import os
import pickle
import signal
import socket
import struct
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


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
        print(msg, flush=True)

    def _safe_load_pickle(self, raw: bytes) -> Any:
        return pickle.loads(raw)

    def _safe_dump_pickle(self, obj: Any) -> bytes:
        return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

    def _drop(self, key: str) -> None:
        e = self.data.pop(key, None)
        if e:
            self.total_bytes -= e.size

    def _drop_lock(self, key: str) -> None:
        self.locks.pop(key, None)

    def _expire_sweep(self) -> None:
        now = self._now()

        # cache
        for k, e in list(self.data.items()):
            if now >= e.expire_at:
                self._drop(k)
                self._expired += 1

        # locks (не считаем в expired-метрику кеша — это отдельная сущность)
        for k, l in list(self.locks.items()):
            if now >= l.expire_at:
                self._drop_lock(k)

    def _gc(self) -> None:
        try:
            # 1) TTL sweep (cache + locks)
            self._expire_sweep()

            if self.total_bytes <= MAX_CACHE_BYTES:
                return

            target = int(MAX_CACHE_BYTES * GC_TARGET_RATIO)

            # 2) eviction по размеру (крупные сначала), а при равных — по expire_at (раньше умрет), затем по last_access (старее)
            items = sorted(
                self.data.items(),
                key=lambda kv: (-kv[1].size, kv[1].expire_at, kv[1].last_access),
            )

            for k, e in items:
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

                # не поднимаем протухшее
                if now >= float(expire_at):
                    continue

                data[k] = _Entry(payload=payload, size=size, expire_at=float(expire_at), last_access=float(last_access))
                total += size
                restored += 1

            self.data = data
            self.total_bytes = total

            # locks никогда не восстанавливаем
            self.locks = {}

            # если после восстановления уже выше лимита — сразу чистим
            if self.total_bytes > MAX_CACHE_BYTES:
                self._gc()

            self._log(f"[cache][DEV] restore: restored_items={restored}, mem={self.total_bytes/1024/1024:.2f}MB")
        except Exception:
            self._errors += 1
        finally:
            # по ТЗ: чистим dump даже если не прочитали
            try:
                self.dump_path.unlink(missing_ok=True)
            except Exception:
                pass

    def _watchdog_loop(self) -> None:
        while not self._stop:
            time.sleep(2)
            now = self._now()
            if (now - self._last_heartbeat) > WATCHDOG_STALL_SEC:
                # dev: лучше умереть, чем уложить тазик
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
                # подчистим протухшее (locks тоже)
                self._expire_sweep()
                return {
                    "ok": True,
                    "items": len(self.data),
                    "locks": len(self.locks),
                    "total_bytes": self.total_bytes,
                    "max_bytes": MAX_CACHE_BYTES,
                }

            # ops без key (кроме STATS) не поддерживаем
            key = req.get("key")
            if not isinstance(key, str) or not key:
                return {"ok": False, "err": "bad_key"}

            now = self._now()

            # -------------------- LOCKS (lease) --------------------
            if op in ("LOCK_TRY", "LOCK_RENEW", "LOCK_RELEASE", "LOCK_STATUS"):
                # всегда подчищаем протухшее перед ответом
                self._expire_sweep()

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

                    ttl_sec = req.get("ttl_sec")
                    try:
                        ttl = float(ttl_sec)
                    except Exception:
                        ttl = 0.0
                    if ttl <= 0:
                        return {"ok": False, "err": "bad_ttl"}

                    cur = self.locks.get(key)
                    if cur and now < cur.expire_at:
                        return {
                            "ok": True,
                            "acquired": False,
                            "owner": cur.owner,
                            "token": cur.token,
                            "expire_at": cur.expire_at,
                        }

                    token = self._new_token()
                    expire_at = now + ttl
                    self.locks[key] = _Lease(owner=owner, token=token, expire_at=expire_at)
                    return {"ok": True, "acquired": True, "owner": owner, "token": token, "expire_at": expire_at}

                if op == "LOCK_RENEW":
                    token = req.get("token")
                    if not isinstance(token, str) or not token:
                        return {"ok": False, "err": "bad_token"}

                    ttl_sec = req.get("ttl_sec")
                    try:
                        ttl = float(ttl_sec)
                    except Exception:
                        ttl = 0.0
                    if ttl <= 0:
                        return {"ok": False, "err": "bad_ttl"}

                    cur = self.locks.get(key)
                    if not cur or now >= cur.expire_at:
                        if cur:
                            self._drop_lock(key)
                        return {"ok": True, "renewed": False, "reason": "not_held"}

                    if cur.token != token:
                        return {"ok": True, "renewed": False, "reason": "token_mismatch", "owner": cur.owner, "expire_at": cur.expire_at}

                    cur.expire_at = now + ttl
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
            ttl_sec = req.get("ttl_sec", DEFAULT_TTL_SEC)
            try:
                ttl_sec = int(ttl_sec)
            except Exception:
                ttl_sec = DEFAULT_TTL_SEC
            if ttl_sec <= 0:
                ttl_sec = DEFAULT_TTL_SEC

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
                e.expire_at = now + ttl_sec
                return {"ok": True, "hit": True, "payload": e.payload}

            if op == "SET":
                payload = req.get("payload")
                if not isinstance(payload, (bytes, bytearray)):
                    return {"ok": False, "err": "bad_payload"}
                payload = bytes(payload)
                size = len(payload)

                if size <= 0 or size > MAX_VALUE_BYTES:
                    return {"ok": True, "stored": False, "reason": "too_big"}

                expire_at = now + ttl_sec

                old = self.data.get(key)
                if old:
                    self.total_bytes -= old.size

                self.data[key] = _Entry(payload=payload, size=size, expire_at=expire_at, last_access=now)
                self.total_bytes += size

                if self.total_bytes > MAX_CACHE_BYTES:
                    self._gc()

                return {"ok": True, "stored": True}

            return {"ok": False, "err": "unknown_op"}
        except Exception:
            self._errors += 1
            return {"ok": False, "err": "server_error"}

    def _on_signal(self, signum: int, _frame: Any) -> None:
        self._log(f"[cache][DEV] signal={signum}, shutting down (dump...)")
        self._stop = True

    def serve_forever(self) -> None:
        # banner
        self._log(
            "[cache][DEV] starting (DEV MODE) | "
            f"watchdog={WATCHDOG_STALL_SEC}s | alive_log={ALIVE_EVERY_SEC}s | "
            f"max_obj={MAX_VALUE_BYTES//1024}KB | max_mem={MAX_CACHE_BYTES//1024//1024}MB | "
            f"gc_to={int(GC_TARGET_RATIO*100)}% | ttl_default=7d | locks=lease"
        )
        self._log("[cache][DEV] NOTE: for production this must be replaced/disabled")

        # signals
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

        # restore (и всегда удаляем dump)
        self._try_restore_dump()

        # remove old socket
        try:
            if self.sock_path.exists():
                self.sock_path.unlink()
        except Exception:
            pass

        # watchdog thread
        t = threading.Thread(target=self._watchdog_loop, daemon=True)
        t.start()

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.bind(str(self.sock_path))
            os.chmod(str(self.sock_path), 0o666)
            s.listen(128)
            s.settimeout(1.0)

            while not self._stop:
                self._beat()
                self._alive_log()

                # периодически подчистим протухшее (locks тоже)
                self._expire_sweep()

                try:
                    conn, _ = s.accept()
                except socket.timeout:
                    continue
                except Exception:
                    self._errors += 1
                    continue

                with conn:
                    try:
                        req = self._recv_msg(conn)
                        resp = self._handle(req)
                        self._send_msg(conn, resp)
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

        # graceful shutdown
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
