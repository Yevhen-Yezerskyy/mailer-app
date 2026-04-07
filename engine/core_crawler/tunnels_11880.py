# FILE: engine/core_crawler/tunnels_11880.py
# DATE: 2026-03-27
# PURPOSE: SSH SOCKS tunnel manager for 11880 crawler IP pools.

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime
import fcntl
import json
import os
import pickle
import re
import signal
import shlex
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from engine.common.cache.client import CLIENT
from engine.common.logs import log


CONFIG_ENV = "CORE_CRAWLER_TUNNELS_11880_JSON"
RUN_DIR = Path(__file__).resolve().parents[2] / "tmp" / "11880_tunnels"
START_TIMEOUT_SEC = 30.0
STOP_TIMEOUT_SEC = 10.0
WATCH_INTERVAL_SEC = 30.0
TUNNEL_LOCK_WAIT_SEC = 1.0
WATCHDOG_RESTART_MAX_CONCURRENCY = 1
WATCHDOG_RESTART_BACKOFF_SEC = 20.0
WATCHDOG_LOCK_BUSY_BACKOFF_SEC = 3.0
WATCHDOG_SNAPSHOT_SEC = 60.0
LOG_FOLDER = "crawler"
TUNNELS_LOG_FILE = "tunnels.log"
STATE_TTL_SEC = 24 * 60 * 60
TUNNEL_STATUS_KEY = "core_crawler:tunnel_status:global"
_WATCHDOG_THREAD: threading.Thread | None = None
_WATCHDOG_STOP = threading.Event()
_WATCHDOG_LAST_STATE: dict[str, tuple[bool, bool, bool]] = {}
_WATCHDOG_LAST_SNAPSHOT_TS = 0.0
_WATCHDOG_RESTART_MU = threading.Lock()
_WATCHDOG_RESTARTING: set[str] = set()
_WATCHDOG_RESTART_BACKOFF_UNTIL: dict[str, float] = {}


class TunnelLockBusyError(RuntimeError):
    pass


def _load_config() -> dict[str, Any]:
    raw = os.environ.get(CONFIG_ENV, "").strip()
    if not raw:
        raise RuntimeError(f"missing {CONFIG_ENV}")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise RuntimeError("bad config")
    return data


def _get_tunnel(cfg: dict[str, Any], name: str) -> dict[str, Any]:
    for tunnel in list(cfg.get("tunnels") or []):
        if str(tunnel.get("name") or "") == str(name):
            return dict(tunnel)
    raise RuntimeError(f"unknown tunnel: {name}")


def list_tunnels() -> list[dict[str, Any]]:
    cfg = _load_config()
    return [dict(row) for row in list(cfg.get("tunnels") or [])]


def _tunnel_user(cfg: dict[str, Any], tunnel: dict[str, Any]) -> str:
    user = tunnel.get("user") or cfg.get("user") or ""
    user = str(user).strip()
    if not user:
        raise RuntimeError(f"missing user for tunnel {tunnel.get('name')}")
    return user


def _tunnel_password(cfg: dict[str, Any], tunnel: dict[str, Any]) -> str:
    password = tunnel.get("password") or cfg.get("password") or ""
    password = str(password)
    if not password:
        raise RuntimeError(f"missing password for tunnel {tunnel.get('name')}")
    return password


def _ensure_run_dir() -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)


def _meta_path(name: str) -> Path:
    return RUN_DIR / f"{name}.json"


def _log_path(name: str) -> Path:
    return RUN_DIR / f"{name}.log"


def _ctl_path(name: str) -> Path:
    return RUN_DIR / f"{name}.ctl"


def _lock_path(name: str) -> Path:
    _ensure_run_dir()
    path = RUN_DIR / f"{name}.lock"
    return path


@contextmanager
def _tunnel_lock(name: str):
    lock_path = _lock_path(name)
    handle = lock_path.open("a+", encoding="utf-8")
    deadline = time.time() + float(TUNNEL_LOCK_WAIT_SEC)
    try:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                if time.time() >= deadline:
                    raise TunnelLockBusyError(f"lock_busy {name}") from exc
                time.sleep(0.05)
    except Exception:
        handle.close()
        raise
    try:
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _write_meta(name: str, payload: dict[str, Any]) -> None:
    _meta_path(name).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_meta(name: str) -> dict[str, Any]:
    path = _meta_path(name)
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(raw) if isinstance(raw, dict) else {}


def _pid_alive(pid: Any) -> bool:
    try:
        pid_int = int(pid or 0)
    except Exception:
        return False
    if pid_int <= 1:
        return False
    try:
        os.kill(pid_int, 0)
        return True
    except ProcessLookupError:
        return False
    except Exception:
        return False


def _known_tunnel_pids(name: str, local_port: int) -> list[int]:
    pids = set(_listener_pids(local_port))
    meta = _read_meta(name)
    launcher_pid = meta.get("launcher_pid")
    if _pid_alive(launcher_pid):
        try:
            pids.add(int(launcher_pid))
        except Exception:
            pass
    for raw_pid in list(meta.get("listener_pids") or []):
        if _pid_alive(raw_pid):
            try:
                pids.add(int(raw_pid))
            except Exception:
                continue
    return sorted(pid for pid in pids if int(pid) > 1)


def _cache_get_obj(key: str) -> Any:
    payload = CLIENT.get(key, ttl_sec=STATE_TTL_SEC)
    if not payload:
        return None
    try:
        return pickle.loads(payload)
    except Exception as exc:
        raise RuntimeError(f"BAD CACHE PAYLOAD {key}: {type(exc).__name__}: {exc}") from exc


def _cache_set_obj(key: str, value: Any) -> None:
    try:
        payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as exc:
        raise RuntimeError(f"CACHE ENCODE FAILED {key}: {type(exc).__name__}: {exc}") from exc
    CLIENT.set(key, payload, ttl_sec=STATE_TTL_SEC)


def _tail_log(path: Path, max_chars: int = 2000) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="replace")[-max_chars:]
    except Exception:
        return ""


def _log_tunnel(message: str) -> None:
    log(TUNNELS_LOG_FILE, folder=LOG_FOLDER, message=message)


def _short_detail(value: Any, max_len: int = 240) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _watch_state_signature(status: dict[str, Any]) -> tuple[bool, bool, bool]:
    alive = bool(status.get("alive"))
    port_open = bool(status.get("port_open"))
    control_ok = bool(status.get("control_ok"))
    return (alive, port_open, control_ok)


def _site_quarantine_key(site: str) -> str:
    site_name = str(site or "").strip()
    if not site_name:
        raise ValueError("site quarantine key requires site")
    return f"core_crawler:slot_quarantine:{site_name}"


def _window_key(site: str) -> str:
    site_name = str(site or "").strip()
    if not site_name:
        raise ValueError("window key requires site")
    return f"core_crawler:slot_window:{site_name}"


def _format_hhmm(seconds_left: float) -> str:
    total_sec = max(0, int(seconds_left))
    hours = total_sec // 3600
    minutes = (total_sec % 3600) // 60
    return f"{hours:02d}:{minutes:02d}"


def _load_quarantine_snapshot(site: str) -> dict[str, str]:
    raw = _cache_get_obj(_site_quarantine_key(site)) or {}
    if not isinstance(raw, dict):
        return {}
    now = time.time()
    out: dict[str, str] = {}
    for slot_name, until in raw.items():
        try:
            until_ts = float(until or 0.0)
        except Exception:
            continue
        if until_ts <= now:
            continue
        out[str(slot_name or "").strip()] = _format_hhmm(until_ts - now)
    return out


def _load_cooldown_snapshot(site: str) -> dict[str, dict[str, str]]:
    raw = _cache_get_obj(_window_key(site)) or {}
    if not isinstance(raw, dict):
        return {}
    now = time.time()
    berlin_tz = ZoneInfo("Europe/Berlin")
    rows: list[tuple[float, str, dict[str, str]]] = []
    for slot_name, row in raw.items():
        if not isinstance(row, dict):
            continue
        try:
            active_until = float(row.get("active_until") or 0.0)
            cool_until = float(row.get("cool_until") or 0.0)
        except Exception:
            continue
        if active_until > now or cool_until <= now:
            continue
        name = str(slot_name or "").strip()
        if not name:
            continue
        rows.append(
            (
                cool_until,
                name,
                {
                    "until": datetime.fromtimestamp(cool_until, tz=berlin_tz).isoformat(timespec="seconds"),
                    "remaining": _format_hhmm(cool_until - now),
                },
            )
        )
    rows.sort(key=lambda item: (item[0], item[1]))
    return {name: payload for _, name, payload in rows}


def _snapshot_tunnels(status_map: dict[str, dict[str, Any]], configured_names: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for name in configured_names:
        row = dict(status_map.get(name) or {})
        alive = bool(row.get("alive"))
        port_open = bool(row.get("port_open"))
        control_ok = bool(row.get("control_ok"))
        out[name] = {
            "state": "up" if alive else "down",
            "alive": alive,
            "port_open": port_open,
            "control_ok": control_ok,
            "local_port": row.get("local_port"),
        }
    return out


def _snapshot_active(active_names: list[str]) -> dict[str, Any]:
    names = [str(name or "").strip() for name in list(active_names or []) if str(name or "").strip()]
    return {
        "count": len(names),
        "names": names,
    }


def _log_watchdog_snapshot(status_map: dict[str, dict[str, Any]], configured_names: list[str]) -> None:
    try:
        from engine.core_crawler.browser.broker_server import current_site_route_plan

        route_plan = dict(current_site_route_plan() or {})
    except Exception:
        route_plan = {}
    quarantine_11880 = _load_quarantine_snapshot("11880")
    quarantine_gs = _load_quarantine_snapshot("gs")
    alive_count = sum(1 for name in configured_names if bool((status_map.get(name) or {}).get("alive")))
    down_count = max(0, len(configured_names) - alive_count)
    payload = {
        "event": "watch_snapshot",
        "alive": alive_count,
        "down": down_count,
        "cooldown": {
            "11880": _load_cooldown_snapshot("11880"),
        },
        "active": {
            "11880": _snapshot_active(list(route_plan.get("11880") or [])),
            "gs": _snapshot_active(list(route_plan.get("gs") or [])),
        },
        "quarantine": {
            "11880": quarantine_11880,
            "gs": quarantine_gs,
        },
    }
    _log_tunnel(json.dumps(payload, ensure_ascii=False, indent=2))
    cooldown_count = len(payload["cooldown"].get("11880") or {})
    quarantine_count = len(quarantine_11880)
    total_count = len([name for name in configured_names if str(name or "").strip()]) + 1
    _log_tunnel(
        f"Alive: {alive_count}, Cooldown: {cooldown_count}, "
        f"Quarantine: {quarantine_count}, Total: {total_count}"
    )


def load_tunnel_statuses(configured_names: list[str] | tuple[str, ...] | None = None) -> dict[str, dict[str, Any]]:
    raw = _cache_get_obj(TUNNEL_STATUS_KEY) or {}
    if not isinstance(raw, dict):
        raw = {}
    if configured_names is None:
        if raw:
            return {str(name): dict(row or {}) for name, row in raw.items()}
        return refresh_tunnel_statuses()
    allowed: set[str] = set()
    for raw_name in configured_names:
        name = str(raw_name or "").strip()
        if name:
            allowed.add(name)
    filtered = {
        str(name): dict(row or {})
        for name, row in raw.items()
        if str(name or "") in allowed
    }
    missing = [name for name in allowed if name not in filtered]
    if missing:
        refreshed = refresh_tunnel_statuses(list(allowed))
        if refreshed:
            return refreshed
    return filtered


def live_tunnel_names(configured_names: list[str] | tuple[str, ...] | None = None) -> list[str]:
    statuses = load_tunnel_statuses(configured_names)
    live: list[str] = []
    for name, row in statuses.items():
        if bool((row or {}).get("alive")):
            live.append(str(name))
    return live


def _port_open(port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.5)
    try:
        sock.connect(("127.0.0.1", int(port)))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def _cleanup_state(name: str) -> None:
    for path in (_meta_path(name), _ctl_path(name)):
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass


def _listener_pids(local_port: int) -> list[int]:
    proc = subprocess.run(
        ["ss", "-ltnp"],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    out: list[int] = []
    pattern = re.compile(r"pid=(\d+)")
    for line in (proc.stdout or "").splitlines():
        if f":{int(local_port)}" not in line:
            continue
        for match in pattern.findall(line):
            try:
                out.append(int(match))
            except Exception:
                continue
    return sorted(set(out))


def _terminate_pids(pids: list[int]) -> None:
    live = [int(pid) for pid in pids if int(pid) > 1]
    if not live:
        return
    for pid in live:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
        except Exception:
            continue
    deadline = time.time() + 3.0
    while time.time() < deadline:
        still_live: list[int] = []
        for pid in live:
            try:
                os.kill(pid, 0)
                still_live.append(pid)
            except ProcessLookupError:
                continue
            except Exception:
                continue
        if not still_live:
            return
        time.sleep(0.1)
    for pid in still_live:
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            continue


def _wait_port_closed(local_port: int, timeout_sec: float) -> bool:
    deadline = time.time() + max(0.5, float(timeout_sec))
    while time.time() < deadline:
        if not _port_open(local_port):
            return True
        time.sleep(0.1)
    return not _port_open(local_port)


def _cleanup_stale_state(cfg: dict[str, Any], tunnel: dict[str, Any], force: bool = False) -> None:
    name = str(tunnel["name"])
    local_port = int(tunnel["local_port"])
    status = status_tunnel(cfg, tunnel)
    if bool(status.get("alive")) and not force:
        return
    pids = _known_tunnel_pids(name, local_port)
    if pids:
        _log_tunnel(
            f"stale_kill name={name} local_port={local_port} "
            f"reason=stale_tunnel_state pids={','.join(str(pid) for pid in pids)}"
        )
        _terminate_pids(pids)
        _wait_port_closed(local_port, 5.0)
    _cleanup_state(name)


def _ssh_args(user: str, tunnel: dict[str, Any]) -> list[str]:
    host = str(tunnel["host"])
    ssh_port = int(tunnel.get("ssh_port") or 22)
    local_port = int(tunnel["local_port"])
    log_file = str(_log_path(str(tunnel["name"])))
    return [
        "ssh",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "PreferredAuthentications=password",
        "-o",
        "PubkeyAuthentication=no",
        "-o",
        "NumberOfPasswordPrompts=1",
        "-o",
        "LogLevel=ERROR",
        "-E",
        log_file,
        "-p",
        str(ssh_port),
        "-D",
        f"127.0.0.1:{local_port}",
        "-N",
        f"{user}@{host}",
    ]


def _spawn_ssh(user: str, password: str, tunnel: dict[str, Any]) -> subprocess.Popen[bytes]:
    log_path = _log_path(str(tunnel["name"]))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("", encoding="utf-8")
    env = dict(os.environ)
    env["SSHPASS"] = str(password)
    return subprocess.Popen(
        ["sshpass", "-e", *_ssh_args(user, tunnel)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
        start_new_session=True,
        close_fds=True,
    )


def _wait_tunnel_ready(cfg: dict[str, Any], tunnel: dict[str, Any], proc: subprocess.Popen[bytes]) -> tuple[bool, str]:
    deadline = time.time() + START_TIMEOUT_SEC
    log_path = _log_path(str(tunnel["name"]))

    while time.time() < deadline:
        status = status_tunnel(cfg, tunnel)
        if status["alive"]:
            return True, _tail_log(log_path)
        rc = proc.poll()
        transcript = _tail_log(log_path)
        lower = transcript.lower()
        if rc is not None:
            return False, transcript or f"ssh exited rc={rc} before port_open"
        if "permission denied" in lower:
            return False, transcript
        if "could not resolve hostname" in lower:
            return False, transcript
        if "connection refused" in lower:
            return False, transcript
        time.sleep(0.2)
    return False, _tail_log(log_path) or "timeout waiting for port_open"


def _reap_bootstrap_ssh(proc: subprocess.Popen[bytes]) -> None:
    try:
        proc.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        return
    except Exception:
        return


def start_tunnel(cfg: dict[str, Any], tunnel: dict[str, Any]) -> dict[str, Any]:
    name = str(tunnel["name"])
    try:
        with _tunnel_lock(name):
            local_port = int(tunnel["local_port"])
            user = _tunnel_user(cfg, tunnel)
            password = _tunnel_password(cfg, tunnel)
            status = status_tunnel(cfg, tunnel)
            if bool(status.get("alive")):
                return {
                    "name": name,
                    "status": "already_up",
                    "local_port": local_port,
                    "host": tunnel["host"],
                }

            _cleanup_stale_state(cfg, tunnel, force=True)
            if _port_open(local_port):
                detail = f"port_busy_after_cleanup local_port={local_port}"
                _log_tunnel(f"start_fail name={name} host={tunnel['host']} local_port={local_port} detail={json.dumps(_short_detail(detail), ensure_ascii=False)}")
                return {
                    "name": name,
                    "status": "failed",
                    "local_port": local_port,
                    "host": tunnel["host"],
                    "transcript": detail,
                }
            _log_tunnel(
                f"start_begin name={name} host={tunnel['host']} ssh_port={int(tunnel.get('ssh_port') or 22)} "
                f"local_port={local_port} user={user}"
            )
            proc = _spawn_ssh(user, password, tunnel)
            ok, transcript = _wait_tunnel_ready(cfg, tunnel, proc)
            _reap_bootstrap_ssh(proc)

            if not ok:
                try:
                    proc.terminate()
                except Exception:
                    pass
                _cleanup_stale_state(cfg, tunnel, force=True)
                _log_tunnel(
                    f"start_fail name={name} host={tunnel['host']} local_port={local_port} "
                    f"detail={json.dumps(_short_detail(transcript), ensure_ascii=False)}"
                )
                return {
                    "name": name,
                    "status": "failed",
                    "pid": int(proc.pid or 0),
                    "local_port": local_port,
                    "host": tunnel["host"],
                    "transcript": transcript,
                }

            meta = {
                "name": name,
                "host": tunnel["host"],
                "ssh_port": int(tunnel.get("ssh_port") or 22),
                "local_port": local_port,
                "launch_id": uuid4().hex,
                "launcher_pid": int(proc.pid or 0),
                "listener_pids": _listener_pids(local_port),
                "started_at": int(time.time()),
                "cmd": shlex.join(["sshpass", "-e", *_ssh_args(user, tunnel)]),
            }
            _write_meta(name, meta)
            _log_tunnel(f"start_ok name={name} host={tunnel['host']} local_port={local_port}")
            return {
                "name": name,
                "status": "started",
                "local_port": local_port,
                "host": tunnel["host"],
            }
    except TunnelLockBusyError as exc:
        return {
            "name": name,
            "status": "lock_busy",
            "host": tunnel["host"],
            "local_port": int(tunnel["local_port"]),
            "transcript": str(exc),
        }


def stop_tunnel(cfg: dict[str, Any], tunnel: dict[str, Any]) -> dict[str, Any]:
    name = str(tunnel["name"])
    try:
        with _tunnel_lock(name):
            local_port = int(tunnel["local_port"])
            host = str(tunnel["host"])
            pids = _known_tunnel_pids(name, local_port)
            if not pids:
                _cleanup_state(name)
                _log_tunnel(f"stop_skip name={name} status=not_running")
                return {"name": name, "status": "not_running"}
            _terminate_pids(pids)
            _wait_port_closed(local_port, 5.0)
            _cleanup_state(name)
            stopped = not _port_open(local_port)
            _log_tunnel(
                f"stop_done name={name} host={host} local_port={local_port} "
                f"status={'stopped' if stopped else 'stop_failed'}"
            )
            return {
                "name": name,
                "status": "stopped" if stopped else "stop_failed",
                "pids": pids,
            }
    except TunnelLockBusyError as exc:
        return {
            "name": name,
            "status": "lock_busy",
            "host": tunnel["host"],
            "local_port": int(tunnel["local_port"]),
            "stderr": str(exc),
        }


def _process_ok(name: str, local_port: int) -> bool:
    return bool(_known_tunnel_pids(name, local_port))


def status_tunnel(cfg: dict[str, Any], tunnel: dict[str, Any]) -> dict[str, Any]:
    name = str(tunnel["name"])
    local_port = int(tunnel["local_port"])
    port_open = _port_open(local_port)
    control_ok = _process_ok(name, local_port)
    meta = _read_meta(name)
    return {
        "name": name,
        "host": tunnel["host"],
        "local_port": local_port,
        "alive": bool(port_open and control_ok),
        "port_open": port_open,
        "control_ok": control_ok,
        "launch_id": str(meta.get("launch_id") or ""),
        "started_at": int(meta.get("started_at") or 0),
        "listener_pids": _known_tunnel_pids(name, local_port),
        "ctl_path": str(_ctl_path(name)),
        "meta_path": str(_meta_path(name)),
        "log_path": str(_log_path(name)),
    }


def _status_error_row(tunnel: dict[str, Any]) -> dict[str, Any]:
    name = str(tunnel.get("name") or "")
    return {
        "name": name,
        "host": str(tunnel.get("host") or ""),
        "local_port": int(tunnel.get("local_port") or 0),
        "alive": False,
        "port_open": False,
        "control_ok": False,
        "launch_id": "",
        "started_at": 0,
        "listener_pids": [],
        "ctl_path": str(_ctl_path(name)),
        "meta_path": str(_meta_path(name)),
        "log_path": str(_log_path(name)),
    }


def refresh_tunnel_statuses(configured_names: list[str] | tuple[str, ...] | None = None) -> dict[str, dict[str, Any]]:
    cfg = _load_config()
    requested: set[str] | None = None
    if configured_names is not None:
        requested = {str(raw_name or "").strip() for raw_name in configured_names if str(raw_name or "").strip()}
    tunnels = [
        dict(tunnel)
        for tunnel in list(cfg.get("tunnels") or [])
        if requested is None or str(tunnel.get("name") or "") in requested
    ]
    raw = _cache_get_obj(TUNNEL_STATUS_KEY) or {}
    status_map: dict[str, dict[str, Any]] = {}
    if isinstance(raw, dict):
        status_map = {str(name): dict(row or {}) for name, row in raw.items()}
    for tunnel in tunnels:
        name = str(tunnel.get("name") or "")
        if not name:
            continue
        try:
            status = status_tunnel(cfg, tunnel)
        except Exception:
            status = _status_error_row(tunnel)
        status["checked_at"] = float(time.time())
        status_map[name] = dict(status)
    _cache_set_obj(TUNNEL_STATUS_KEY, status_map)
    if requested is None:
        return status_map
    return {name: dict(row or {}) for name, row in status_map.items() if name in requested}


def _watchdog_restart_tunnel(cfg: dict[str, Any], tunnel: dict[str, Any]) -> None:
    name = str(tunnel.get("name") or "")
    try:
        result = start_tunnel(cfg, tunnel)
        status_name = str(result.get("status") or "")
        detail = _short_detail(result.get("transcript") or result.get("stderr") or "")
        _log_tunnel(
            f"watch_restart_result name={name} status={result.get('status')} "
            f"detail={json.dumps(detail, ensure_ascii=False)}"
        )
        now = time.time()
        with _WATCHDOG_RESTART_MU:
            if status_name in {"started", "already_up"}:
                _WATCHDOG_RESTART_BACKOFF_UNTIL.pop(name, None)
            elif status_name == "lock_busy":
                _WATCHDOG_RESTART_BACKOFF_UNTIL[name] = now + float(WATCHDOG_LOCK_BUSY_BACKOFF_SEC)
            else:
                _WATCHDOG_RESTART_BACKOFF_UNTIL[name] = now + float(WATCHDOG_RESTART_BACKOFF_SEC)
    except Exception as exc:
        _log_tunnel(f"watch_restart_result name={name} status=error detail={json.dumps(_short_detail(exc), ensure_ascii=False)}")
        with _WATCHDOG_RESTART_MU:
            _WATCHDOG_RESTART_BACKOFF_UNTIL[name] = time.time() + float(WATCHDOG_RESTART_BACKOFF_SEC)
    finally:
        with _WATCHDOG_RESTART_MU:
            _WATCHDOG_RESTARTING.discard(name)


def _watchdog_loop() -> None:
    global _WATCHDOG_LAST_SNAPSHOT_TS
    while not _WATCHDOG_STOP.is_set():
        try:
            loop_now = time.time()
            cfg = _load_config()
            tunnels = list(cfg.get("tunnels") or [])
            configured_names = [
                str(tunnel.get("name") or "").strip()
                for tunnel in tunnels
                if str(tunnel.get("name") or "").strip()
            ]
            raw = _cache_get_obj(TUNNEL_STATUS_KEY) or {}
            status_map: dict[str, dict[str, Any]] = {}
            if isinstance(raw, dict):
                status_map = {str(name): dict(row or {}) for name, row in raw.items()}
            for tunnel in tunnels:
                name = str(tunnel.get("name") or "")
                if not name:
                    continue
                try:
                    status = status_tunnel(cfg, tunnel)
                except Exception as exc:
                    status = dict(status_map.get(name) or _status_error_row(tunnel))
                    status["checked_at"] = float(time.time())
                    status_map[name] = dict(status)
                    _cache_set_obj(TUNNEL_STATUS_KEY, status_map)
                    _log_tunnel(
                        f"watch_status_error name={name} local_port={status['local_port']} "
                        f"error={type(exc).__name__}: {exc}"
                    )
                    continue
                status["checked_at"] = float(time.time())
                status_map[name] = dict(status)
                _cache_set_obj(TUNNEL_STATUS_KEY, status_map)
                signature = _watch_state_signature(status)
                if _WATCHDOG_LAST_STATE.get(name) != signature:
                    _WATCHDOG_LAST_STATE[name] = signature
                    _log_tunnel(
                        f"watch_status name={name} alive={status['alive']} port_open={status['port_open']} "
                        f"control_ok={status['control_ok']} local_port={status['local_port']}"
                    )
                if status["alive"]:
                    with _WATCHDOG_RESTART_MU:
                        _WATCHDOG_RESTART_BACKOFF_UNTIL.pop(name, None)
                    continue
                with _WATCHDOG_RESTART_MU:
                    if name in _WATCHDOG_RESTARTING:
                        continue
                    if len(_WATCHDOG_RESTARTING) >= int(WATCHDOG_RESTART_MAX_CONCURRENCY):
                        continue
                    if float(_WATCHDOG_RESTART_BACKOFF_UNTIL.get(name) or 0.0) > loop_now:
                        continue
                    _WATCHDOG_RESTARTING.add(name)
                _log_tunnel(f"watch_restart name={name} local_port={status['local_port']}")
                threading.Thread(
                    target=_watchdog_restart_tunnel,
                    args=(cfg, dict(tunnel)),
                    name=f"core_crawler_tunnel_restart_{name}",
                    daemon=True,
                ).start()
            if (loop_now - float(_WATCHDOG_LAST_SNAPSHOT_TS or 0.0)) >= float(WATCHDOG_SNAPSHOT_SEC):
                _log_watchdog_snapshot(status_map, configured_names)
                _WATCHDOG_LAST_SNAPSHOT_TS = loop_now
            _cache_set_obj(TUNNEL_STATUS_KEY, status_map)
        except Exception as exc:
            _log_tunnel(f"watch_error error={type(exc).__name__}: {exc}")
        _WATCHDOG_STOP.wait(WATCH_INTERVAL_SEC)


def ensure_tunnel_watchdog() -> None:
    global _WATCHDOG_THREAD
    if _WATCHDOG_THREAD is not None and _WATCHDOG_THREAD.is_alive():
        return
    _WATCHDOG_STOP.clear()
    _WATCHDOG_THREAD = threading.Thread(
        target=_watchdog_loop,
        name="core_crawler_tunnels_watchdog",
        daemon=True,
    )
    _WATCHDOG_THREAD.start()
    _log_tunnel("watch_start interval_sec=5")


def stop_tunnel_watchdog() -> None:
    _WATCHDOG_STOP.set()


def ensure_tunnel_up(name: str) -> dict[str, Any]:
    cfg = _load_config()
    tunnel = _get_tunnel(cfg, name)
    return start_tunnel(cfg, tunnel)


def stop_tunnel_by_name(name: str) -> dict[str, Any]:
    cfg = _load_config()
    tunnel = _get_tunnel(cfg, name)
    return stop_tunnel(cfg, tunnel)


def status_tunnel_by_name(name: str) -> dict[str, Any]:
    cfg = _load_config()
    tunnel = _get_tunnel(cfg, name)
    return status_tunnel(cfg, tunnel)


def _pick_tunnels(cfg: dict[str, Any], target: str) -> list[dict[str, Any]]:
    tunnels = list(cfg.get("tunnels") or [])
    if target == "all":
        return tunnels
    for tunnel in tunnels:
        if str(tunnel.get("name")) == target:
            return [tunnel]
    raise SystemExit(f"unknown tunnel: {target}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["start", "stop", "status"])
    parser.add_argument("target", nargs="?", default="all")
    args = parser.parse_args()

    cfg = _load_config()
    tunnels = _pick_tunnels(cfg, str(args.target))

    for tunnel in tunnels:
        if args.action == "start":
            row = start_tunnel(cfg, tunnel)
        elif args.action == "stop":
            row = stop_tunnel(cfg, tunnel)
        else:
            row = status_tunnel(cfg, tunnel)
        print(json.dumps(row, ensure_ascii=False))


if __name__ == "__main__":
    main()
