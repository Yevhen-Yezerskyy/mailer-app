# FILE: engine/core_crawler/tunnels_11880.py
# DATE: 2026-03-27
# PURPOSE: SSH SOCKS tunnel manager for 11880 crawler IP pools.

from __future__ import annotations

import argparse
import json
import os
import pty
import fcntl
import shlex
import socket
import subprocess
import time
from pathlib import Path
from typing import Any


CONFIG_ENV = "CORE_CRAWLER_TUNNELS_11880_JSON"
RUN_DIR = Path(__file__).resolve().parents[2] / "tmp" / "11880_tunnels"
START_TIMEOUT_SEC = 30.0
STOP_TIMEOUT_SEC = 10.0


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


def _write_meta(name: str, payload: dict[str, Any]) -> None:
    _meta_path(name).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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


def _ssh_args(user: str, password: str, tunnel: dict[str, Any]) -> list[str]:
    del password
    host = str(tunnel["host"])
    ssh_port = int(tunnel.get("ssh_port") or 22)
    local_port = int(tunnel["local_port"])
    log_file = str(_log_path(str(tunnel["name"])))
    ctl_path = str(_ctl_path(str(tunnel["name"])))
    return [
        "ssh",
        "-f",
        "-M",
        "-S",
        ctl_path,
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "PreferredAuthentications=password",
        "-o",
        "PubkeyAuthentication=no",
        "-o",
        "NumberOfPasswordPrompts=1",
        "-o",
        "ControlPersist=yes",
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


def _spawn_ssh(user: str, password: str, tunnel: dict[str, Any]) -> tuple[int, int]:
    _ensure_run_dir()
    pid, master_fd = pty.fork()
    if pid == 0:
        os.execvp("ssh", _ssh_args(user, password, tunnel))
    flags = fcntl.fcntl(master_fd, fcntl.F_GETFL)
    fcntl.fcntl(master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    return pid, master_fd


def _read_ready(pid: int, master_fd: int, password: str, local_port: int) -> tuple[bool, str]:
    deadline = time.time() + START_TIMEOUT_SEC
    sent_password = False
    chunks: list[str] = []

    while time.time() < deadline:
        if _port_open(local_port):
            return True, "".join(chunks)[-2000:]

        try:
            data = os.read(master_fd, 4096)
        except BlockingIOError:
            data = b""
        except OSError:
            data = b""

        if data:
            text = data.decode("utf-8", errors="replace")
            chunks.append(text)
            lower = text.lower()
            if "password:" in lower and not sent_password:
                os.write(master_fd, (password + "\n").encode("utf-8"))
                sent_password = True
            if "permission denied" in lower:
                break
            if "connection refused" in lower or "could not resolve hostname" in lower:
                break
            if "backgrounding" in lower:
                time.sleep(0.5)
                if _port_open(local_port):
                    return True, "".join(chunks)[-2000:]
        else:
            time.sleep(0.2)

    return _port_open(local_port), "".join(chunks)[-2000:]


def start_tunnel(cfg: dict[str, Any], tunnel: dict[str, Any]) -> dict[str, Any]:
    name = str(tunnel["name"])
    local_port = int(tunnel["local_port"])
    user = _tunnel_user(cfg, tunnel)
    password = _tunnel_password(cfg, tunnel)
    if _port_open(local_port):
        return {
            "name": name,
            "status": "already_up",
            "local_port": local_port,
            "host": tunnel["host"],
        }

    _cleanup_state(name)
    pid, master_fd = _spawn_ssh(user, password, tunnel)
    ok, transcript = _read_ready(pid, master_fd, password, local_port)
    try:
        os.close(master_fd)
    except Exception:
        pass

    if not ok:
        return {
            "name": name,
            "status": "failed",
            "pid": pid,
            "local_port": local_port,
            "host": tunnel["host"],
            "transcript": transcript,
        }

    meta = {
        "name": name,
        "host": tunnel["host"],
        "ssh_port": int(tunnel.get("ssh_port") or 22),
        "local_port": local_port,
        "bootstrap_pid": pid,
        "started_at": int(time.time()),
        "cmd": shlex.join(_ssh_args(user, password, tunnel)),
    }
    _write_meta(name, meta)
    return {
        "name": name,
        "status": "started",
        "local_port": local_port,
        "host": tunnel["host"],
    }


def stop_tunnel(cfg: dict[str, Any], tunnel: dict[str, Any]) -> dict[str, Any]:
    name = str(tunnel["name"])
    ctl_path = _ctl_path(name)
    if not ctl_path.exists():
        _cleanup_state(name)
        return {"name": name, "status": "not_running"}
    user = _tunnel_user(cfg, tunnel)
    host = str(tunnel["host"])
    ssh_port = int(tunnel.get("ssh_port") or 22)
    cmd = [
        "ssh",
        "-S",
        str(ctl_path),
        "-O",
        "exit",
        "-p",
        str(ssh_port),
        f"{user}@{host}",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=STOP_TIMEOUT_SEC)
    _cleanup_state(name)
    return {
        "name": name,
        "status": "stopped" if proc.returncode == 0 else "stop_failed",
        "stdout": (proc.stdout or "").strip(),
        "stderr": (proc.stderr or "").strip(),
    }


def _control_ok(cfg: dict[str, Any], tunnel: dict[str, Any]) -> bool:
    name = str(tunnel["name"])
    ctl_path = _ctl_path(name)
    if not ctl_path.exists():
        return False
    user = _tunnel_user(cfg, tunnel)
    host = str(tunnel["host"])
    ssh_port = int(tunnel.get("ssh_port") or 22)
    cmd = [
        "ssh",
        "-S",
        str(ctl_path),
        "-O",
        "check",
        "-p",
        str(ssh_port),
        f"{user}@{host}",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
    return proc.returncode == 0


def status_tunnel(cfg: dict[str, Any], tunnel: dict[str, Any]) -> dict[str, Any]:
    name = str(tunnel["name"])
    port_open = _port_open(int(tunnel["local_port"]))
    control_ok = _control_ok(cfg, tunnel)
    return {
        "name": name,
        "host": tunnel["host"],
        "local_port": int(tunnel["local_port"]),
        "alive": bool(port_open or control_ok),
        "port_open": port_open,
        "control_ok": control_ok,
        "ctl_path": str(_ctl_path(name)),
        "meta_path": str(_meta_path(name)),
        "log_path": str(_log_path(name)),
    }


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
