# FILE: config/nginx-worker/processor.py
# DATE: 2026-04-27
# PURPOSE: Nginx cert watcher + daily renew worker (state in Redis, cert/path repair only).

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import time
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from engine.common.cache.client import CLIENT
from engine.common.logs import log
from engine.common.worker import Worker


LOG_FILE = "nginx_worker.log"
LOG_FOLDER = "nginx-worker"

CONF_DIR = Path(os.environ.get("NGINX_CONF_DIR") or "/etc/nginx/conf.d")
LE_LIVE_DIR = Path(os.environ.get("LE_LIVE_DIR") or "/etc/letsencrypt/live")
ACME_ROOT = Path(os.environ.get("ACME_ROOT") or "/var/www/certbot")

WATCH_EVERY_SEC = int((os.environ.get("NGINX_WATCH_EVERY_SEC") or "30").strip() or "30")
RENEW_EVERY_SEC = int((os.environ.get("NGINX_RENEW_EVERY_SEC") or "86400").strip() or "86400")
STATE_TTL_SEC = int((os.environ.get("NGINX_STATE_TTL_SEC") or "604800").strip() or "604800")

STATE_KEY = "nginx_worker:conf_state:v1"
WATCH_LOCK_KEY = "nginx_worker:watch"
RENEW_LOCK_KEY = "nginx_worker:renew"

WATCH_LOCK_TTL_SEC = max(10, WATCH_EVERY_SEC - 5)
RENEW_LOCK_TTL_SEC = 3600

CERTBOT_EMAIL = (os.environ.get("CERTBOT_EMAIL") or "").strip()

_RE_DOMAIN = re.compile(
    r"^(?=.{1,255}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
)
_RE_SERVER_START = re.compile(r"(?m)^[ \t]*server[ \t]*\{")
_RE_SERVER_NAME = re.compile(r"server_name\s+([^;]+);", re.MULTILINE | re.DOTALL)
_RE_SSL_CERT = re.compile(r"(?m)^([ \t]*ssl_certificate[ \t]+)(\S+)([ \t]*;[^\n]*)$")
_RE_SSL_CERT_KEY = re.compile(r"(?m)^([ \t]*ssl_certificate_key[ \t]+)(\S+)([ \t]*;[^\n]*)$")
_RE_LE_FULLCHAIN = re.compile(r"^/etc/letsencrypt/live/([^/]+)/fullchain\.pem$")
_RE_LE_PRIVKEY = re.compile(r"^/etc/letsencrypt/live/([^/]+)/privkey\.pem$")


def _j(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _log(event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    log(LOG_FILE, folder=LOG_FOLDER, message=_j(payload))


def _tail(text: str, max_len: int = 900) -> str:
    if len(text) <= max_len:
        return text
    return text[-max_len:]


def _acquire_lock(lock_key: str, ttl_sec: int) -> Optional[str]:
    owner = f"pid={os.getpid()} ts={int(time.time())}"
    info = CLIENT.lock_try(lock_key, ttl_sec=float(ttl_sec), owner=owner)
    if not info or not info.get("acquired"):
        return None
    token = str(info.get("token") or "")
    return token or None


def _release_lock(lock_key: str, token: Optional[str]) -> None:
    if not token:
        return
    try:
        CLIENT.lock_release(lock_key, token=str(token))
    except Exception:
        pass


def _list_conf_files() -> List[Path]:
    if not CONF_DIR.is_dir():
        return []
    return sorted(p for p in CONF_DIR.glob("*.conf") if p.is_file())


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _build_conf_state() -> Dict[str, Any]:
    files: Dict[str, str] = {}
    for path in _list_conf_files():
        data = path.read_bytes()
        files[str(path.name)] = sha256(data).hexdigest()
    state_hash = sha256(json.dumps(files, sort_keys=True).encode("utf-8")).hexdigest()
    return {"hash": state_hash, "files": files}


def _load_state() -> Optional[Dict[str, Any]]:
    raw = CLIENT.get(STATE_KEY, ttl_sec=STATE_TTL_SEC)
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def _save_state(state: Dict[str, Any]) -> None:
    payload = json.dumps(state, sort_keys=True).encode("utf-8")
    CLIENT.set(STATE_KEY, payload, ttl_sec=STATE_TTL_SEC)


def _iter_server_blocks(text: str) -> List[Tuple[int, int, str]]:
    out: List[Tuple[int, int, str]] = []
    pos = 0
    total = len(text)
    while pos < total:
        match = _RE_SERVER_START.search(text, pos)
        if not match:
            break
        brace_idx = text.find("{", match.start(), match.end() + 4)
        if brace_idx < 0:
            break
        depth = 0
        end_idx = -1
        i = brace_idx
        while i < total:
            ch = text[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end_idx = i + 1
                    break
            i += 1
        if end_idx < 0:
            break
        out.append((match.start(), end_idx, text[match.start():end_idx]))
        pos = end_idx
    return out


def _extract_server_domains(block_text: str) -> List[str]:
    domains: List[str] = []
    for m in _RE_SERVER_NAME.finditer(block_text):
        raw = m.group(1)
        for token in raw.split():
            d = token.strip().lower()
            if not d or d == "_" or d.startswith("~"):
                continue
            if _RE_DOMAIN.fullmatch(d):
                if d not in domains:
                    domains.append(d)
    return domains


def _cert_name_from_fullchain(path: str) -> str:
    m = _RE_LE_FULLCHAIN.fullmatch(path.strip())
    return (m.group(1) if m else "").strip()


def _cert_name_from_privkey(path: str) -> str:
    m = _RE_LE_PRIVKEY.fullmatch(path.strip())
    return (m.group(1) if m else "").strip()


def _cert_paths(cert_name: str) -> Tuple[Path, Path]:
    base = LE_LIVE_DIR / cert_name
    return base / "fullchain.pem", base / "privkey.pem"


def _cert_exists(cert_name: str) -> bool:
    if not cert_name:
        return False
    fullchain, privkey = _cert_paths(cert_name)
    return fullchain.is_file() and privkey.is_file()


def _replace_ssl_line(block_text: str, pattern: re.Pattern[str], new_path: str) -> str:
    def _repl(m: re.Match[str]) -> str:
        return f"{m.group(1)}{new_path}{m.group(3)}"

    return pattern.sub(_repl, block_text, count=1)


def _issue_cert(cert_name: str, domains: Iterable[str]) -> bool:
    domains_list: List[str] = []
    for d in domains:
        dd = str(d).strip().lower()
        if dd and _RE_DOMAIN.fullmatch(dd) and dd not in domains_list:
            domains_list.append(dd)

    if not cert_name:
        _log("cert_issue_skip", reason="empty_cert_name")
        return False
    if not domains_list:
        _log("cert_issue_skip", reason="empty_domains", cert_name=cert_name)
        return False
    if not CERTBOT_EMAIL:
        _log("cert_issue_skip", reason="missing_CERTBOT_EMAIL", cert_name=cert_name)
        return False

    cmd = [
        "certbot",
        "certonly",
        "--webroot",
        "-w",
        str(ACME_ROOT),
        "--non-interactive",
        "--agree-tos",
        "--keep-until-expiring",
        "--expand",
        "--email",
        CERTBOT_EMAIL,
        "--cert-name",
        cert_name,
    ]
    for d in domains_list:
        cmd.extend(["-d", d])

    proc = subprocess.run(cmd, capture_output=True, text=True)
    _log(
        "cert_issue",
        cert_name=cert_name,
        domains=domains_list,
        rc=proc.returncode,
        stdout_tail=_tail(proc.stdout or ""),
        stderr_tail=_tail(proc.stderr or ""),
    )
    return proc.returncode == 0


def _repair_block(block_text: str, file_name: str) -> Tuple[str, bool, List[str]]:
    cert_m = _RE_SSL_CERT.search(block_text)
    key_m = _RE_SSL_CERT_KEY.search(block_text)
    if not cert_m and not key_m:
        return block_text, False, []

    issues: List[str] = []
    domains = _extract_server_domains(block_text)

    cert_name_from_cert = _cert_name_from_fullchain(cert_m.group(2)) if cert_m else ""
    cert_name_from_key = _cert_name_from_privkey(key_m.group(2)) if key_m else ""

    candidates: List[str] = []
    for item in [cert_name_from_cert, cert_name_from_key, *domains]:
        val = (item or "").strip().lower()
        if val and val not in candidates:
            candidates.append(val)

    if not candidates:
        issues.append("no_cert_candidate")
        return block_text, False, issues

    chosen = ""
    for cand in candidates:
        if _cert_exists(cand):
            chosen = cand
            break

    if not chosen:
        for cand in candidates:
            _issue_cert(cand, domains or [cand])
            if _cert_exists(cand):
                chosen = cand
                break

    if not chosen:
        issues.append("cert_unresolved")
        _log("repair_block_unresolved", file=file_name, domains=domains, candidates=candidates)
        return block_text, False, issues

    fullchain_path, privkey_path = _cert_paths(chosen)
    expected_fullchain = str(fullchain_path)
    expected_privkey = str(privkey_path)

    new_text = block_text
    changed = False

    if cert_m and cert_m.group(2) != expected_fullchain:
        new_text = _replace_ssl_line(new_text, _RE_SSL_CERT, expected_fullchain)
        changed = True
    if key_m and key_m.group(2) != expected_privkey:
        new_text = _replace_ssl_line(new_text, _RE_SSL_CERT_KEY, expected_privkey)
        changed = True

    if changed:
        _log(
            "repair_block_changed",
            file=file_name,
            cert_name=chosen,
            domains=domains,
            expected_fullchain=expected_fullchain,
            expected_privkey=expected_privkey,
        )

    return new_text, changed, issues


def _repair_file(path: Path) -> Dict[str, Any]:
    original = _read_text(path)
    blocks = _iter_server_blocks(original)
    if not blocks:
        return {"file": path.name, "changed": False, "issues": []}

    parts: List[str] = []
    cursor = 0
    changed = False
    issues: List[str] = []

    for start, end, block in blocks:
        parts.append(original[cursor:start])
        fixed_block, block_changed, block_issues = _repair_block(block, path.name)
        parts.append(fixed_block)
        changed = changed or block_changed
        issues.extend(block_issues)
        cursor = end
    parts.append(original[cursor:])

    new_text = "".join(parts)
    if changed and new_text != original:
        _write_text(path, new_text)

    return {"file": path.name, "changed": changed and (new_text != original), "issues": issues}


def _check_cert_path_integrity() -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for path in _list_conf_files():
        text = _read_text(path)
        for _start, _end, block in _iter_server_blocks(text):
            cert_m = _RE_SSL_CERT.search(block)
            key_m = _RE_SSL_CERT_KEY.search(block)
            if not cert_m and not key_m:
                continue
            cert_path = cert_m.group(2) if cert_m else ""
            key_path = key_m.group(2) if key_m else ""
            cert_ok = bool(cert_path and Path(cert_path).is_file())
            key_ok = bool(key_path and Path(key_path).is_file())
            if cert_ok and key_ok:
                continue
            issues.append(
                {
                    "file": path.name,
                    "cert_path": cert_path,
                    "key_path": key_path,
                    "cert_exists": cert_ok,
                    "key_exists": key_ok,
                }
            )
    return issues


def _reload_nginx() -> bool:
    try:
        cmdline = Path("/proc/1/cmdline").read_bytes()
    except Exception as exc:
        _log("nginx_reload_failed", reason="cmdline_read_error", detail=str(exc))
        return False

    if b"nginx" not in cmdline:
        _log("nginx_reload_failed", reason="pid1_not_nginx")
        return False

    try:
        os.kill(1, signal.SIGHUP)
        _log("nginx_reloaded")
        return True
    except Exception as exc:
        _log("nginx_reload_failed", reason="signal_error", detail=str(exc))
        return False


def task_watch_nginx_conf_once() -> Dict[str, Any]:
    token = _acquire_lock(WATCH_LOCK_KEY, WATCH_LOCK_TTL_SEC)
    if not token:
        _log("watch_skip_locked")
        return {"status": "locked"}

    try:
        current = _build_conf_state()
        previous = _load_state() or {}
        previous_hash = str(previous.get("hash") or "")
        current_hash = str(current.get("hash") or "")

        if previous_hash and previous_hash == current_hash:
            _log("watch_no_changes", state_hash=current_hash)
            return {"status": "no_changes", "state_hash": current_hash}

        prev_files = previous.get("files") if isinstance(previous.get("files"), dict) else {}
        curr_files = current.get("files") if isinstance(current.get("files"), dict) else {}
        changed_files = sorted(
            name
            for name in set(prev_files.keys()) | set(curr_files.keys())
            if prev_files.get(name) != curr_files.get(name)
        )
        _log("watch_changes_detected", changed_files=changed_files, previous_hash=previous_hash, current_hash=current_hash)

        repaired: List[Dict[str, Any]] = []
        for path in _list_conf_files():
            repaired.append(_repair_file(path))

        integrity_issues = _check_cert_path_integrity()
        if integrity_issues:
            _log("integrity_issues", issues=integrity_issues)
        else:
            _log("integrity_ok")

        reload_ok = _reload_nginx()

        final_state = _build_conf_state()
        _save_state(final_state)

        changed_count = sum(1 for item in repaired if item.get("changed"))
        return {
            "status": "processed",
            "changed_files": changed_files,
            "repaired_files": changed_count,
            "integrity_issues": len(integrity_issues),
            "reload_ok": reload_ok,
            "state_hash": final_state.get("hash"),
        }
    finally:
        _release_lock(WATCH_LOCK_KEY, token)


def task_daily_renew_once() -> Dict[str, Any]:
    token = _acquire_lock(RENEW_LOCK_KEY, RENEW_LOCK_TTL_SEC)
    if not token:
        _log("renew_skip_locked")
        return {"status": "locked"}

    try:
        cmd = [
            "certbot",
            "renew",
            "--webroot",
            "-w",
            str(ACME_ROOT),
            "--non-interactive",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""

        _log(
            "renew_run",
            rc=proc.returncode,
            stdout_tail=_tail(stdout),
            stderr_tail=_tail(stderr),
        )

        if proc.returncode != 0:
            return {"status": "renew_failed", "rc": proc.returncode}

        text_all = f"{stdout}\n{stderr}"
        no_change_markers = (
            "No renewals were attempted",
            "No renewals were due",
            "No hooks were run",
        )
        renewed = not any(marker in text_all for marker in no_change_markers)

        reload_ok = False
        if renewed:
            reload_ok = _reload_nginx()
        _log("renew_done", renewed=renewed, reload_ok=reload_ok)

        return {"status": "ok", "renewed": renewed, "reload_ok": reload_ok}
    finally:
        _release_lock(RENEW_LOCK_KEY, token)


def main() -> None:
    worker = Worker(
        name="nginx_worker_processor",
        tick_sec=1,
        max_parallel=1,
    )
    worker.register(
        name="watch_nginx_conf_once",
        fn=task_watch_nginx_conf_once,
        every_sec=max(5, WATCH_EVERY_SEC),
        timeout_sec=max(120, WATCH_EVERY_SEC * 4),
        singleton=True,
        heavy=False,
        priority=10,
    )
    worker.register(
        name="daily_renew_once",
        fn=task_daily_renew_once,
        every_sec=max(60, RENEW_EVERY_SEC),
        timeout_sec=3600,
        singleton=True,
        heavy=False,
        priority=50,
    )
    worker.run_forever()


if __name__ == "__main__":
    main()
