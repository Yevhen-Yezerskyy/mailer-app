# FILE: engine/core_crawler/browser/broker_client.py
# DATE: 2026-03-27
# PURPOSE: Blocking unix-socket client for the local core_crawler browser broker.

from __future__ import annotations

import json
import socket
import struct
import time
from typing import Any

from engine.core_crawler.browser.broker_server import BROKER_SOCKET_PATH
from engine.core_crawler.browser.session_router import FetchResult

BROKER_TIMEOUT_SEC = 180.0
BROKER_POLL_INTERVAL_SEC = 0.05


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    out = bytearray()
    need = int(size)
    while len(out) < need:
        chunk = sock.recv(need - len(out))
        if not chunk:
            raise ConnectionError("socket_closed")
        out.extend(chunk)
    return bytes(out)


def _rpc(payload: dict[str, Any], timeout_sec: float = BROKER_TIMEOUT_SEC) -> dict[str, Any]:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(float(timeout_sec))
    try:
        sock.connect(BROKER_SOCKET_PATH)
        raw = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        sock.sendall(struct.pack("!I", len(raw)))
        sock.sendall(raw)
        raw_size = _recv_exact(sock, 4)
        size = struct.unpack("!I", raw_size)[0]
        body = _recv_exact(sock, size)
        return json.loads(body.decode("utf-8"))
    finally:
        sock.close()


def wait_for_broker(timeout_sec: float = 10.0) -> None:
    deadline = time.time() + float(timeout_sec)
    last_error = ""
    while time.time() < deadline:
        try:
            response = _rpc({"action": "ping"}, timeout_sec=1.0)
            if response.get("ok") is True and response.get("pong") is True:
                return
        except Exception as exc:
            last_error = str(exc)
        time.sleep(0.1)
    raise RuntimeError(f"BROKER_NOT_READY {last_error}".strip())


def _submit_fetch(
    *,
    site: str,
    url: str,
    kind: str,
    task_id: int,
    cb_id: int,
    referer: str = "",
    mode: str = "",
) -> str:
    response = _rpc(
        {
            "action": "submit",
            "site": str(site),
            "url": str(url),
            "kind": str(kind),
            "task_id": int(task_id),
            "cb_id": int(cb_id),
            "referer": str(referer or ""),
            "mode": str(mode or ""),
        },
        timeout_sec=5.0,
    )
    if response.get("ok") is not True:
        error = str(response.get("error") or "BROKER_ERROR")
        detail = str(response.get("detail") or "")
        raise RuntimeError(f"{error}: {detail}".strip())
    request_id = str(response.get("request_id") or "")
    if not request_id:
        raise RuntimeError("BROKER_ERROR: EMPTY_REQUEST_ID")
    return request_id


def _poll_fetch_result(request_id: str) -> dict[str, Any]:
    return _rpc(
        {
            "action": "result",
            "request_id": str(request_id or ""),
        },
        timeout_sec=5.0,
    )


def fetch_html_via_broker(
    site: str,
    url: str,
    kind: str,
    task_id: int,
    cb_id: int,
    referer: str = "",
    mode: str = "",
) -> FetchResult:
    request_id = _submit_fetch(
        site=str(site),
        url=str(url),
        kind=str(kind),
        task_id=int(task_id),
        cb_id=int(cb_id),
        referer=str(referer or ""),
        mode=str(mode or ""),
    )
    deadline = time.time() + float(BROKER_TIMEOUT_SEC)
    response: dict[str, Any] | None = None
    while time.time() < deadline:
        response = _poll_fetch_result(request_id)
        if response.get("ok") is True and response.get("pending") is True:
            time.sleep(BROKER_POLL_INTERVAL_SEC)
            continue
        break
    if response is None:
        raise RuntimeError("BROKER_ERROR: EMPTY_RESPONSE")
    if response.get("ok") is True and response.get("pending") is True:
        raise RuntimeError("BROKER_TIMEOUT: RESULT_NOT_READY")
    if response.get("ok") is not True:
        error = str(response.get("error") or "BROKER_ERROR")
        detail = str(response.get("detail") or "")
        raise RuntimeError(f"{error}: {detail}".strip())
    result = dict(response.get("result") or {})
    return FetchResult(
        status=int(result.get("status") or 0),
        url=str(result.get("url") or ""),
        final_url=str(result.get("final_url") or ""),
        html=str(result.get("html") or ""),
        title=str(result.get("title") or ""),
        ms=int(result.get("ms") or 0),
        site=str(result.get("site") or ""),
        session_id=str(result.get("session_id") or ""),
        tunnel=dict(result.get("tunnel") or {}),
    )
