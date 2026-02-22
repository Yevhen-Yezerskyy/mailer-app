"""
FILE: engine/common/logs.py
DATE: 2026-02-22
SUMMARY: Simple print-like file logging wrappers for host/system log roots.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any


HOST_ROOT = Path("/host-logs")
SYS_ROOT = Path("/serenity-logs")


def _safe_part(value: str) -> str:
    part = (value or "").strip().replace("\\", "/")
    part = part.strip("/")
    if not part:
        return ""
    return "/".join(p for p in part.split("/") if p not in ("", ".", ".."))


def _build_path(root: Path, log_file: str, folder: str | None = None) -> Path:
    name = _safe_part(log_file)
    if not name:
        raise ValueError("log_file is required")
    fold = _safe_part(folder or "")
    base = root / fold if fold else root
    return base / name


def _line(message: Any) -> str:
    ts = datetime.now(timezone.utc).isoformat()
    return f"{ts}\t{message}\n"


def _append(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(text)


def log(log_file: str, folder: str | None = None, message: Any = "") -> None:
    """
    Write only to host logs.
    """
    print(message, flush=True)
    path = _build_path(HOST_ROOT, log_file, folder)
    _append(path, _line(message))


def sys_log(log_file: str, folder: str | None = None, message: Any = "") -> None:
    """
    Write to host logs and system logs.
    """
    print(message, flush=True)
    text = _line(message)
    _append(_build_path(HOST_ROOT, log_file, folder), text)
    _append(_build_path(SYS_ROOT, log_file, folder), text)


class HostFilePrintHandler(logging.Handler):
    """
    Logging handler that prints to stdout and appends to /host-logs.
    """

    def __init__(self, log_file: str, folder: str | None = None):
        super().__init__()
        self.log_file = log_file
        self.folder = folder

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            log(self.log_file, self.folder, message)
        except Exception:
            self.handleError(record)
