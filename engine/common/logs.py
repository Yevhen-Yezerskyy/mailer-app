"""
FILE: engine/common/logs.py
DATE: 2026-03-29
SUMMARY: Single file logger writing only into the shared absolute project log root.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LOG_ROOT = Path("/home/eee/mailer-app/logs")


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
        f.flush()


def log(log_file: str, folder: str | None = None, message: Any = "") -> None:
    path = _build_path(LOG_ROOT, log_file, folder)
    _append(path, _line(message))


class HostFilePrintHandler(logging.Handler):
    """
    Logging handler that appends to the shared project log root.
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
