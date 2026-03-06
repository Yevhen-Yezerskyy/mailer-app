# FILE: engine/core_imap/imap_bounce.py
# DATE: 2026-03-04
# PURPOSE: Scan recent IMAP mailboxes, move obvious mailer trash into "SerenityMailer",
#          and upsert global blocked recipients for hard bounces.

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

from engine.common import db
from engine.common.mail.imap import IMAPConn
from engine.core_imap.imap_message import process_imap_message

SERENITY_FOLDER = "SerenityMailer"
DEFAULT_WINDOW_DAYS = 3
DEFAULT_MAX_UIDS_PER_FOLDER = 200

def _window_days() -> int:
    raw = (os.environ.get("IMAP_BOUNCE_WINDOW_DAYS") or "").strip()
    if raw.isdigit() and int(raw) > 0:
        return int(raw)
    return DEFAULT_WINDOW_DAYS


def _max_uids_per_folder() -> int:
    raw = (os.environ.get("IMAP_BOUNCE_MAX_UIDS") or "").strip()
    if raw.isdigit() and int(raw) > 0:
        return int(raw)
    return DEFAULT_MAX_UIDS_PER_FOLDER


def _imap_since(days_back: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=int(days_back))
    return dt.strftime("%d-%b-%Y")


def _recent_mailbox_ids(days_back: int) -> list[int]:
    rows = db.fetch_all(
        """
        SELECT DISTINCT (ms.data->>'mailbox_id')::bigint AS mailbox_id
        FROM public.mailbox_sent ms
        JOIN public.aap_settings_mailboxes m
          ON m.id = (ms.data->>'mailbox_id')::bigint
        JOIN public.aap_settings_imap_mailboxes im
          ON im.mailbox_id = m.id
         AND im.is_active = true
        WHERE ms.created_at >= now() - (%s::int * interval '1 day')
          AND COALESCE(ms.data->>'mailbox_id', '') ~ '^[0-9]+$'
          AND m.archived = false
        ORDER BY mailbox_id
        """,
        [int(days_back)],
    )
    return [int(r[0]) for r in rows if r and r[0] is not None]


def _parse_list_row(row: str) -> tuple[str, str]:
    s = str(row or "").strip()
    m = re.match(r'^\((?P<flags>[^)]*)\)\s+"[^"]*"\s+(?P<name>.+)$', s)
    if not m:
        return "", s.strip('"')
    name = m.group("name").strip()
    if len(name) >= 2 and name[0] == '"' and name[-1] == '"':
        name = name[1:-1].replace(r"\"", '"')
    return m.group("flags"), name


def _target_scan_folders(rows: Iterable[str]) -> list[str]:
    folders: list[str] = []
    seen: set[str] = set()
    for raw in rows:
        flags, name = _parse_list_row(str(raw))
        if not name:
            continue
        low = name.lower()
        if low == SERENITY_FOLDER.lower():
            continue
        if low in seen:
            continue
        is_inbox = low == "inbox"
        is_junk = "\\junk" in flags.lower() or any(x in low for x in ("spam", "junk", "trash", "papierkorb"))
        is_noise = any(x in low for x in ("sent", "gesendet", "draft", "entw", "archive"))
        if is_noise:
            continue
        if is_inbox or is_junk:
            seen.add(low)
            folders.append(name)
    if "inbox" not in seen:
        folders.insert(0, "INBOX")
    return folders


def _ensure_serenity_folder(conn: IMAPConn, folders: Iterable[str]) -> bool:
    for raw in folders:
        _flags, name = _parse_list_row(str(raw))
        if name.lower() == SERENITY_FOLDER.lower():
            return True
    created = conn.create_mailbox(SERENITY_FOLDER)
    return created is not None


def _process_mailbox(mailbox_id: int, days_back: int) -> Dict[str, Any]:
    conn = IMAPConn(mailbox_id, cache_key=f"imap-bounce:{mailbox_id}")
    out: Dict[str, Any] = {
        "mailbox_id": int(mailbox_id),
        "folders": 0,
        "seen": 0,
        "moved": 0,
        "blocked": 0,
        "errors": [],
    }

    if not conn.conn():
        out["errors"].append(conn.log or {"error": "imap_connect_failed"})
        return out

    try:
        folder_rows = conn.list_mailboxes() or []
        if not _ensure_serenity_folder(conn, folder_rows):
            out["errors"].append({"error": "serenity_folder_create_failed"})
            return out

        folders = _target_scan_folders(folder_rows)
        out["folders"] = len(folders)

        for folder in folders:
            sel = conn.select(folder, readonly=False)
            if sel is None:
                out["errors"].append({"folder": folder, "error": "select_failed"})
                continue

            uids = conn.uid_search(f"SINCE {_imap_since(days_back)}") or []
            if not uids:
                continue

            for uid in uids[-_max_uids_per_folder():]:
                raw_msg = conn.uid_fetch_rfc822(uid)
                if raw_msg is None:
                    out["errors"].append({"folder": folder, "uid": uid, "error": "fetch_failed"})
                    continue

                out["seen"] += 1
                res = process_imap_message(mailbox_id, folder, uid, raw_msg)
                if not res.get("moved"):
                    continue

                moved = conn.uid_move(uid, SERENITY_FOLDER)
                if moved is None:
                    out["errors"].append({"folder": folder, "uid": uid, "error": "move_failed", "kind": res.get("kind")})
                    continue

                out["moved"] += 1
                if res.get("blocked"):
                    out["blocked"] += 1
    finally:
        try:
            conn.close()
        except Exception as e:
            out["errors"].append({"error": f"logout_failed:{type(e).__name__}:{e}"})

    return out


def task_imap_bounce_scan_once() -> Dict[str, Any]:
    days_back = _window_days()
    mailbox_ids = _recent_mailbox_ids(days_back)
    result: Dict[str, Any] = {
        "window_days": days_back,
        "mailboxes_total": len(mailbox_ids),
        "mailboxes": [],
        "seen": 0,
        "moved": 0,
        "blocked": 0,
    }
    for mailbox_id in mailbox_ids:
        one = _process_mailbox(mailbox_id, days_back)
        result["mailboxes"].append(one)
        result["seen"] += int(one.get("seen", 0))
        result["moved"] += int(one.get("moved", 0))
        result["blocked"] += int(one.get("blocked", 0))
    return result


def main() -> None:
    print(json.dumps(task_imap_bounce_scan_once(), ensure_ascii=False))


if __name__ == "__main__":
    main()
