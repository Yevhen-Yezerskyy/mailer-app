# FILE: engine/core_imap/imap_bounce.py
# DATE: 2026-04-20
# PURPOSE: Scan recent IMAP mailboxes for Serenity-related letters, move outloop to SerenityMailer,
#          and avoid re-reading already processed UIDs via Redis cache.

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, Optional

from engine.common import db
from engine.common.cache.client import CLIENT
from engine.common.gpt import GPTClient
from engine.common.mail.imap import IMAPConn
from engine.common.translate import get_prompt
from engine.core_imap.imap_message import process_imap_message

SERENITY_FOLDER = "SerenityMailer"
DEFAULT_WINDOW_DAYS = 7
DEFAULT_UID_BATCH_SIZE = 200


def _window_days() -> int:
    raw = (os.environ.get("IMAP_BOUNCE_WINDOW_DAYS") or "").strip()
    if raw.isdigit() and int(raw) > 0:
        return int(raw)
    return DEFAULT_WINDOW_DAYS


def _uid_batch_size() -> int:
    raw = (os.environ.get("IMAP_BOUNCE_UID_BATCH_SIZE") or "").strip()
    if raw.isdigit() and int(raw) > 0:
        return int(raw)
    return DEFAULT_UID_BATCH_SIZE


def _imap_since(days_back: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=int(days_back))
    return dt.strftime("%d-%b-%Y")


def _recent_mailbox_ids(days_back: int) -> list[int]:
    rows = db.fetch_all(
        """
        WITH recent_campaigns AS (
            SELECT DISTINCT campaign_id
            FROM public.sending_log
            WHERE campaign_id IS NOT NULL
              AND created_at >= now() - (%s::int * interval '1 day')
        )
        SELECT DISTINCT c.mailbox_id
        FROM recent_campaigns rc
        JOIN public.campaigns_campaigns c
          ON c.id = rc.campaign_id
        JOIN public.aap_settings_imap_mailboxes im
          ON im.mailbox_id = c.mailbox_id
        WHERE c.mailbox_id IS NOT NULL
        ORDER BY c.mailbox_id
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


def _ensure_serenity_folder(conn: IMAPConn, folders: Iterable[str]) -> bool:
    for raw in folders:
        _flags, name = _parse_list_row(str(raw))
        if name.lower() == SERENITY_FOLDER.lower():
            return True
    created = conn.create_mailbox(SERENITY_FOLDER)
    return created is not None


def _base_scan_folders(rows: Iterable[str]) -> list[str]:
    out: list[str] = []
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
        if is_inbox or is_junk:
            out.append(name)
            seen.add(low)

    if "inbox" not in seen:
        out.insert(0, "INBOX")
    return out


def _safe_json_load(raw: str) -> Optional[dict[str, Any]]:
    txt = (raw or "").strip()
    if not txt:
        return None
    try:
        obj = json.loads(txt)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    lpos = txt.find("{")
    rpos = txt.rfind("}")
    if lpos < 0 or rpos <= lpos:
        return None
    try:
        obj2 = json.loads(txt[lpos : rpos + 1])
        return obj2 if isinstance(obj2, dict) else None
    except Exception:
        return None


def _gpt_pick_scan_folders(all_folders: list[str]) -> list[str]:
    if not all_folders:
        return []

    instructions = (get_prompt("imap_bounce_select_folders") or "").strip()
    if not instructions:
        return []
    inp = json.dumps({"folders": all_folders}, ensure_ascii=False)

    try:
        resp = GPTClient().ask(
            model="gpt-5.4-mini",
            instructions=instructions,
            input=inp,
            user_id="engine.imap.folders",
            service_tier="flex",
            web_search=False,
        )
    except Exception:
        return []

    data = _safe_json_load(str(resp.content or ""))
    if not data:
        return []

    raw_folders = data.get("folders")
    if not isinstance(raw_folders, list):
        return []

    allowed = {f.lower(): f for f in all_folders}
    out: list[str] = []
    seen: set[str] = set()
    for item in raw_folders:
        name = str(item or "").strip()
        if not name:
            continue
        low = name.lower()
        if low in seen:
            continue
        real_name = allowed.get(low)
        if not real_name:
            continue
        if low == SERENITY_FOLDER.lower():
            continue
        seen.add(low)
        out.append(real_name)
    return out


def _merge_folders(base: list[str], extra: list[str], all_folders: list[str]) -> list[str]:
    allowed = {f.lower(): f for f in all_folders}
    out: list[str] = []
    seen: set[str] = set()
    for candidate in list(base) + list(extra):
        low = str(candidate or "").strip().lower()
        if not low or low in seen:
            continue
        real_name = allowed.get(low)
        if not real_name:
            continue
        seen.add(low)
        out.append(real_name)
    return out


def _chunks(items: list[str], size: int) -> Iterator[list[str]]:
    if size <= 0:
        size = DEFAULT_UID_BATCH_SIZE
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _folder_hash(folder: str) -> str:
    return hashlib.sha1(str(folder).lower().encode("utf-8", errors="replace")).hexdigest()[:16]


def _done_cache_key(mailbox_id: int, folder: str, uid: str) -> str:
    return f"imap:bounce:done:v1:{int(mailbox_id)}:{_folder_hash(folder)}:{str(uid)}"


def _is_done(mailbox_id: int, folder: str, uid: str, ttl_sec: int) -> bool:
    try:
        payload = CLIENT.get(_done_cache_key(mailbox_id, folder, uid), ttl_sec=ttl_sec)
        return payload is not None
    except Exception:
        return False


def _mark_done(mailbox_id: int, folder: str, uid: str, ttl_sec: int) -> None:
    try:
        CLIENT.set(_done_cache_key(mailbox_id, folder, uid), b"1", ttl_sec=ttl_sec)
    except Exception:
        return


def _process_mailbox(mailbox_id: int, days_back: int) -> Dict[str, Any]:
    conn = IMAPConn(mailbox_id, cache_key=f"imap-bounce:{mailbox_id}")
    ttl_sec = int(days_back) * 86400
    out: Dict[str, Any] = {
        "mailbox_id": int(mailbox_id),
        "folders": 0,
        "seen": 0,
        "cached_skip": 0,
        "moved": 0,
        "ours": 0,
        "outloop": 0,
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

        all_folders = []
        for raw in folder_rows:
            _flags, name = _parse_list_row(str(raw))
            if name:
                all_folders.append(name)

        base_folders = _base_scan_folders(folder_rows)
        gpt_folders = _gpt_pick_scan_folders(all_folders)
        folders = _merge_folders(base_folders, gpt_folders, all_folders)
        out["folders"] = len(folders)

        for folder in folders:
            sel = conn.select(folder, readonly=False)
            if sel is None:
                out["errors"].append({"folder": folder, "error": "select_failed"})
                continue

            uids = conn.uid_search(f"SINCE {_imap_since(days_back)}") or []
            if not uids:
                continue

            for uid_batch in _chunks(uids, _uid_batch_size()):
                for uid in uid_batch:
                    if _is_done(mailbox_id, folder, uid, ttl_sec):
                        out["cached_skip"] += 1
                        continue

                    raw_msg = conn.uid_fetch_rfc822(uid)
                    if raw_msg is None:
                        out["errors"].append({"folder": folder, "uid": uid, "error": "fetch_failed"})
                        continue

                    out["seen"] += 1
                    res = process_imap_message(mailbox_id, folder, uid, raw_msg)

                    if res.get("is_ours"):
                        out["ours"] += 1
                    if str(res.get("kind") or "").startswith("OUTLOOP_"):
                        out["outloop"] += 1

                    if res.get("move_to_serenity"):
                        moved = conn.uid_move(uid, SERENITY_FOLDER)
                        if moved is None:
                            out["errors"].append(
                                {
                                    "folder": folder,
                                    "uid": uid,
                                    "error": "move_failed",
                                    "kind": res.get("kind"),
                                }
                            )
                            continue
                        out["moved"] += 1

                    if res.get("updated_bad_address"):
                        out["blocked"] += 1

                    if res.get("cache_done", True):
                        _mark_done(mailbox_id, folder, uid, ttl_sec)
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
        "uid_batch_size": _uid_batch_size(),
        "mailboxes_total": len(mailbox_ids),
        "mailboxes": [],
        "seen": 0,
        "cached_skip": 0,
        "moved": 0,
        "ours": 0,
        "outloop": 0,
        "blocked": 0,
    }
    for mailbox_id in mailbox_ids:
        one = _process_mailbox(mailbox_id, days_back)
        result["mailboxes"].append(one)
        result["seen"] += int(one.get("seen", 0))
        result["cached_skip"] += int(one.get("cached_skip", 0))
        result["moved"] += int(one.get("moved", 0))
        result["ours"] += int(one.get("ours", 0))
        result["outloop"] += int(one.get("outloop", 0))
        result["blocked"] += int(one.get("blocked", 0))
    return result


def main() -> None:
    print(json.dumps(task_imap_bounce_scan_once(), ensure_ascii=False))


if __name__ == "__main__":
    main()
