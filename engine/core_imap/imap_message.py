# FILE: engine/core_imap/imap_message.py
# DATE: 2026-04-20
# PURPOSE: Parse one IMAP message, keep only Serenity mails, handle outloop DB updates,
#          and classify non-outloop replies into replay_log.

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from email import policy
from email.message import Message
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any, Dict, Optional

from engine.common import db
from engine.common.gpt import GPTClient
from engine.common.translate import get_prompt

_MAILER_APP_BYTES_RE = re.compile(rb"x-mailer-app\s*:\s*serenity\s+mailer", re.IGNORECASE)
_MAILER_ID_BYTES_RE = re.compile(rb"x-mailer-id\s*:\s*([1-9][0-9]*)", re.IGNORECASE)
_STATUS_LINE_RE = re.compile(r"^\s*status\s*:\s*([245]\.\d{1,3}\.\d{1,3})\b", re.IGNORECASE | re.MULTILINE)
_SMTP_CODE_RE = re.compile(r"\b([245][0-9]{2})\b")
_BOUNCE_MARKER_RE = re.compile(
    r"(mailer-daemon|postmaster|delivery status notification|mail delivery subsystem|undeliverable|delivery failure)",
    re.IGNORECASE,
)
_HTML_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t\r\f\v]+")

_STATUS_BAD_ADDRESS = "BAD_ADDRESS"
_STATUS_AUTO_RESPONSE = "AUTO_RESPONSE"
_STATUS_ANGRY_CONTACT = "ANGRY_CONTACT"
_STATUS_ANSWER = "ANSWER"
_WRONG_EMAIL_REASON = "RETURNED MAIL"
_YES_TOKENS = {"yes", "да"}
_NO_TOKENS = {"no", "нет"}


def _extract_sending_log_id_if_ours(raw_msg: bytes) -> Optional[int]:
    if not _MAILER_APP_BYTES_RE.search(raw_msg):
        return None
    m = _MAILER_ID_BYTES_RE.search(raw_msg)
    if not m:
        return None
    try:
        sending_log_id = int(m.group(1))
    except Exception:
        return None
    return sending_log_id if sending_log_id > 0 else None


def _parse_message(raw_msg: bytes) -> Optional[Message]:
    try:
        return BytesParser(policy=policy.default).parsebytes(raw_msg)
    except Exception:
        return None


def _first_status_code(raw_text: str) -> Optional[str]:
    m = _STATUS_LINE_RE.search(raw_text or "")
    if m:
        return str(m.group(1))

    for line in (raw_text or "").splitlines():
        low = line.strip().lower()
        if not low:
            continue
        if "diagnostic-code:" not in low and "smtp" not in low:
            continue
        m2 = _SMTP_CODE_RE.search(low)
        if m2:
            return str(m2.group(1))
    return None


def _has_dsn_part(msg: Optional[Message]) -> bool:
    if msg is None:
        return False
    for part in msg.walk():
        ctype = (part.get_content_type() or "").lower()
        if ctype in ("message/delivery-status", "message/rfc822"):
            return True
    return False


def _detect_outloop(raw_text: str, msg: Optional[Message]) -> tuple[bool, bool, Optional[str]]:
    status_code = _first_status_code(raw_text)
    if status_code:
        if status_code.startswith("5") or status_code.startswith("5."):
            return True, True, status_code
        if status_code.startswith("4") or status_code.startswith("4."):
            return True, False, status_code

    if _has_dsn_part(msg):
        return True, False, None

    if _BOUNCE_MARKER_RE.search(raw_text or ""):
        return True, False, None

    return False, False, None


def _load_sending_context(sending_log_id: int) -> Optional[tuple[int, Optional[int]]]:
    row = db.fetch_one(
        """
        SELECT id, aggr_contact_cb_id
        FROM public.sending_log
        WHERE id = %s
        LIMIT 1
        """,
        [int(sending_log_id)],
    )
    if not row:
        return None
    aggr_contact_cb_id = None if row[1] is None else int(row[1])
    return int(row[0]), aggr_contact_cb_id


def _apply_hard_bounce_updates(*, sending_log_id: int, aggr_contact_cb_id: Optional[int]) -> bool:
    db.execute(
        """
        UPDATE public.sending_log
        SET status = %s
        WHERE id = %s
        """,
        [_STATUS_BAD_ADDRESS, int(sending_log_id)],
    )

    if aggr_contact_cb_id is None:
        return False

    db.execute(
        """
        UPDATE public.aggr_contacts_cb
        SET wrong_email = true,
            wrong_email_reason = %s,
            updated_at = now()
        WHERE id = %s
        """,
        [_WRONG_EMAIL_REASON, int(aggr_contact_cb_id)],
    )
    db.execute(
        """
        UPDATE public.sending_lists
        SET removed = true,
            updated_at = now()
        WHERE aggr_contact_cb_id = %s
        """,
        [int(aggr_contact_cb_id)],
    )
    return True


def _decode_part_text(part: Message) -> str:
    try:
        payload = part.get_payload(decode=True)
    except Exception:
        payload = None
    if payload is None:
        try:
            text = part.get_content()
            return str(text or "")
        except Exception:
            return ""
    charset = (part.get_content_charset() or "utf-8").strip() or "utf-8"
    try:
        return bytes(payload).decode(charset, errors="replace")
    except Exception:
        return bytes(payload).decode("utf-8", errors="replace")


def _extract_text_parts(msg: Optional[Message]) -> tuple[str, str]:
    if msg is None:
        return "", ""

    html_raw = ""
    text_plain = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = (part.get_content_type() or "").lower()
            if ctype == "text/html" and not html_raw:
                html_raw = _decode_part_text(part)
            elif ctype == "text/plain" and not text_plain:
                text_plain = _decode_part_text(part)
    else:
        ctype = (msg.get_content_type() or "").lower()
        if ctype == "text/html":
            html_raw = _decode_part_text(msg)
        elif ctype == "text/plain":
            text_plain = _decode_part_text(msg)

    return html_raw, text_plain


def _clean_html_to_text(html_raw: str) -> str:
    if not html_raw:
        return ""
    s = _HTML_SCRIPT_STYLE_RE.sub(" ", html_raw)
    s = _HTML_TAG_RE.sub(" ", s)
    s = unescape(s)
    lines = []
    for line in s.splitlines():
        clean = _WS_RE.sub(" ", line).strip()
        if clean:
            lines.append(clean)
    return "\n".join(lines).strip()


def _parse_reply_time(msg: Optional[Message]) -> datetime:
    if msg is None:
        return datetime.now(timezone.utc)
    raw_date = (msg.get("Date") or "").strip()
    if not raw_date:
        return datetime.now(timezone.utc)
    try:
        dt = parsedate_to_datetime(raw_date)
    except Exception:
        dt = None
    if dt is None:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_yes_no(raw: str) -> Optional[bool]:
    token = (str(raw or "").strip().split() or [""])[0].strip().lower().strip(".,!?:;\"'()[]{}")
    if token in _YES_TOKENS:
        return True
    if token in _NO_TOKENS:
        return False
    return None


def _ask_reply_yes_no(*, prompt_key: str, input_text: str, user_id: str) -> bool:
    instructions = (get_prompt(prompt_key) or "").strip()
    if not instructions:
        return False
    try:
        resp = GPTClient().ask(
            model="gpt-5.4-mini",
            instructions=instructions,
            input=input_text,
            user_id=user_id,
            service_tier="flex",
            web_search=False,
        )
    except Exception:
        return False

    yn = _normalize_yes_no(str(resp.content or ""))
    return bool(yn is True)


def _classify_reply_status(text_for_model: str) -> str:
    payload = (text_for_model or "").strip()
    if not payload:
        return _STATUS_ANSWER

    inp = payload
    if len(inp) > 16000:
        inp = inp[:16000]

    is_auto = _ask_reply_yes_no(
        prompt_key="imap_reply_is_autoresponse",
        input_text=inp,
        user_id="engine.imap.reply.is_autoresponse",
    )
    is_angry = _ask_reply_yes_no(
        prompt_key="imap_reply_is_angry",
        input_text=inp,
        user_id="engine.imap.reply.is_angry",
    )

    if is_auto and not is_angry:
        return _STATUS_AUTO_RESPONSE
    if is_angry:
        return _STATUS_ANGRY_CONTACT
    return _STATUS_ANSWER


def _build_replay_json(*, msg: Optional[Message], html_raw: str, html_text_clean: str, text_plain: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if msg is not None:
        for hdr in ("From", "To", "Subject", "Date", "Message-Id", "In-Reply-To", "References"):
            v = (msg.get(hdr) or "").strip()
            if not v:
                continue
            key = hdr.lower().replace("-", "_")
            payload[key] = v
    if html_raw:
        payload["html_raw"] = html_raw
    if html_text_clean:
        payload["html_text_clean"] = html_text_clean
    if text_plain:
        payload["text_plain"] = text_plain
    return payload


def _insert_replay_log(*, sending_log_id: int, status: str, replay_json: Dict[str, Any], replay_time: datetime) -> None:
    db.execute(
        """
        INSERT INTO public.replay_log (sending_log_id, status, replay_json, replay_time)
        VALUES (%s, %s, %s::jsonb, %s)
        """,
        [
            int(sending_log_id),
            str(status),
            json.dumps(replay_json, ensure_ascii=False),
            replay_time,
        ],
    )


def process_imap_message(mailbox_id: int, folder: str, uid: str, raw_msg: bytes) -> Dict[str, Any]:
    raw_text = raw_msg.decode("utf-8", errors="replace")
    sending_log_id = _extract_sending_log_id_if_ours(raw_msg)
    if sending_log_id is None:
        return {
            "kind": "SKIP_NOT_OURS",
            "is_ours": False,
            "move_to_serenity": False,
            "updated_bad_address": False,
            "cache_done": True,
            "folder": folder,
            "uid": uid,
        }

    sending_ctx = _load_sending_context(sending_log_id)
    if sending_ctx is None:
        return {
            "kind": "SKIP_UNKNOWN_SENDING_LOG",
            "is_ours": False,
            "move_to_serenity": False,
            "updated_bad_address": False,
            "cache_done": True,
            "folder": folder,
            "uid": uid,
            "sending_log_id": sending_log_id,
        }

    msg = _parse_message(raw_msg)
    _sl_id, aggr_contact_cb_id = sending_ctx
    is_outloop, is_hard, status_code = _detect_outloop(raw_text, msg)

    if is_outloop:
        updated_bad = False
        if is_hard:
            updated_bad = _apply_hard_bounce_updates(
                sending_log_id=sending_log_id,
                aggr_contact_cb_id=aggr_contact_cb_id,
            )
        return {
            "kind": "OUTLOOP_HARD" if is_hard else "OUTLOOP_SOFT",
            "is_ours": True,
            "move_to_serenity": True,
            "updated_bad_address": bool(updated_bad),
            "cache_done": True,
            "status_code": status_code,
            "folder": folder,
            "uid": uid,
            "sending_log_id": sending_log_id,
            "aggr_contact_cb_id": aggr_contact_cb_id,
        }

    html_raw, text_plain = _extract_text_parts(msg)
    html_text_clean = _clean_html_to_text(html_raw) if html_raw else ""
    text_for_model = html_text_clean or text_plain
    reply_status = _classify_reply_status(text_for_model)
    replay_payload = _build_replay_json(
        msg=msg,
        html_raw=html_raw,
        html_text_clean=html_text_clean,
        text_plain=text_plain,
    )
    replay_time = _parse_reply_time(msg)
    _insert_replay_log(
        sending_log_id=sending_log_id,
        status=reply_status,
        replay_json=replay_payload,
        replay_time=replay_time,
    )

    return {
        "kind": "REPLY_CLASSIFIED",
        "is_ours": True,
        "move_to_serenity": False,
        "updated_bad_address": False,
        "cache_done": True,
        "reply_status": reply_status,
        "folder": folder,
        "uid": uid,
        "sending_log_id": sending_log_id,
        "aggr_contact_cb_id": aggr_contact_cb_id,
    }
