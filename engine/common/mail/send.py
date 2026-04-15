# FILE: engine/common/mail/send.py
# PATH: engine/common/mail/send.py
# DATE: 2026-04-14
# SUMMARY:
# - send_one is the single sender orchestrator: render/template, smrel, SMTP send, sending_log write, status accounting
# - no campaign/contact lookup queries inside; caller passes full campaign/contact payload

from __future__ import annotations

import json
import random
from typing import Any, Dict, List, Optional

from engine.common import db
from engine.common.cache.client import CLIENT
from engine.common.email_template import DEFAULT_VARS, build_send_bodies, build_send_vars_from_contact
from engine.common.mail.logs import log_mail_event
from engine.common.mail.smtp import SMTPConn
from engine.common.utils import safe_dict

_SENDING_LOG_SEQ_NAME: Optional[str] = None
_SMTP_451_STATE_TTL_SEC = 7 * 24 * 60 * 60
_SMTP_451_MAX_FAILS_PER_EMAIL = 3
_SMTP_451_EMAIL_WRONG_REASON = "451 TMP"
_BAD_ID_SENTINEL = 0


def _sending_log_seq_name() -> str:
    global _SENDING_LOG_SEQ_NAME
    if _SENDING_LOG_SEQ_NAME:
        return _SENDING_LOG_SEQ_NAME

    row = db.fetch_one(
        """
        SELECT substring(
            c.column_default
            FROM $$nextval\\('([^']+)'::regclass\\)$$
        )
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.table_name = 'sending_log'
          AND c.column_name = 'id'
        LIMIT 1
        """,
        [],
    )
    if not row or not row[0]:
        raise RuntimeError("SENDING_LOG_SEQUENCE_NOT_FOUND")

    _SENDING_LOG_SEQ_NAME = str(row[0])
    return _SENDING_LOG_SEQ_NAME


def _next_sending_log_id() -> int:
    row = db.fetch_one(
        "SELECT nextval(%s::regclass)",
        [str(_sending_log_seq_name())],
    )
    if not row or row[0] is None:
        raise RuntimeError("SENDING_LOG_NEXTVAL_FAILED")
    return int(row[0])


def _insert_sending_log_row(
    *,
    log_id: int,
    campaign_id: int,
    sending_list_id: int,
    status: str,
    payload: Dict[str, Any],
    processed: bool = True,
) -> None:
    db.fetch_one(
        """
        WITH ins AS (
            INSERT INTO public.sending_log (
                id,
                campaign_id,
                sending_list_id,
                processed,
                status,
                data,
                processed_at
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s::jsonb,
                CASE WHEN %s THEN now() ELSE NULL END
            )
            ON CONFLICT DO NOTHING
            RETURNING campaign_id, processed
        ),
        upd AS (
            UPDATE public.campaigns_campaigns c
            SET sent_num = COALESCE(c.sent_num, 0) + 1,
                updated_at = now()
            FROM ins
            WHERE c.id = ins.campaign_id
              AND ins.processed = true
            RETURNING c.id
        )
        SELECT COUNT(*)::int
        FROM ins
        """,
        (
            int(log_id),
            int(campaign_id),
            int(sending_list_id),
            bool(processed),
            str(status),
            json.dumps(payload, ensure_ascii=False),
            bool(processed),
        ),
    )


def _as_pos_int(value: Any) -> Optional[int]:
    try:
        out = int(value)
    except Exception:
        return None
    return out if out > 0 else None


def _status_from_smtp_code(code: Optional[int]) -> str:
    if code is None:
        return "OTHER"
    if 500 <= int(code) <= 599:
        if int(code) in (550, 551, 553):
            return "BAD_ADDRESS"
        if int(code) == 554:
            return "REPUTATION"
    return "OTHER"


def _smtp_451_count_key(aggr_contact_id: Optional[int], email: str) -> Optional[str]:
    if aggr_contact_id is not None and int(aggr_contact_id) > 0:
        return f"send_one:smtp451:count:aggr:{int(aggr_contact_id)}"
    email_s = str(email or "").strip().lower()
    if email_s:
        return f"send_one:smtp451:count:email:{email_s}"
    return None


def _smtp_451_get_count(aggr_contact_id: Optional[int], email: str) -> int:
    key = _smtp_451_count_key(aggr_contact_id, email)
    if not key:
        return 0
    raw = CLIENT.get(key, ttl_sec=1)
    if raw is None:
        return 0
    try:
        return max(0, int(bytes(raw).decode("utf-8", errors="replace").strip() or "0"))
    except Exception:
        return 0


def _smtp_451_inc_count(aggr_contact_id: Optional[int], email: str) -> int:
    key = _smtp_451_count_key(aggr_contact_id, email)
    if not key:
        return 0
    value = _smtp_451_get_count(aggr_contact_id, email) + 1
    CLIENT.set(key, str(value).encode("utf-8"), ttl_sec=_SMTP_451_STATE_TTL_SEC)
    return value


def _smtp_451_clear_count(aggr_contact_id: Optional[int], email: str) -> None:
    key = _smtp_451_count_key(aggr_contact_id, email)
    if key:
        CLIENT.delete_many([key])


def _mark_aggr_email_wrong(aggr_contact_id: Optional[int], reason: str) -> None:
    if aggr_contact_id is None:
        return
    db.execute(
        """
        UPDATE public.aggr_contacts_cb
        SET wrong_email = true,
            wrong_email_reason = %s,
            updated_at = now()
        WHERE id = %s
        """,
        [str(reason or "").strip() or _SMTP_451_EMAIL_WRONG_REASON, int(aggr_contact_id)],
    )


def send_one(
    *,
    campaign: Dict[str, Any],
    contact: Optional[Dict[str, Any]] = None,
    sending_list_id: Optional[int] = None,
    to_email_override: Optional[str] = None,
    record_sent: bool = True,
) -> bool:
    campaign_obj = safe_dict(campaign)
    campaign_id = _as_pos_int(campaign_obj.get("id"))
    mailbox_id = _as_pos_int(campaign_obj.get("mailbox_id"))
    sending_list_id_int = _as_pos_int(sending_list_id)

    ready_html = campaign_obj.get("ready_content")
    subjects = campaign_obj.get("subjects")
    letter_headers = campaign_obj.get("headers")
    html_tpl = str(ready_html or "").strip()
    contact_obj = safe_dict(contact)

    if not bool(record_sent):
        if campaign_id is None:
            raise RuntimeError("CAMPAIGN_ID_REQUIRED")
        if mailbox_id is None:
            raise RuntimeError("MAILBOX_ID_REQUIRED")
        if not html_tpl:
            raise RuntimeError("READY_CONTENT_EMPTY")
        if not isinstance(subjects, list):
            raise RuntimeError("SUBJECTS_BAD")
        subject_pool = [str(item).strip() for item in subjects if str(item or "").strip()]
        if not subject_pool:
            raise RuntimeError("SUBJECTS_EMPTY")
        to_email = str(to_email_override or "").strip()
        if not to_email:
            raise RuntimeError("TEST_EMAIL_REQUIRED")
        subj = random.choice(subject_pool[:3] if len(subject_pool) >= 3 else subject_pool)
        log_id = 0
        utm = "smrel=0"
    else:
        bad_campaign_reason = ""
        if campaign_id is None:
            bad_campaign_reason = "CAMPAIGN_ID_MISSING_OR_INVALID"
        elif sending_list_id_int is None:
            bad_campaign_reason = "SENDING_LIST_ID_MISSING_OR_INVALID"
        elif mailbox_id is None:
            bad_campaign_reason = "MAILBOX_ID_MISSING_OR_INVALID"
        elif not html_tpl:
            bad_campaign_reason = "READY_CONTENT_EMPTY"
        elif not isinstance(subjects, list):
            bad_campaign_reason = "SUBJECTS_BAD"
        else:
            subject_pool = [str(item).strip() for item in subjects if str(item or "").strip()]
            if not subject_pool:
                bad_campaign_reason = "SUBJECTS_EMPTY"

        if bad_campaign_reason:
            log_id = _next_sending_log_id()
            _insert_sending_log_row(
                log_id=int(log_id),
                campaign_id=int(campaign_id or _BAD_ID_SENTINEL),
                sending_list_id=int(sending_list_id_int or _BAD_ID_SENTINEL),
                status="BAD_CAMPAIGN_DATA",
                payload={
                    "reason": bad_campaign_reason,
                    "smtp_trace": [],
                    "smtp_code": None,
                },
                processed=False,
            )
            return False

        subj = random.choice(subject_pool[:3] if len(subject_pool) >= 3 else subject_pool)
        to_email = str(to_email_override or "").strip() or str(contact_obj.get("email") or "").strip()
        if not to_email:
            aggr_contact_id = _as_pos_int(contact_obj.get("aggr_contact_id"))
            log_id = _next_sending_log_id()
            _insert_sending_log_row(
                log_id=int(log_id),
                campaign_id=int(campaign_id),
                sending_list_id=int(sending_list_id_int),
                status="BAD_CONTACT_IN_DATABASE",
                payload={
                    "reason": "EMAIL_MISSING_OR_EMPTY",
                    "aggr_contact_id": int(aggr_contact_id or _BAD_ID_SENTINEL),
                    "smtp_trace": [],
                    "smtp_code": None,
                },
                processed=True,
            )
            return False

        log_id = _next_sending_log_id()
        utm = f"smrel={int(log_id)}"

    if contact_obj:
        vars_map = build_send_vars_from_contact(contact=contact_obj, utm=utm)
    else:
        vars_map = dict(DEFAULT_VARS)
        vars_map["UTM"] = utm
    vars_map["company_email"] = to_email

    body_html, body_text = build_send_bodies(html_tpl, vars_map, utm)

    headers: Dict[str, str] = {}
    for k, v in (safe_dict(letter_headers)).items():
        kk = str(k or "").strip()
        vv = str(v or "").strip()
        if kk and vv:
            headers[kk] = vv
    headers["X-Mailer-Id"] = str(log_id if bool(record_sent) else 0)

    aggr_contact_id_raw = contact_obj.get("aggr_contact_id")
    aggr_contact_id = _as_pos_int(aggr_contact_id_raw)
    is_blocked = bool(contact_obj.get("blocked"))
    is_wrong_email = bool(contact_obj.get("wrong_email"))

    payload_base: Dict[str, Any] = {
        "smtp_trace": [],
        "smtp_code": None,
    }

    if record_sent and (is_blocked or is_wrong_email):
        _insert_sending_log_row(
            log_id=int(log_id),
            campaign_id=int(campaign_id),
            sending_list_id=int(sending_list_id_int),
            status="BAD_CONTACT_IN_DATABASE",
            payload={
                "reason": "CONTACT_BLOCKED_OR_WRONG_EMAIL",
                "aggr_contact_id": int(aggr_contact_id or _BAD_ID_SENTINEL),
                **payload_base,
            },
        )
        return False

    smtp = SMTPConn(int(mailbox_id))
    ok = smtp.send_mail(
        to_email,
        subj,
        body_text=body_text,
        body_html=body_html,
        headers=headers,
    )

    trace = list(smtp.trace or [])
    code = smtp.last_send_code(to_email)

    payload = dict(payload_base)
    payload["smtp_trace"] = trace
    payload["smtp_code"] = code

    if ok:
        _smtp_451_clear_count(aggr_contact_id, to_email)
        if record_sent:
            _insert_sending_log_row(
                log_id=int(log_id),
                campaign_id=int(campaign_id),
                sending_list_id=int(sending_list_id_int),
                status="SEND",
                payload=payload,
            )
        return True

    if code is not None and 400 <= int(code) <= 499:
        log_mail_event(
            mailbox_id=int(mailbox_id),
            action="SMTP_SEND_CHECK",
            status="FAIL_TMP",
            payload_json={"code": int(code), "smtp_trace": trace},
        )
        if int(code) == 451 and bool(record_sent):
            fail_count = _smtp_451_inc_count(aggr_contact_id, to_email)
            if fail_count >= _SMTP_451_MAX_FAILS_PER_EMAIL:
                _mark_aggr_email_wrong(aggr_contact_id, _SMTP_451_EMAIL_WRONG_REASON)
                _smtp_451_clear_count(aggr_contact_id, to_email)
                _insert_sending_log_row(
                    log_id=int(log_id),
                    campaign_id=int(campaign_id),
                    sending_list_id=int(sending_list_id_int),
                    status="BAD_ADDRESS",
                    payload={
                        "reason": "SMTP_451_LIMIT_REACHED",
                        "aggr_contact_id": int(aggr_contact_id or _BAD_ID_SENTINEL),
                        **payload,
                    },
                )
        return False

    status = _status_from_smtp_code(code)
    if status == "BAD_ADDRESS" and bool(record_sent):
        _mark_aggr_email_wrong(aggr_contact_id, f"smtp_{int(code)}" if code is not None else "smtp_bad_address")
    if record_sent:
        _insert_sending_log_row(
            log_id=int(log_id),
            campaign_id=int(campaign_id),
            sending_list_id=int(sending_list_id_int),
            status=status,
            payload=payload,
        )
    return False
