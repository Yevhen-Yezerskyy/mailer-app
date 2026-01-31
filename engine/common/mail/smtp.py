# FILE: engine/common/mail/smtp.py
# PATH: engine/common/mail/smtp.py
# DATE: 2026-01-26
# SUMMARY:
# - From формируется ТОЛЬКО из aap_settings_smtp_mailboxes (sender_name + from_email).
# - extra_headers_json из БД применяется (перетирает дефолты).
# - headers из аргументов перетирают всё.
# - credentials_json используется ТОЛЬКО для auth/connect (types.get не трогаем).

from __future__ import annotations

import smtplib
import ssl
import time
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Dict, Optional, Tuple, cast

from engine.common import db
from engine.common.cache.client import memo as cache_memo
from engine.common.mail import types
from engine.common.mail.types import SMTP_CREDENTIALS_FORMAT

SMTP_ACTIONS = ("CONNECT", "AUTH", "SEND", "DISCONNECT")
STATUS_OK = "OK"
STATUS_FAILED = "FAILED"

_DB_CACHE_TTL_SEC = 60
_DB_CACHE_VERSION = "mailbox_creds_v2"


def _smtp_load_from_db_uncached(q: Tuple[Optional[str], int]) -> Tuple[str, Dict[str, Any], str, str, Dict[str, Any]]:
    _cache_key, mailbox_id = q

    r = db.fetch_one(
        """
        SELECT auth_type, credentials_json, sender_name, from_email, extra_headers_json
        FROM aap_settings_smtp_mailboxes
        WHERE mailbox_id=%s
        LIMIT 1
        """,
        (int(mailbox_id),),
    )
    if not r:
        raise RuntimeError("smtp_mailbox_not_found")

    auth_type, creds, sender_name, from_email, extra_headers = r

    if not isinstance(auth_type, str) or not auth_type:
        raise RuntimeError("bad_auth_type")
    if not isinstance(creds, dict):
        raise RuntimeError("bad_credentials_json")
    if auth_type not in SMTP_CREDENTIALS_FORMAT:
        raise RuntimeError(f"unknown_auth_type: {auth_type}")
    if not isinstance(from_email, str) or not from_email:
        raise RuntimeError("bad_from_email")
    if sender_name is None:
        sender_name = ""
    if not isinstance(extra_headers, dict):
        extra_headers = {}

    return auth_type, cast(Dict[str, Any], creds), sender_name, from_email, cast(Dict[str, Any], extra_headers)


class SMTPConn:
    def __init__(self, mailbox_id: int, cache_key: Optional[str] = None) -> None:
        self.mailbox_id = int(mailbox_id)
        self.cache_key = (cache_key or "").strip() or None

        self.auth_type: Optional[str] = None
        self.creds: Optional[Dict[str, Any]] = None
        self.sender_name: str = ""
        self.from_email: str = ""
        self.extra_headers: Dict[str, Any] = {}

        self.conn_obj: Optional[smtplib.SMTP] = None
        self.log: Dict[str, Any] = {}
        self.trace: list[Dict[str, Any]] = []

    # -------------------------
    # Public API
    # -------------------------

    def conn(self) -> bool:
        try:
            auth_type, creds_raw, sender_name, from_email, extra_headers = self._load_from_db()
            fmt = SMTP_CREDENTIALS_FORMAT[auth_type]
            creds = types.get(creds_raw, fmt)
        except Exception as e:
            self._set_log("CONNECT", STATUS_FAILED, {"error": "load_or_validate_failed", "detail": str(e)})
            return False

        self.auth_type = auth_type
        self.creds = creds
        self.sender_name = sender_name
        self.from_email = from_email
        self.extra_headers = extra_headers

        if auth_type == "LOGIN":
            return self._conn_LOGIN()

        if auth_type in ("GOOGLE_OAUTH_2_0", "MICROSOFT_OAUTH_2_0"):
            self._set_log("AUTH", STATUS_FAILED, {"error": "not_supported", "auth_type": auth_type})
            return False

        self._set_log("CONNECT", STATUS_FAILED, {"error": "unknown_auth_type", "auth_type": auth_type})
        return False

    def close(self) -> bool:
        if not self.conn_obj:
            self._set_log("DISCONNECT", STATUS_OK, {"note": "already_closed"})
            return True

        c = self.conn_obj
        try:
            code, msg = c.quit()
            self._set_log("DISCONNECT", STATUS_OK, {"server_reply": {"quit": {"code": code, "msg": _b2s(msg)}}})
            return True
        except Exception as e:
            try:
                c.close()
            except Exception:
                pass
            self._set_log("DISCONNECT", STATUS_FAILED, {"error": "quit_failed", "detail": str(e)})
            return False
        finally:
            self.conn_obj = None

    def _send_mail(
        self,
        to_email: str,
        subject: str,
        *,
        body_text: str = "",
        body_html: str = "",
        headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        if not self.conn_obj:
            self._set_log("SEND", STATUS_FAILED, {"error": "not_connected", "to": to_email})
            return False

        msg = EmailMessage()

        # --- defaults ---
        from_value = (
            f'{self.sender_name} <{self.from_email}>'
            if self.sender_name
            else self.from_email
        )
        msg["From"] = from_value
        msg["To"] = to_email
        #msg["To"] = "yevhen.yezerskyy@gmail.com"
        msg["Subject"] = subject
        msg["X-Mailer-App"] = "Serenity Mailer"

        # --- DB extra headers override defaults ---
        for k, v in (self.extra_headers or {}).items():
            if k and v:
                msg[k] = v

        # --- call headers override everything ---
        if headers:
            for k, v in headers.items():
                if k and v:
                    msg[k] = v

        if not msg.get("From"):
            self._set_log("SEND", STATUS_FAILED, {"error": "from_missing"})
            return False

        if body_html:
            msg.set_content(body_text or "")
            msg.add_alternative(body_html, subtype="html")
        else:
            msg.set_content(body_text or "")

        try:
            refused = self.conn_obj.send_message(msg) or {}
            refused_s: Dict[str, Any] = {}
            for rcpt, rr in refused.items():
                try:
                    code = int(rr[0])
                    resp = _b2s(rr[1])
                except Exception:
                    code, resp = None, str(rr)
                refused_s[str(rcpt)] = {"code": code, "resp": resp}

            if refused_s:
                self._set_log("SEND", STATUS_FAILED, {"to": to_email, "refused": refused_s})
                return False

            self._set_log("SEND", STATUS_OK, {"to": to_email, "refused": {}})
            return True

        except Exception as e:
            self._set_log("SEND", STATUS_FAILED, {"to": to_email, "error": "send_failed", "detail": str(e)})
            return False

    def send_mail(
        self,
        to_email: str,
        subject: str,
        *,
        body_text: str = "",
        body_html: str = "",
        headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        if not self.conn():
            return False
        try:
            return self._send_mail(
                to_email,
                subject,
                body_text=body_text,
                body_html=body_html,
                headers=headers,
            )
        finally:
            self.close()

    # -------------------------
    # Internal: DB
    # -------------------------

    def _load_from_db(self) -> Tuple[str, Dict[str, Any], str, str, Dict[str, Any]]:
        q: Tuple[Optional[str], int] = (self.cache_key, int(self.mailbox_id))
        if not self.cache_key:
            return _smtp_load_from_db_uncached(q)
        return cache_memo(
            q,
            _smtp_load_from_db_uncached,
            ttl=_DB_CACHE_TTL_SEC,
            version=_DB_CACHE_VERSION,
        )

    # -------------------------
    # Internal: connect handlers
    # -------------------------

    def _conn_LOGIN(self) -> bool:
        assert self.creds is not None

        host = cast(str, self.creds["host"])
        port = cast(int, self.creds["port"])
        security_mode = cast(str, self.creds["security"])
        username = cast(str, self.creds["username"])
        password = cast(str, self.creds["password"])

        base = {"auth_type": "LOGIN", "host": host, "port": port, "security": security_mode}

        c: Optional[smtplib.SMTP] = None
        try:
            if security_mode == "ssl":
                c = smtplib.SMTP_SSL(host=host, port=port, timeout=10)
            else:
                c = smtplib.SMTP(host=host, port=port, timeout=10)

            diag: Dict[str, Any] = {**base, "server_reply": {}}

            ehlo_code, ehlo_msg = c.ehlo()
            diag["server_reply"]["ehlo"] = {"code": ehlo_code, "msg": _b2s(ehlo_msg)}

            if security_mode == "starttls":
                ctx = ssl.create_default_context()
                tls_code, tls_msg = c.starttls(context=ctx)
                diag["server_reply"]["starttls"] = {"code": tls_code, "msg": _b2s(tls_msg)}

                ehlo2_code, ehlo2_msg = c.ehlo()
                diag["server_reply"]["ehlo_after_tls"] = {"code": ehlo2_code, "msg": _b2s(ehlo2_msg)}

            self._set_log("CONNECT", STATUS_OK, diag)

            a_code, a_msg = c.login(username, password)
            self._set_log("AUTH", STATUS_OK, {**base, "server_reply": {"login": {"code": a_code, "msg": _b2s(a_msg)}}})

            self.conn_obj = c
            return True

        except Exception as e:
            if c is not None:
                try:
                    c.quit()
                except Exception:
                    try:
                        c.close()
                    except Exception:
                        pass
            self.conn_obj = None

            if (self.log or {}).get("action") == "CONNECT" and (self.log or {}).get("status") == STATUS_OK:
                self._set_log("AUTH", STATUS_FAILED, {**base, "detail": str(e)})
            else:
                self._set_log("CONNECT", STATUS_FAILED, {**base, "detail": str(e)})
            return False

    # -------------------------
    # Internal: log helpers
    # -------------------------

    def _set_log(self, action: str, status: str, data: Dict[str, Any]) -> None:
        d = dict(data or {})
        d.setdefault("mailbox_id", self.mailbox_id)
        d.setdefault("timestamp", int(time.time()))
        d.setdefault("timestamp_iso", datetime.now(timezone.utc).isoformat())
        d.setdefault("server_reply", None)

        rec = {"action": action, "status": status, "data": d}
        self.log = rec
        self.trace.append(rec)


def _b2s(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "replace")
    return str(x)
