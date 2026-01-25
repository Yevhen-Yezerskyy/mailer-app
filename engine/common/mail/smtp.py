# FILE: engine/common/mail/smtp.py
# PATH: engine/common/mail/smtp.py
# DATE: 2026-01-25 (новое)
# SUMMARY:
# - SMTPConn(mailbox_id): stateful object with .conn(), .close(), .send_mail(), ._send_mail()
# - Reads auth_type + credentials_json from aap_settings_smtp_mailboxes, validates against types.SMTP_CREDENTIALS_FORMAT
# - Only LOGIN реально работает; OAUTH -> NOT_SUPPORTED (with log)
# - No DB logging. Every operation updates self.log (dict) + appends to self.trace (list)

from __future__ import annotations

import smtplib
import ssl
from email.message import EmailMessage
from typing import Any, Dict, Optional, Tuple, cast

from engine.common import db
from engine.common.mail.types import SMTP_CREDENTIALS_FORMAT


class SMTPConn:
    def __init__(self, mailbox_id: int) -> None:
        self.mailbox_id = int(mailbox_id)
        self.auth_type: Optional[str] = None
        self.creds: Optional[Dict[str, Any]] = None
        self.conn_obj: Optional[smtplib.SMTP] = None

        self.log: Dict[str, Any] = {}
        self.trace: list[Dict[str, Any]] = []

    # -------------------------
    # Public API
    # -------------------------

    def conn(self) -> bool:
        """
        Open + authenticate. Returns bool, details in self.log.
        """
        self._set_log("SMTP_CONNECT", "START", {"mailbox_id": self.mailbox_id})

        auth_type, creds, err = self._load_from_db()
        if err:
            self._set_log("SMTP_CONNECT", "CHECK_FAILED", err)
            return False

        ok, v_err = self._validate_contract(auth_type, creds)
        if not ok:
            self._set_log("SMTP_CONNECT", "CHECK_FAILED", v_err)
            return False

        self.auth_type = auth_type
        self.creds = creds

        if auth_type == "LOGIN":
            return self._conn_LOGIN()
        if auth_type == "GOOGLE_OAUTH_2_0":
            return self._conn_GOOGLE_OAUTH2()
        if auth_type == "MICROSOFT_OAUTH_2_0":
            return self._conn_MICROSOFT_OAUTH2()

        self._set_log("SMTP_CONNECT", "CHECK_FAILED", {"error": "unknown_auth_type", "auth_type": auth_type})
        return False

    def close(self) -> bool:
        """
        QUIT + close. Returns bool, details in self.log.
        """
        if not self.conn_obj:
            self._set_log("SMTP_DISCONNECT", "OK", {"note": "already_closed"})
            return True

        try:
            self.conn_obj.quit()
            self._set_log("SMTP_DISCONNECT", "OK", {})
            return True
        except Exception as e:
            try:
                self.conn_obj.close()
            except Exception:
                pass
            self._set_log("SMTP_DISCONNECT", "FAIL", {"error": "quit_failed", "detail": str(e)})
            return False
        finally:
            self.conn_obj = None

    def _send_mail(
        self,
        to_email: str,
        subject: str,
        *,
        from_email: str,
        body_text: str = "",
        body_html: str = "",
        headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Send using existing connection (does NOT close). Returns bool, details in self.log.
        """
        if not self.conn_obj:
            self._set_log("SMTP_SEND", "CHECK_FAILED", {"error": "not_connected"})
            return False

        msg = EmailMessage()
        msg["From"] = from_email
        msg["To"] = to_email
        msg["Subject"] = subject

        if headers:
            for k, v in headers.items():
                if k and v:
                    msg[k] = v

        if body_html:
            msg.set_content(body_text or "")
            msg.add_alternative(body_html, subtype="html")
        else:
            msg.set_content(body_text or "")

        try:
            refused = self.conn_obj.send_message(msg) or {}
            # refused: dict[rcpt -> (code, resp)]
            refused_s: Dict[str, Any] = {}
            for rcpt, rr in refused.items():
                try:
                    code = int(rr[0])
                    resp = rr[1].decode("utf-8", "replace") if isinstance(rr[1], (bytes, bytearray)) else str(rr[1])
                except Exception:
                    code, resp = None, str(rr)
                refused_s[str(rcpt)] = {"code": code, "resp": resp}

            status = "OK" if not refused_s else "FAIL"
            self._set_log("SMTP_SEND", status, {"to": to_email, "refused": refused_s})
            return status == "OK"
        except Exception as e:
            self._set_log("SMTP_SEND", "FAIL", {"to": to_email, "error": "send_failed", "detail": str(e)})
            return False

    def send_mail(
        self,
        to_email: str,
        subject: str,
        *,
        from_email: str,
        body_text: str = "",
        body_html: str = "",
        headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        One-shot: conn -> _send_mail -> close. Returns bool, full details in self.trace and last op in self.log.
        """
        if not self.conn():
            return False
        try:
            return self._send_mail(
                to_email,
                subject,
                from_email=from_email,
                body_text=body_text,
                body_html=body_html,
                headers=headers,
            )
        finally:
            self.close()

    # -------------------------
    # Internal: DB + validation
    # -------------------------

    def _load_from_db(self) -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        r = db.fetch_one(
            """
            SELECT auth_type, credentials_json
            FROM aap_settings_smtp_mailboxes
            WHERE mailbox_id=%s
            LIMIT 1
            """,
            (int(self.mailbox_id),),
        )
        if not r:
            return None, None, {"error": "smtp_mailbox_not_found"}

        auth_type = r[0]
        creds = r[1]

        if not isinstance(auth_type, str) or not auth_type:
            return None, None, {"error": "bad_auth_type"}
        if not isinstance(creds, dict):
            return None, None, {"error": "bad_credentials_json"}

        return auth_type, cast(Dict[str, Any], creds), None

    def _validate_contract(self, auth_type: str, creds: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        if auth_type not in SMTP_CREDENTIALS_FORMAT:
            return False, {"error": "unknown_auth_type", "auth_type": auth_type}

        if auth_type == "LOGIN":
            need = ("host", "port", "security", "username", "password")
            miss = [k for k in need if k not in creds]
            if miss:
                return False, {"error": "bad_format", "missing": miss}

            if not isinstance(creds.get("host"), str) or not creds["host"]:
                return False, {"error": "bad_format", "field": "host"}
            if not isinstance(creds.get("port"), int):
                return False, {"error": "bad_format", "field": "port"}
            if creds.get("security") not in ("none", "ssl", "starttls"):
                return False, {"error": "bad_format", "field": "security"}
            if not isinstance(creds.get("username"), str):
                return False, {"error": "bad_format", "field": "username"}
            if not isinstance(creds.get("password"), str):
                return False, {"error": "bad_format", "field": "password"}

            return True, {}

        # For now: only ensure it's a dict (contract existence checked above)
        return True, {}

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

        try:
            if security_mode == "ssl":
                c: smtplib.SMTP = smtplib.SMTP_SSL(host=host, port=port, timeout=10)
            else:
                c = smtplib.SMTP(host=host, port=port, timeout=10)

            c.ehlo()

            if security_mode == "starttls":
                ctx = ssl.create_default_context()
                c.starttls(context=ctx)
                c.ehlo()

            c.login(username, password)

            self.conn_obj = c
            self._set_log("SMTP_CONNECT", "OK", base)
            return True

        except Exception as e:
            try:
                c.quit()
            except Exception:
                try:
                    c.close()
                except Exception:
                    pass
            self.conn_obj = None
            self._set_log("SMTP_CONNECT", "FAIL", {**base, "detail": str(e)})
            return False

    def _conn_GOOGLE_OAUTH2(self) -> bool:
        self._set_log("SMTP_CONNECT", "NOT_SUPPORTED", {"auth_type": "GOOGLE_OAUTH_2_0"})
        return False

    def _conn_MICROSOFT_OAUTH2(self) -> bool:
        self._set_log("SMTP_CONNECT", "NOT_SUPPORTED", {"auth_type": "MICROSOFT_OAUTH_2_0"})
        return False

    # -------------------------
    # Internal: log helpers
    # -------------------------

    def _set_log(self, action: str, status: str, data: Dict[str, Any]) -> None:
        rec = {"action": action, "status": status, "data": (data or {})}
        self.log = rec
        self.trace.append(rec)
