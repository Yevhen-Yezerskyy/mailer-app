# FILE: engine/common/mail/imap.py
# PATH: engine/common/mail/imap.py
# DATE: 2026-01-25 (новое)
# SUMMARY:
# - IMAPConn(mailbox_id): stateful object with .conn(), .close(), and IMAP ops returning result-or-None
# - Reads auth_type + credentials_json from aap_settings_imap_mailboxes, validates against types.IMAP_CREDENTIALS_FORMAT
# - Only LOGIN реально работает; OAUTH -> NOT_SUPPORTED (with log)
# - No DB logging. Every operation updates self.log (dict) + appends to self.trace (list)

from __future__ import annotations

import imaplib
from typing import Any, Dict, Optional, Tuple, cast

from engine.common import db
from engine.common.mail.types import IMAP_CREDENTIALS_FORMAT


class IMAPConn:
    def __init__(self, mailbox_id: int) -> None:
        self.mailbox_id = int(mailbox_id)
        self.auth_type: Optional[str] = None
        self.creds: Optional[Dict[str, Any]] = None
        self.conn_obj: Optional[imaplib.IMAP4] = None

        self.log: Dict[str, Any] = {}
        self.trace: list[Dict[str, Any]] = []

    # -------------------------
    # Lifecycle
    # -------------------------

    def conn(self) -> bool:
        self._set_log("IMAP_CONNECT", "START", {"mailbox_id": self.mailbox_id})

        auth_type, creds, err = self._load_from_db()
        if err:
            self._set_log("IMAP_CONNECT", "CHECK_FAILED", err)
            return False

        ok, v_err = self._validate_contract(auth_type, creds)
        if not ok:
            self._set_log("IMAP_CONNECT", "CHECK_FAILED", v_err)
            return False

        self.auth_type = auth_type
        self.creds = creds

        if auth_type == "LOGIN":
            return self._conn_LOGIN()
        if auth_type == "GOOGLE_OAUTH_2_0":
            return self._conn_GOOGLE_OAUTH2()
        if auth_type == "MICROSOFT_OAUTH_2_0":
            return self._conn_MICROSOFT_OAUTH2()

        self._set_log("IMAP_CONNECT", "CHECK_FAILED", {"error": "unknown_auth_type", "auth_type": auth_type})
        return False

    def close(self) -> bool:
        if not self.conn_obj:
            self._set_log("IMAP_DISCONNECT", "OK", {"note": "already_closed"})
            return True
        try:
            self.conn_obj.logout()
            self._set_log("IMAP_DISCONNECT", "OK", {})
            return True
        except Exception as e:
            self._set_log("IMAP_DISCONNECT", "FAIL", {"error": "logout_failed", "detail": str(e)})
            return False
        finally:
            self.conn_obj = None

    # -------------------------
    # IMAP ops (result or None)
    # -------------------------

    def select(self, mailbox: str = "INBOX", readonly: bool = True) -> Optional[Dict[str, Any]]:
        if not self.conn_obj:
            self._set_log("IMAP_SELECT", "CHECK_FAILED", {"error": "not_connected"})
            return None
        try:
            typ, data = self.conn_obj.select(mailbox, readonly)
            if typ != "OK":
                self._set_log("IMAP_SELECT", "FAIL", {"mailbox": mailbox, "typ": typ, "data": _b2s_list(data)})
                return None
            out = {"mailbox": mailbox, "count": _parse_count(data)}
            self._set_log("IMAP_SELECT", "OK", out)
            return out
        except Exception as e:
            self._set_log("IMAP_SELECT", "FAIL", {"mailbox": mailbox, "detail": str(e)})
            return None

    def uid_search(self, criteria: str = "ALL") -> Optional[list[str]]:
        if not self.conn_obj:
            self._set_log("IMAP_UID_SEARCH", "CHECK_FAILED", {"error": "not_connected"})
            return None
        try:
            typ, data = self.conn_obj.uid("SEARCH", None, criteria)
            if typ != "OK":
                self._set_log("IMAP_UID_SEARCH", "FAIL", {"criteria": criteria, "typ": typ, "data": _b2s_list(data)})
                return None
            uids = _parse_uid_list(data)
            self._set_log("IMAP_UID_SEARCH", "OK", {"criteria": criteria, "count": len(uids)})
            return uids
        except Exception as e:
            self._set_log("IMAP_UID_SEARCH", "FAIL", {"criteria": criteria, "detail": str(e)})
            return None

    def uid_fetch_rfc822(self, uid: str) -> Optional[bytes]:
        if not self.conn_obj:
            self._set_log("IMAP_UID_FETCH", "CHECK_FAILED", {"error": "not_connected"})
            return None
        try:
            typ, data = self.conn_obj.uid("FETCH", uid, "(RFC822)")
            if typ != "OK":
                self._set_log("IMAP_UID_FETCH", "FAIL", {"uid": uid, "typ": typ, "data": _b2s_list(data)})
                return None
            raw = _extract_first_bytes(data)
            if raw is None:
                self._set_log("IMAP_UID_FETCH", "FAIL", {"uid": uid, "error": "no_bytes"})
                return None
            self._set_log("IMAP_UID_FETCH", "OK", {"uid": uid, "bytes": len(raw)})
            return raw
        except Exception as e:
            self._set_log("IMAP_UID_FETCH", "FAIL", {"uid": uid, "detail": str(e)})
            return None

    def uid_store_flags(self, uid: str, flags: str, mode: str = "+") -> Optional[Dict[str, Any]]:
        if not self.conn_obj:
            self._set_log("IMAP_UID_STORE", "CHECK_FAILED", {"error": "not_connected"})
            return None
        cmd = f"{mode}FLAGS" if mode in ("+", "-") else "FLAGS"
        try:
            typ, data = self.conn_obj.uid("STORE", uid, cmd, flags)
            if typ != "OK":
                self._set_log("IMAP_UID_STORE", "FAIL", {"uid": uid, "op": cmd, "flags": flags, "typ": typ, "data": _b2s_list(data)})
                return None
            out = {"uid": uid, "op": cmd, "flags": flags}
            self._set_log("IMAP_UID_STORE", "OK", out)
            return out
        except Exception as e:
            self._set_log("IMAP_UID_STORE", "FAIL", {"uid": uid, "op": cmd, "flags": flags, "detail": str(e)})
            return None

    # -------------------------
    # DB + validation
    # -------------------------

    def _load_from_db(self) -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        r = db.fetch_one(
            """
            SELECT auth_type, credentials_json
            FROM aap_settings_imap_mailboxes
            WHERE mailbox_id=%s
            LIMIT 1
            """,
            (int(self.mailbox_id),),
        )
        if not r:
            return None, None, {"error": "imap_mailbox_not_found"}

        auth_type = r[0]
        creds = r[1]

        if not isinstance(auth_type, str) or not auth_type:
            return None, None, {"error": "bad_auth_type"}
        if not isinstance(creds, dict):
            return None, None, {"error": "bad_credentials_json"}

        return auth_type, cast(Dict[str, Any], creds), None

    def _validate_contract(self, auth_type: str, creds: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        if auth_type not in IMAP_CREDENTIALS_FORMAT:
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

        return True, {}

    # -------------------------
    # Connect handlers
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
                c: imaplib.IMAP4 = imaplib.IMAP4_SSL(host=host, port=port, timeout=10)
            else:
                c = imaplib.IMAP4(host=host, port=port, timeout=10)

            if security_mode == "starttls":
                c.starttls()

            typ, _ = c.login(username, password)
            if typ != "OK":
                try:
                    c.logout()
                except Exception:
                    pass
                self.conn_obj = None
                self._set_log("IMAP_CONNECT", "FAIL", {**base, "error": "login_failed", "typ": typ})
                return False

            self.conn_obj = c
            self._set_log("IMAP_CONNECT", "OK", base)
            return True

        except Exception as e:
            try:
                c.logout()
            except Exception:
                pass
            self.conn_obj = None
            self._set_log("IMAP_CONNECT", "FAIL", {**base, "detail": str(e)})
            return False

    def _conn_GOOGLE_OAUTH2(self) -> bool:
        self._set_log("IMAP_CONNECT", "NOT_SUPPORTED", {"auth_type": "GOOGLE_OAUTH_2_0"})
        return False

    def _conn_MICROSOFT_OAUTH2(self) -> bool:
        self._set_log("IMAP_CONNECT", "NOT_SUPPORTED", {"auth_type": "MICROSOFT_OAUTH_2_0"})
        return False

    # -------------------------
    # Log helpers
    # -------------------------

    def _set_log(self, action: str, status: str, data: Dict[str, Any]) -> None:
        rec = {"action": action, "status": status, "data": (data or {})}
        self.log = rec
        self.trace.append(rec)


def _b2s_list(x: Any) -> Any:
    if isinstance(x, list):
        return [_b2s_list(v) for v in x]
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="replace")
    return x


def _parse_uid_list(data: Any) -> list[str]:
    if not data or not isinstance(data, list) or not data[0]:
        return []
    raw = data[0]
    s = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    s = s.strip()
    return [p for p in s.split() if p]


def _extract_first_bytes(data: Any) -> Optional[bytes]:
    # imaplib returns list like: [(b'1 (RFC822 {N}', b'...raw...'), b')']
    if not isinstance(data, list):
        return None
    for item in data:
        if isinstance(item, tuple) and len(item) >= 2 and isinstance(item[1], (bytes, bytearray)):
            return bytes(item[1])
    return None


def _parse_count(data: Any) -> Optional[int]:
    if not data or not isinstance(data, list) or not data[0]:
        return None
    raw = data[0]
    try:
        s = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
        s = s.strip()
        return int(s) if s.isdigit() else None
    except Exception:
        return None
