# FILE: engine/common/mail/imap.py
# PATH: engine/common/mail/imap.py
# DATE: 2026-01-25
# SUMMARY:
# - IMAPConn(mailbox_id, cache_key=None): stateful object with .conn(), .close(), and IMAP ops returning result-or-None.
# - Actions list at top: CONNECT/AUTH/SELECT/FETCH/LOGOUT. Status: OK/FAILED only.
# - Reads auth_type + credentials_json from DB (optional cache if cache_key provided) and validates+decrypts via engine.common.mail.types.get(fmt).
# - LOGIN works; OAUTH types -> FAILED (not_supported) with log.
# - Log/trace always include timestamp + server replies/diagnostics in data.

from __future__ import annotations

import imaplib
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, cast

from engine.common import db
from engine.common.cache.client import memo as cache_memo
from engine.common.mail import types
from engine.common.mail.types import IMAP_CREDENTIALS_FORMAT

IMAP_ACTIONS = ("CONNECT", "AUTH", "SELECT", "FETCH", "LOGOUT")
STATUS_OK = "OK"
STATUS_FAILED = "FAILED"

# Small TTL: changes in UI should be visible quickly.
_DB_CACHE_TTL_SEC = 60
_DB_CACHE_VERSION = "mailbox_creds_v1"


def _imap_load_from_db_uncached(q: Tuple[Optional[str], int]) -> Tuple[str, Dict[str, Any]]:
    _cache_key, mailbox_id = q

    r = db.fetch_one(
        """
        SELECT auth_type, credentials_json
        FROM aap_settings_imap_mailboxes
        WHERE mailbox_id=%s
        LIMIT 1
        """,
        (int(mailbox_id),),
    )
    if not r:
        raise RuntimeError("imap_mailbox_not_found")

    auth_type = r[0]
    creds = r[1]

    if not isinstance(auth_type, str) or not auth_type:
        raise RuntimeError("bad_auth_type")
    if not isinstance(creds, dict):
        raise RuntimeError("bad_credentials_json")

    if auth_type not in IMAP_CREDENTIALS_FORMAT:
        raise RuntimeError(f"unknown_auth_type: {auth_type}")

    return auth_type, cast(Dict[str, Any], creds)


class IMAPConn:
    def __init__(self, mailbox_id: int, cache_key: Optional[str] = None) -> None:
        self.mailbox_id = int(mailbox_id)
        self.cache_key = (cache_key or "").strip() or None
        self.auth_type: Optional[str] = None
        self.creds: Optional[Dict[str, Any]] = None
        self.conn_obj: Optional[imaplib.IMAP4] = None

        self.log: Dict[str, Any] = {}
        self.trace: list[Dict[str, Any]] = []

    # -------------------------
    # Lifecycle
    # -------------------------

    def conn(self) -> bool:
        try:
            auth_type, creds_raw = self._load_from_db()
            fmt = IMAP_CREDENTIALS_FORMAT[auth_type]
            creds = types.get(creds_raw, fmt)
        except Exception as e:
            self._set_log("CONNECT", STATUS_FAILED, {"error": "load_or_validate_failed", "detail": str(e)})
            return False

        self.auth_type = auth_type
        self.creds = creds

        if auth_type == "LOGIN":
            return self._conn_LOGIN()

        if auth_type in ("GOOGLE_OAUTH_2_0", "MICROSOFT_OAUTH_2_0"):
            self._set_log("AUTH", STATUS_FAILED, {"error": "not_supported", "auth_type": auth_type})
            return False

        self._set_log("CONNECT", STATUS_FAILED, {"error": "unknown_auth_type", "auth_type": auth_type})
        return False

    def close(self) -> bool:
        if not self.conn_obj:
            self._set_log("LOGOUT", STATUS_OK, {"note": "already_closed"})
            return True

        c = self.conn_obj
        try:
            typ, data = c.logout()
            self._set_log("LOGOUT", STATUS_OK, {"server_reply": {"logout": {"typ": typ, "data": _b2s_list(data)}}})
            return True
        except Exception as e:
            self._set_log("LOGOUT", STATUS_FAILED, {"error": "logout_failed", "detail": str(e)})
            return False
        finally:
            self.conn_obj = None

    # -------------------------
    # IMAP ops (result or None)
    # -------------------------

    def select(self, mailbox: str = "INBOX", readonly: bool = True) -> Optional[Dict[str, Any]]:
        if not self.conn_obj:
            self._set_log("SELECT", STATUS_FAILED, {"error": "not_connected", "mailbox": mailbox})
            return None
        try:
            typ, data = self.conn_obj.select(mailbox, readonly)
            rep = {"typ": typ, "data": _b2s_list(data)}
            if typ != "OK":
                self._set_log("SELECT", STATUS_FAILED, {"mailbox": mailbox, "server_reply": rep})
                return None
            out = {"mailbox": mailbox, "count": _parse_count(data), "server_reply": rep}
            self._set_log("SELECT", STATUS_OK, out)
            return out
        except Exception as e:
            self._set_log("SELECT", STATUS_FAILED, {"mailbox": mailbox, "detail": str(e)})
            return None

    def uid_search(self, criteria: str = "ALL") -> Optional[list[str]]:
        if not self.conn_obj:
            self._set_log("FETCH", STATUS_FAILED, {"error": "not_connected", "op": "UID_SEARCH", "criteria": criteria})
            return None
        try:
            typ, data = self.conn_obj.uid("SEARCH", None, criteria)
            rep = {"typ": typ, "data": _b2s_list(data)}
            if typ != "OK":
                self._set_log("FETCH", STATUS_FAILED, {"op": "UID_SEARCH", "criteria": criteria, "server_reply": rep})
                return None
            uids = _parse_uid_list(data)
            self._set_log("FETCH", STATUS_OK, {"op": "UID_SEARCH", "criteria": criteria, "count": len(uids), "server_reply": rep})
            return uids
        except Exception as e:
            self._set_log("FETCH", STATUS_FAILED, {"op": "UID_SEARCH", "criteria": criteria, "detail": str(e)})
            return None

    def uid_fetch_rfc822(self, uid: str) -> Optional[bytes]:
        if not self.conn_obj:
            self._set_log("FETCH", STATUS_FAILED, {"error": "not_connected", "op": "UID_FETCH_RFC822", "uid": uid})
            return None
        try:
            typ, data = self.conn_obj.uid("FETCH", uid, "(RFC822)")
            rep = {"typ": typ, "data": _b2s_list(data)}
            if typ != "OK":
                self._set_log("FETCH", STATUS_FAILED, {"op": "UID_FETCH_RFC822", "uid": uid, "server_reply": rep})
                return None
            raw = _extract_first_bytes(data)
            if raw is None:
                self._set_log("FETCH", STATUS_FAILED, {"op": "UID_FETCH_RFC822", "uid": uid, "error": "no_bytes", "server_reply": rep})
                return None
            self._set_log("FETCH", STATUS_OK, {"op": "UID_FETCH_RFC822", "uid": uid, "bytes": len(raw), "server_reply": rep})
            return raw
        except Exception as e:
            self._set_log("FETCH", STATUS_FAILED, {"op": "UID_FETCH_RFC822", "uid": uid, "detail": str(e)})
            return None

    def uid_store_flags(self, uid: str, flags: str, mode: str = "+") -> Optional[Dict[str, Any]]:
        if not self.conn_obj:
            self._set_log("FETCH", STATUS_FAILED, {"error": "not_connected", "op": "UID_STORE_FLAGS", "uid": uid})
            return None
        cmd = f"{mode}FLAGS" if mode in ("+", "-") else "FLAGS"
        try:
            typ, data = self.conn_obj.uid("STORE", uid, cmd, flags)
            rep = {"typ": typ, "data": _b2s_list(data)}
            if typ != "OK":
                self._set_log("FETCH", STATUS_FAILED, {"op": "UID_STORE_FLAGS", "uid": uid, "cmd": cmd, "flags": flags, "server_reply": rep})
                return None
            out = {"op": "UID_STORE_FLAGS", "uid": uid, "cmd": cmd, "flags": flags, "server_reply": rep}
            self._set_log("FETCH", STATUS_OK, out)
            return out
        except Exception as e:
            self._set_log("FETCH", STATUS_FAILED, {"op": "UID_STORE_FLAGS", "uid": uid, "cmd": cmd, "flags": flags, "detail": str(e)})
            return None

    # -------------------------
    # Internal: DB
    # -------------------------

    def _load_from_db(self) -> Tuple[str, Dict[str, Any]]:
        q: Tuple[Optional[str], int] = (self.cache_key, int(self.mailbox_id))

        if not self.cache_key:
            return _imap_load_from_db_uncached(q)

        return cache_memo(
            q,
            _imap_load_from_db_uncached,
            ttl=_DB_CACHE_TTL_SEC,
            version=_DB_CACHE_VERSION,
        )

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

        c: Optional[imaplib.IMAP4] = None
        try:
            if security_mode == "ssl":
                c = imaplib.IMAP4_SSL(host=host, port=port, timeout=10)
            else:
                c = imaplib.IMAP4(host=host, port=port, timeout=10)

            diag: Dict[str, Any] = {**base, "server_reply": {}}

            welcome = getattr(c, "welcome", None)
            diag["server_reply"]["welcome"] = _b2s(welcome)

            if security_mode == "starttls":
                t_typ, t_data = c.starttls()
                diag["server_reply"]["starttls"] = {"typ": t_typ, "data": _b2s_list(t_data)}

            self._set_log("CONNECT", STATUS_OK, diag)

            typ, data = c.login(username, password)
            rep = {"typ": typ, "data": _b2s_list(data)}

            if typ != "OK":
                try:
                    c.logout()
                except Exception:
                    pass
                self.conn_obj = None
                self._set_log("AUTH", STATUS_FAILED, {**base, "server_reply": {"login": rep}})
                return False

            self.conn_obj = c
            self._set_log("AUTH", STATUS_OK, {**base, "server_reply": {"login": rep}})
            return True

        except Exception as e:
            if c is not None:
                try:
                    c.logout()
                except Exception:
                    pass
            self.conn_obj = None

            if (self.log or {}).get("action") == "CONNECT" and (self.log or {}).get("status") == STATUS_OK:
                self._set_log("AUTH", STATUS_FAILED, {**base, "detail": str(e)})
            else:
                self._set_log("CONNECT", STATUS_FAILED, {**base, "detail": str(e)})
            return False

    # -------------------------
    # Log helpers
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


def _b2s_list(x: Any) -> Any:
    if isinstance(x, list):
        return [_b2s_list(v) for v in x]
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="replace")
    return x


def _b2s(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", errors="replace")
    return str(x)


def _parse_uid_list(data: Any) -> list[str]:
    if not data or not isinstance(data, list) or not data[0]:
        return []
    raw = data[0]
    s = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    s = s.strip()
    return [p for p in s.split() if p]


def _extract_first_bytes(data: Any) -> Optional[bytes]:
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
