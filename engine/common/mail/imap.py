# FILE: engine/common/mail/imap.py
# DATE: 2026-01-23
# PURPOSE: IMAP helpers: build cfg, connect/auth, list folders, yield next email.
# CHANGE:
# - Added imap_check_and_list_folders(): one connection + one auth, then LIST, then logout.
# - Keep old imap_check / imap_list_folders for backward compatibility.

from __future__ import annotations

import imaplib
import socket
import time
from email import message_from_bytes
from email.header import decode_header, make_header
from typing import Any, Dict, Generator, List, Optional, Tuple

from engine.common import db
from engine.common.cache.client import CLIENT as CACHE
from engine.common.mail.logs import decrypt_secret

from .types import ImapCfg, MailResult


IMAP_CFG_CACHE_TTL_SEC = 60 * 10


def imap_build_cfg(mailbox_id: int, *, cache_key: Optional[str] = None) -> Tuple[Optional[ImapCfg], MailResult]:
    t0 = time.perf_counter()

    mid = _to_int(mailbox_id)
    if mid is None:
        return None, MailResult(ok=False, action="imap_build_cfg", stage="input", code="bad_mailbox_id")

    if cache_key:
        try:
            payload = CACHE.get(str(cache_key), ttl_sec=IMAP_CFG_CACHE_TTL_SEC)
            if payload:
                cfg = _loads_cfg(payload)
                if cfg and isinstance(cfg, ImapCfg):
                    res = MailResult(ok=True, action="imap_build_cfg", stage="cache", code="cache_hit")
                    res.latency_ms = _ms(t0)
                    return cfg, res
        except Exception:
            pass

    mb_row = db.fetch_one(
        """
        SELECT id, email, domain, is_active
        FROM aap_settings_mailboxes
        WHERE id = %s
        """,
        (mid,),
    )
    if not mb_row:
        res = MailResult(ok=False, action="imap_build_cfg", stage="db", code="mailbox_not_found")
        res.latency_ms = _ms(t0)
        return None, res

    mb_id, mb_email, mb_domain, mb_active = mb_row
    if not bool(mb_active):
        res = MailResult(ok=False, action="imap_build_cfg", stage="db", code="mailbox_inactive")
        res.latency_ms = _ms(t0)
        return None, res

    conn_row = db.fetch_one(
        """
        SELECT host, port, security, auth_type, username, secret_enc, extra_json
        FROM aap_settings_mailbox_connections
        WHERE mailbox_id = %s AND kind = 'imap'
        ORDER BY id DESC
        LIMIT 1
        """,
        (mid,),
    )
    if not conn_row:
        res = MailResult(ok=False, action="imap_build_cfg", stage="db", code="imap_connection_not_found")
        res.latency_ms = _ms(t0)
        return None, res

    host, port, security, auth_type, username, secret_enc, extra_json = conn_row

    host_s = (str(host) if host is not None else "").strip()
    if not host_s:
        res = MailResult(ok=False, action="imap_build_cfg", stage="cfg", code="imap_host_empty")
        res.latency_ms = _ms(t0)
        return None, res

    port_i = _to_int(port)
    if port_i is None or port_i <= 0 or port_i > 65535:
        res = MailResult(ok=False, action="imap_build_cfg", stage="cfg", code="imap_port_bad")
        res.details = {"port": port}
        res.latency_ms = _ms(t0)
        return None, res

    sec = str(security or "none").strip().lower()
    if sec not in ("none", "ssl", "starttls"):
        sec = "none"

    auth = str(auth_type or "login").strip().lower()
    if auth not in ("login", "oauth2"):
        auth = "login"

    user_s = (str(username) if username is not None else "").strip()
    if not user_s:
        user_s = (str(mb_email) if mb_email is not None else "").strip()

    secret = _decrypt_secret(str(secret_enc or "").strip())
    if not secret and auth in ("login", "oauth2"):
        res = MailResult(ok=False, action="imap_build_cfg", stage="cfg", code="imap_secret_empty")
        res.latency_ms = _ms(t0)
        return None, res

    extra = extra_json if isinstance(extra_json, dict) else {}

    cfg = ImapCfg(
        mailbox_id=int(mb_id),
        email=str(mb_email or "").strip(),
        domain=str(mb_domain or "").strip(),
        host=host_s,
        port=int(port_i),
        security=sec,  # type: ignore[arg-type]
        auth_type=auth,  # type: ignore[arg-type]
        username=user_s,
        secret=secret,
        extra=extra,
        timeout_sec=float(_to_float(extra.get("timeout_sec"), 10.0)),
    )

    if cache_key:
        try:
            CACHE.set(str(cache_key), _dumps_cfg(cfg), ttl_sec=IMAP_CFG_CACHE_TTL_SEC)
        except Exception:
            pass

    res = MailResult(ok=True, action="imap_build_cfg", stage="db", code="ok")
    res.latency_ms = _ms(t0)
    return cfg, res


def imap_check(mailbox_id: int, *, cache_key: Optional[str] = None) -> MailResult:
    t0 = time.perf_counter()

    cfg, r0 = imap_build_cfg(mailbox_id, cache_key=cache_key)
    if not cfg:
        return r0

    conn = None
    try:
        conn = _imap_connect_and_auth(cfg)
        try:
            conn.noop()
        except Exception:
            pass

        res = MailResult(ok=True, action="imap_check", stage="done", code="ok")
        res.details = {"host": cfg.host, "port": cfg.port, "security": cfg.security, "auth_type": cfg.auth_type}
        res.latency_ms = _ms(t0)
        return res
    except Exception as e:
        res = _err("imap_check", _stage_from_exc(e), e)
        res.details.update({"host": cfg.host, "port": cfg.port, "security": cfg.security, "auth_type": cfg.auth_type})
        res.latency_ms = _ms(t0)
        return res
    finally:
        if conn is not None:
            _imap_logout_quiet(conn)


def imap_list_folders(mailbox_id: int, *, cache_key: Optional[str] = None) -> Tuple[List[str], MailResult]:
    t0 = time.perf_counter()

    cfg, r0 = imap_build_cfg(mailbox_id, cache_key=cache_key)
    if not cfg:
        return [], r0

    conn = None
    try:
        conn = _imap_connect_and_auth(cfg)
        typ, data = conn.list()

        if str(typ).upper() != "OK":
            res = MailResult(ok=False, action="imap_list_folders", stage="protocol", code="list_failed")
            res.latency_ms = _ms(t0)
            return [], res

        folders = _parse_list_folders(data)
        res = MailResult(ok=True, action="imap_list_folders", stage="done", code="ok")
        res.details = {"count": len(folders)}
        res.latency_ms = _ms(t0)
        return folders, res
    except Exception as e:
        res = _err("imap_list_folders", _stage_from_exc(e), e)
        res.latency_ms = _ms(t0)
        return [], res
    finally:
        if conn is not None:
            _imap_logout_quiet(conn)


def imap_check_and_list_folders(
    mailbox_id: int, *, cache_key: Optional[str] = None
) -> Tuple[List[str], MailResult]:
    """ONE connection + ONE auth: check(noop) + list folders."""
    t0 = time.perf_counter()

    cfg, r0 = imap_build_cfg(mailbox_id, cache_key=cache_key)
    if not cfg:
        return [], r0

    conn = None
    try:
        conn = _imap_connect_and_auth(cfg)

        try:
            conn.noop()
        except Exception:
            pass

        typ, data = conn.list()
        if str(typ).upper() != "OK":
            res = MailResult(ok=False, action="imap_check_and_list_folders", stage="protocol", code="list_failed")
            res.details = {"host": cfg.host, "port": cfg.port, "security": cfg.security, "auth_type": cfg.auth_type}
            res.latency_ms = _ms(t0)
            return [], res

        folders = _parse_list_folders(data)

        res = MailResult(ok=True, action="imap_check_and_list_folders", stage="done", code="ok")
        res.details = {
            "host": cfg.host,
            "port": cfg.port,
            "security": cfg.security,
            "auth_type": cfg.auth_type,
            "count": len(folders),
        }
        res.latency_ms = _ms(t0)
        return folders, res
    except Exception as e:
        res = _err("imap_check_and_list_folders", _stage_from_exc(e), e)
        res.details.update({"host": cfg.host, "port": cfg.port, "security": cfg.security, "auth_type": cfg.auth_type})
        res.latency_ms = _ms(t0)
        return [], res
    finally:
        if conn is not None:
            _imap_logout_quiet(conn)


def imap_yield_next(
    mailbox_id: int,
    *,
    folder: str = "INBOX",
    only_unseen: bool = True,
    include_raw: bool = False,
    cache_key: Optional[str] = None,
) -> Generator[Dict[str, Any], None, MailResult]:
    t0 = time.perf_counter()

    cfg, r0 = imap_build_cfg(mailbox_id, cache_key=cache_key)
    if not cfg:
        return r0  # type: ignore[return-value]

    folder_s = (folder or "INBOX").strip() or "INBOX"

    conn = None
    try:
        conn = _imap_connect_and_auth(cfg)

        typ, _ = conn.select(_imap_quote(folder_s), readonly=True)
        if str(typ).upper() != "OK":
            res = MailResult(ok=False, action="imap_yield_next", stage="protocol", code="select_failed")
            res.details = {"folder": folder_s}
            res.latency_ms = _ms(t0)
            return res  # type: ignore[return-value]

        crit = "UNSEEN" if only_unseen else "ALL"
        typ, data = conn.uid("SEARCH", None, crit)
        if str(typ).upper() != "OK":
            res = MailResult(ok=False, action="imap_yield_next", stage="protocol", code="search_failed")
            res.details = {"folder": folder_s, "criteria": crit}
            res.latency_ms = _ms(t0)
            return res  # type: ignore[return-value]

        uids = (data[0] or b"").split() if data else []
        if not uids:
            res = MailResult(ok=True, action="imap_yield_next", stage="done", code="no_messages")
            res.details = {"folder": folder_s, "criteria": crit}
            res.latency_ms = _ms(t0)
            return res  # type: ignore[return-value]

        uid = uids[0].decode("ascii", errors="ignore")

        typ, fdata = conn.uid("FETCH", uid, "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE MESSAGE-ID)])")
        if str(typ).upper() != "OK" or not fdata:
            res = MailResult(ok=False, action="imap_yield_next", stage="protocol", code="fetch_headers_failed")
            res.details = {"uid": uid, "folder": folder_s}
            res.latency_ms = _ms(t0)
            return res  # type: ignore[return-value]

        header_bytes = _extract_fetch_bytes(fdata)
        msg = message_from_bytes(header_bytes or b"", _class=None)

        out: Dict[str, Any] = {
            "uid": uid,
            "subject": _decode_mime_header(msg.get("Subject", "")),
            "from": _decode_mime_header(msg.get("From", "")),
            "date": (msg.get("Date", "") or "").strip(),
            "message_id": (msg.get("Message-ID", "") or "").strip(),
        }

        if include_raw:
            typ, rdata = conn.uid("FETCH", uid, "(RFC822)")
            if str(typ).upper() == "OK" and rdata:
                out["raw"] = _extract_fetch_bytes(rdata)

        yield out

        res = MailResult(ok=True, action="imap_yield_next", stage="done", code="ok")
        res.details = {"folder": folder_s, "criteria": crit, "uid": uid}
        res.latency_ms = _ms(t0)
        return res  # type: ignore[return-value]
    except Exception as e:
        res = _err("imap_yield_next", _stage_from_exc(e), e)
        res.details.update({"folder": folder_s})
        res.latency_ms = _ms(t0)
        return res  # type: ignore[return-value]
    finally:
        if conn is not None:
            _imap_logout_quiet(conn)


# =========================
# Internals
# =========================


def _imap_connect_and_auth(cfg: ImapCfg) -> imaplib.IMAP4:
    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(float(cfg.timeout_sec or 10.0))
    try:
        if cfg.security == "ssl":
            conn: imaplib.IMAP4 = imaplib.IMAP4_SSL(cfg.host, cfg.port)
        else:
            conn = imaplib.IMAP4(cfg.host, cfg.port)
            if cfg.security == "starttls":
                conn.starttls()

        if cfg.auth_type == "oauth2":
            xoauth2 = (cfg.secret or "").encode("utf-8", errors="ignore")

            def _auth_cb(_: bytes) -> bytes:
                return xoauth2

            conn.authenticate("XOAUTH2", _auth_cb)
        else:
            conn.login(cfg.username, cfg.secret)

        return conn
    finally:
        socket.setdefaulttimeout(old)


def _imap_logout_quiet(conn: imaplib.IMAP4) -> None:
    try:
        conn.logout()
    except Exception:
        pass


def _parse_list_folders(lines: Any) -> List[str]:
    out: List[str] = []
    if not isinstance(lines, list):
        return out
    for it in lines:
        if not it:
            continue
        s = it.decode("utf-8", errors="ignore") if isinstance(it, (bytes, bytearray)) else str(it)
        s = s.strip()
        if not s:
            continue
        parts = s.split('"')
        if len(parts) >= 2:
            name = parts[-2].strip()
            if name:
                out.append(name)
                continue
        out.append(s.split()[-1].strip())
    seen = set()
    uniq: List[str] = []
    for f in out:
        if f not in seen:
            seen.add(f)
            uniq.append(f)
    return uniq


def _extract_fetch_bytes(fetch_data: Any) -> bytes:
    if not isinstance(fetch_data, list):
        return b""
    for it in fetch_data:
        if isinstance(it, tuple) and len(it) >= 2 and isinstance(it[1], (bytes, bytearray)):
            return bytes(it[1])
    return b""


def _decode_mime_header(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return ""
    try:
        dh = decode_header(s)
        return str(make_header(dh)).strip()
    except Exception:
        return s


def _imap_quote(folder: str) -> str:
    f = (folder or "").strip()
    if not f:
        return "INBOX"
    if any(ch.isspace() for ch in f):
        return f'"{f}"'
    return f


def _decrypt_secret(secret_enc: str) -> str:
    if not secret_enc:
        return ""
    try:
        return decrypt_secret(secret_enc)
    except Exception:
        return ""


def _dumps_cfg(cfg: ImapCfg) -> str:
    return repr(
        {
            "mailbox_id": cfg.mailbox_id,
            "email": cfg.email,
            "domain": cfg.domain,
            "host": cfg.host,
            "port": cfg.port,
            "security": cfg.security,
            "auth_type": cfg.auth_type,
            "username": cfg.username,
            "secret": cfg.secret,
            "extra": cfg.extra,
            "timeout_sec": cfg.timeout_sec,
        }
    )


def _loads_cfg(payload: str) -> Optional[ImapCfg]:
    s = (payload or "").strip()
    if not s:
        return None
    try:
        x = eval(s, {"__builtins__": {}}, {})  # noqa: S307
        if not isinstance(x, dict):
            return None
        return ImapCfg(
            mailbox_id=int(x.get("mailbox_id") or 0),
            email=str(x.get("email") or ""),
            domain=str(x.get("domain") or ""),
            host=str(x.get("host") or ""),
            port=int(x.get("port") or 0),
            security=str(x.get("security") or "none"),  # type: ignore[arg-type]
            auth_type=str(x.get("auth_type") or "login"),  # type: ignore[arg-type]
            username=str(x.get("username") or ""),
            secret=str(x.get("secret") or ""),
            extra=x.get("extra") if isinstance(x.get("extra"), dict) else {},
            timeout_sec=float(x.get("timeout_sec") or 10.0),
        )
    except Exception:
        return None


def _stage_from_exc(e: Exception) -> str:
    if isinstance(e, socket.timeout):
        return "timeout"
    if isinstance(e, imaplib.IMAP4.abort):
        return "disconnect"
    if isinstance(e, imaplib.IMAP4.error):
        return "auth"
    if isinstance(e, OSError):
        return "connect"
    return "unknown"


def _err(action: str, stage: str, e: Exception) -> MailResult:
    res = MailResult(ok=False, action=action, stage=stage, code=e.__class__.__name__)
    res.message = str(e)[:500]
    return res


def _to_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(str(v).strip())
    except Exception:
        return None


def _to_float(v: Any, default: float) -> float:
    try:
        if v is None:
            return float(default)
        return float(str(v).strip())
    except Exception:
        return float(default)


def _ms(t0: float) -> int:
    return int((time.perf_counter() - t0) * 1000)
