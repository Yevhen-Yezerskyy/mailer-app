# FILE: engine/common/mail/smtp.py
# DATE: 2026-01-22
# PURPOSE: SMTP helpers: build cfg from DB by mailbox_id (optional cache), check connectivity/auth, send email.
# CHANGE: (new) Introduce SMTP build/check/send with explicit opt-in caching by external cache_key.

from __future__ import annotations

import base64
import json
import smtplib
import socket
import time
from email.message import EmailMessage
from email.utils import formataddr, make_msgid
from typing import Any, Dict, Optional, Tuple

from engine.common import db
from engine.common.cache.client import CLIENT as CACHE
from engine.common.mail.logs import decrypt_secret

from .types import MailResult, SmtpCfg


# Opt-in caching TTL (only used when caller provides cache_key)
SMTP_CFG_CACHE_TTL_SEC = 60 * 10


def smtp_build_cfg(mailbox_id: int, *, cache_key: Optional[str] = None) -> Tuple[Optional[SmtpCfg], MailResult]:
    """Build normalized SMTP config for mailbox.

    Caching rules:
      - cache is used ONLY if cache_key is provided by caller
      - on cache hit, DB is not touched
    """
    t0 = time.perf_counter()

    mid = _to_int(mailbox_id)
    if mid is None:
        return None, MailResult(ok=False, action="smtp_build_cfg", stage="input", code="bad_mailbox_id")

    if cache_key:
        try:
            payload = CACHE.get(str(cache_key), ttl_sec=SMTP_CFG_CACHE_TTL_SEC)
            if payload:
                cfg = _loads_cfg(payload)
                if cfg and isinstance(cfg, SmtpCfg):
                    res = MailResult(ok=True, action="smtp_build_cfg", stage="cache", code="cache_hit")
                    res.latency_ms = _ms(t0)
                    return cfg, res
        except Exception:
            # cache is best-effort; ignore errors
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
        res = MailResult(ok=False, action="smtp_build_cfg", stage="db", code="mailbox_not_found")
        res.latency_ms = _ms(t0)
        return None, res

    mb_id, mb_email, mb_domain, mb_active = mb_row
    if not bool(mb_active):
        res = MailResult(ok=False, action="smtp_build_cfg", stage="db", code="mailbox_inactive")
        res.latency_ms = _ms(t0)
        return None, res

    conn_row = db.fetch_one(
        """
        SELECT host, port, security, auth_type, username, secret_enc, extra_json
        FROM aap_settings_mailbox_connections
        WHERE mailbox_id = %s AND kind = 'smtp'
        ORDER BY id DESC
        LIMIT 1
        """,
        (mid,),
    )

    if not conn_row:
        res = MailResult(ok=False, action="smtp_build_cfg", stage="db", code="smtp_connection_not_found")
        res.latency_ms = _ms(t0)
        return None, res

    host, port, security, auth_type, username, secret_enc, extra_json = conn_row
    host_s = (str(host) if host is not None else "").strip()
    if not host_s:
        res = MailResult(ok=False, action="smtp_build_cfg", stage="cfg", code="smtp_host_empty")
        res.latency_ms = _ms(t0)
        return None, res

    port_i = _to_int(port)
    if port_i is None or port_i <= 0 or port_i > 65535:
        res = MailResult(ok=False, action="smtp_build_cfg", stage="cfg", code="smtp_port_bad")
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
        res = MailResult(ok=False, action="smtp_build_cfg", stage="cfg", code="smtp_secret_empty")
        res.latency_ms = _ms(t0)
        return None, res

    extra = extra_json if isinstance(extra_json, dict) else {}

    cfg = SmtpCfg(
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
            CACHE.set(str(cache_key), _dumps_cfg(cfg), ttl_sec=SMTP_CFG_CACHE_TTL_SEC)
        except Exception:
            pass

    res = MailResult(ok=True, action="smtp_build_cfg", stage="db", code="ok")
    res.latency_ms = _ms(t0)
    return cfg, res


def smtp_check(mailbox_id: int, *, cache_key: Optional[str] = None) -> MailResult:
    """Check TCP/TLS/AUTH for mailbox SMTP."""
    t0 = time.perf_counter()

    cfg, r0 = smtp_build_cfg(mailbox_id, cache_key=cache_key)
    if not cfg:
        return r0

    try:
        _smtp_connect_and_auth(cfg)
        res = MailResult(ok=True, action="smtp_check", stage="done", code="ok")
        res.details = {"host": cfg.host, "port": cfg.port, "security": cfg.security, "auth_type": cfg.auth_type}
        res.latency_ms = _ms(t0)
        return res
    except Exception as e:
        res = _err("smtp_check", _stage_from_exc(e), e)
        res.details.update({"host": cfg.host, "port": cfg.port, "security": cfg.security, "auth_type": cfg.auth_type})
        res.latency_ms = _ms(t0)
        return res


def smtp_send_mail(
    mailbox_id: int,
    to_email: str,
    required_json: Dict[str, Any],
    extra_json: Optional[Dict[str, Any]] = None,
    *,
    cache_key: Optional[str] = None,
) -> MailResult:
    """Send email via mailbox SMTP.

    required_json minimal contract (content only):
      - subject: str (required)
      - text: str (optional if html provided)
      - html: str (optional if text provided)

    extra_json:
      - may override anything EXCEPT From address
      - allowed: from_name, reply_to, headers, message_id, etc.
    """
    t0 = time.perf_counter()

    cfg, r0 = smtp_build_cfg(mailbox_id, cache_key=cache_key)
    if not cfg:
        return r0

    to_s = (to_email or "").strip()
    if not to_s:
        return MailResult(ok=False, action="smtp_send_mail", stage="input", code="to_empty")

    req = required_json if isinstance(required_json, dict) else {}
    ext = extra_json if isinstance(extra_json, dict) else {}

    subj = str(req.get("subject") or "").strip()
    if not subj:
        return MailResult(ok=False, action="smtp_send_mail", stage="input", code="subject_empty")

    text = req.get("text")
    html = req.get("html")
    text_s = str(text) if text is not None else ""
    html_s = str(html) if html is not None else ""
    if not text_s.strip() and not html_s.strip():
        return MailResult(ok=False, action="smtp_send_mail", stage="input", code="body_empty")

    # FROM ADDRESS: immutable (always from cfg/email)
    from_email = (cfg.email or "").strip()
    if not from_email:
        return MailResult(ok=False, action="smtp_send_mail", stage="cfg", code="from_email_empty")

    # FROM NAME: default from cfg.extra, may be overridden ONLY via extra_json
    base_from_name = str(cfg.extra.get("from_name") or "").strip()
    override_from_name = str(ext.get("from_name") or "").strip()
    from_name = override_from_name if override_from_name else base_from_name

    reply_to = ext.get("reply_to") or cfg.extra.get("reply_to")
    reply_to = str(reply_to or "").strip()

    headers = ext.get("headers")
    headers = headers if isinstance(headers, dict) else {}

    # ignore any from_* inside required_json (by design)
    msg = _build_message(
        from_email=from_email,
        from_name=from_name,
        to_email=to_s,
        subject=subj,
        text=text_s,
        html=html_s,
        reply_to=reply_to,
        headers=headers,
        message_id=str(ext.get("message_id") or "").strip(),
    )

    try:
        server = _smtp_connect_and_auth(cfg)
        try:
            refused = server.send_message(msg)
        finally:
            try:
                server.quit()
            except Exception:
                pass

        res = MailResult(ok=True, action="smtp_send_mail", stage="done", code="ok")
        res.message_id = str(msg.get("Message-ID") or "")
        res.details = {
            "host": cfg.host,
            "port": cfg.port,
            "security": cfg.security,
            "auth_type": cfg.auth_type,
            "refused": refused or {},
        }
        res.latency_ms = _ms(t0)
        return res
    except Exception as e:
        res = _err("smtp_send_mail", _stage_from_exc(e), e)
        res.details.update({"host": cfg.host, "port": cfg.port, "security": cfg.security, "auth_type": cfg.auth_type})
        res.latency_ms = _ms(t0)
        return res

# ------------------------- internals -------------------------


def _smtp_connect_and_auth(cfg: SmtpCfg) -> smtplib.SMTP:
    """Return connected+authed SMTP client (caller must quit())."""
    timeout = float(cfg.timeout_sec or 10.0)

    if cfg.security == "ssl":
        server: smtplib.SMTP = smtplib.SMTP_SSL(cfg.host, cfg.port, timeout=timeout)
    else:
        server = smtplib.SMTP(cfg.host, cfg.port, timeout=timeout)

    try:
        server.ehlo()

        if cfg.security == "starttls":
            server.starttls()
            server.ehlo()

        if cfg.auth_type == "login":
            server.login(cfg.username, cfg.secret)
        elif cfg.auth_type == "oauth2":
            _smtp_auth_xoauth2(server, cfg.username, cfg.secret)

        return server
    except Exception:
        try:
            server.quit()
        except Exception:
            try:
                server.close()
            except Exception:
                pass
        raise


def _smtp_auth_xoauth2(server: smtplib.SMTP, username: str, access_token: str) -> None:
    # RFC 7628 / XOAUTH2 format
    user = (username or "").strip()
    tok = (access_token or "").strip()
    if not user or not tok:
        raise ValueError("oauth2_missing_user_or_token")

    raw = f"user={user}\x01auth=Bearer {tok}\x01\x01".encode("utf-8")
    b64 = base64.b64encode(raw).decode("ascii")
    code, resp = server.docmd("AUTH", "XOAUTH2 " + b64)
    if int(code) != 235:
        # try to surface server message without leaking token
        msg = "".join([c for c in str(resp) if c not in tok])
        raise RuntimeError(f"oauth2_auth_failed:{code}:{msg[:200]}")


def _build_message(
    *,
    from_email: str,
    from_name: str,
    to_email: str,
    subject: str,
    text: str,
    html: str,
    reply_to: str,
    headers: Dict[str, Any],
    message_id: str,
) -> EmailMessage:
    msg = EmailMessage()

    msg["To"] = str(to_email).strip()
    msg["From"] = formataddr((from_name, str(from_email).strip())) if from_name else str(from_email).strip()
    msg["Subject"] = subject

    if reply_to:
        msg["Reply-To"] = reply_to

    # message-id
    mid = message_id.strip() if isinstance(message_id, str) else ""
    if not mid:
        mid = make_msgid()
    msg["Message-ID"] = mid

    # user headers
    for k, v in (headers or {}).items():
        kk = str(k).strip()
        if not kk:
            continue
        if kk.lower() in ("to", "from", "subject", "reply-to", "message-id"):
            continue
        msg[kk] = str(v)

    # bodies
    if text and text.strip():
        msg.set_content(text)
        if html and html.strip():
            msg.add_alternative(html, subtype="html")
    else:
        # html-only
        msg.set_content(html, subtype="html")

    return msg


def _decrypt_secret(secret_enc: str) -> str:
    """
    Decrypt SMTP secret stored in DB.
    Temporary implementation uses local Fernet key (see mail/logs.py).
    """
    try:
        return decrypt_secret(secret_enc)
    except Exception:
        # fail hard → auth error upstream, without leaking secret
        return ""


def _dumps_cfg(cfg: SmtpCfg) -> bytes:
    # Keep it JSON for transparency/debug (and avoid pickle security concerns).
    payload = {
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
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _loads_cfg(payload: bytes) -> Optional[SmtpCfg]:
    try:
        raw = json.loads(payload.decode("utf-8", errors="replace"))
        if not isinstance(raw, dict):
            return None
        return SmtpCfg(
            mailbox_id=int(raw.get("mailbox_id")),
            email=str(raw.get("email") or ""),
            domain=str(raw.get("domain") or ""),
            host=str(raw.get("host") or ""),
            port=int(raw.get("port")),
            security=str(raw.get("security") or "none"),  # type: ignore[arg-type]
            auth_type=str(raw.get("auth_type") or "login"),  # type: ignore[arg-type]
            username=str(raw.get("username") or ""),
            secret=str(raw.get("secret") or ""),
            extra=raw.get("extra") if isinstance(raw.get("extra"), dict) else {},
            timeout_sec=float(_to_float(raw.get("timeout_sec"), 10.0)),
        )
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None


def _to_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _ms(t0: float) -> int:
    return int(round((time.perf_counter() - t0) * 1000.0))


def _stage_from_exc(e: Exception) -> str:
    if isinstance(e, (socket.timeout, TimeoutError)):
        return "timeout"
    if isinstance(e, (ConnectionRefusedError, ConnectionResetError, socket.gaierror)):
        return "connect"
    if isinstance(e, smtplib.SMTPAuthenticationError):
        return "auth"
    if isinstance(e, smtplib.SMTPConnectError):
        return "connect"
    if isinstance(e, smtplib.SMTPServerDisconnected):
        return "disconnect"
    return "smtp"


def _err(action: str, stage: str, e: Exception) -> MailResult:
    code = type(e).__name__
    msg = str(e) or code
    if len(msg) > 300:
        msg = msg[:300]
    return MailResult(ok=False, action=action, stage=stage, code=code, message=msg)
