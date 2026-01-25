# FILE: engine/common/mail/types.py
# PATH: engine/common/mail/types.py
# DATE: 2026-01-25
# PURPOSE: Single source of truth for SMTP/IMAP credentials_json formats + strict put/get validators.
# CHANGE:
# - Added EncryptStr marker (callables) to mark encrypted fields inside TypedDict annotations.
# - Added put()/get() that validate payload by TypedDict annotations and encrypt/decrypt marked fields.
# - Validation now raises RuntimeError immediately (no bool/returns).

from typing import Any, Dict, Literal, TypedDict, get_args, get_origin

from engine.common.crypto import decrypt_secret, encrypt_secret


# =========================
# Encryption marker (one source of truth)
# =========================

EncryptStr = {
    "encrypt_func": encrypt_secret,
    "decrypt_func": decrypt_secret,
}


# =========================
# Shared primitives
# =========================

ConnSecurity = Literal["none", "ssl", "starttls"]


# =========================
# SMTP credentials_json formats
# =========================

class SmtpCredsLogin(TypedDict):
    host: str
    port: int
    security: ConnSecurity
    username: str
    password: EncryptStr


class SmtpCredsGoogleOAuth2(TypedDict):
    host: str
    port: int
    security: ConnSecurity
    email: str
    access_token: str
    refresh_token_enc: EncryptStr
    expires_at: int  # unix epoch seconds


class SmtpCredsMicrosoftOAuth2(TypedDict):
    host: str
    port: int
    security: ConnSecurity
    email: str
    tenant: str
    access_token: str
    refresh_token_enc: EncryptStr
    expires_at: int  # unix epoch seconds


# =========================
# IMAP credentials_json formats
# Strict 1:1 aliases of SMTP formats (identical today)
# =========================

ImapCredsLogin = SmtpCredsLogin
ImapCredsGoogleOAuth2 = SmtpCredsGoogleOAuth2
ImapCredsMicrosoftOAuth2 = SmtpCredsMicrosoftOAuth2


# =========================
# auth_type -> format binding
# THE canonical list of supported connection types
# =========================

SMTP_CREDENTIALS_FORMAT = {
    "LOGIN": SmtpCredsLogin,
    "GOOGLE_OAUTH_2_0": SmtpCredsGoogleOAuth2,
    "MICROSOFT_OAUTH_2_0": SmtpCredsMicrosoftOAuth2,
}

IMAP_CREDENTIALS_FORMAT = {
    "LOGIN": ImapCredsLogin,
    "GOOGLE_OAUTH_2_0": ImapCredsGoogleOAuth2,
    "MICROSOFT_OAUTH_2_0": ImapCredsMicrosoftOAuth2,
}


# =========================
# Strict put/get (encrypt/decrypt + validation)
# =========================

def put(payload: Dict[str, Any], fmt: type) -> Dict[str, Any]:
    """
    Validate dict by TypedDict format and ENCRYPT marked fields.
    Returns credentials_json ready for DB write.
    Raises RuntimeError on any mismatch.
    """
    return _apply(payload, fmt, mode="put")


def get(payload: Dict[str, Any], fmt: type) -> Dict[str, Any]:
    """
    Validate dict by TypedDict format and DECRYPT marked fields.
    Returns runtime credentials dict.
    Raises RuntimeError on any mismatch.
    """
    return _apply(payload, fmt, mode="get")


def _apply(payload: Dict[str, Any], fmt: type, *, mode: str) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise RuntimeError("credentials_json: payload must be dict")

    ann = getattr(fmt, "__annotations__", None)
    if not isinstance(ann, dict) or not ann:
        raise RuntimeError("credentials_json: fmt must be a TypedDict class with annotations")

    expected_keys = set(ann.keys())
    got_keys = set(payload.keys())

    missing = sorted(expected_keys - got_keys)
    extra = sorted(got_keys - expected_keys)
    if missing:
        raise RuntimeError(f"credentials_json: missing keys: {missing}")
    if extra:
        raise RuntimeError(f"credentials_json: extra keys: {extra}")

    out: Dict[str, Any] = {}
    for k, a in ann.items():
        v = payload.get(k)

        if a is EncryptStr:
            if not isinstance(v, str):
                raise RuntimeError(f"credentials_json: field '{k}' must be str (encrypted)")
            if mode == "put":
                out[k] = EncryptStr["encrypt_func"](v)
            elif mode == "get":
                out[k] = EncryptStr["decrypt_func"](v)
            else:
                raise RuntimeError("credentials_json: internal error (bad mode)")
            continue

        _validate_value_or_raise(k, a, v)
        out[k] = v

    return out


def _validate_value_or_raise(field: str, anno: Any, value: Any) -> None:
    origin = get_origin(anno)

    if origin is Literal:
        allowed = set(get_args(anno))
        if value in allowed:
            return
        raise RuntimeError(f"credentials_json: field '{field}' must be one of {sorted(allowed)}")

    if anno is str:
        if isinstance(value, str):
            return
        raise RuntimeError(f"credentials_json: field '{field}' must be str")

    if anno is int:
        if isinstance(value, int) and not isinstance(value, bool):
            return
        raise RuntimeError(f"credentials_json: field '{field}' must be int")

    if anno is bool:
        if isinstance(value, bool):
            return
        raise RuntimeError(f"credentials_json: field '{field}' must be bool")

    if anno is dict or origin is dict:
        if isinstance(value, dict):
            return
        raise RuntimeError(f"credentials_json: field '{field}' must be dict")

    if anno is list or origin is list:
        if isinstance(value, list):
            return
        raise RuntimeError(f"credentials_json: field '{field}' must be list")

    try:
        if isinstance(value, anno):
            return
    except Exception:
        pass

    raise RuntimeError(f"credentials_json: field '{field}' has unsupported type annotation")