# FILE: engine/common/mail/logs.py
# DATE: 2026-01-22
# PURPOSE: Temporary reversible obfuscation for mail secrets (stdlib only).
# NOTE:
# - НЕ криптостойко
# - только чтобы не хранить пароль в открытом виде
# - без зависимостей
# - позже заменим целиком и перекодируем всё

from __future__ import annotations

import base64


# TEMP key (просто соль, не безопасность)
_KEY = b"serenity-mail-secret-key"


def _xor(data: bytes, key: bytes) -> bytes:
    klen = len(key)
    return bytes(b ^ key[i % klen] for i, b in enumerate(data))


def encrypt_secret(plain: str) -> str:
    s = (plain or "")
    if not s:
        return ""
    raw = s.encode("utf-8")
    x = _xor(raw, _KEY)
    return base64.urlsafe_b64encode(x).decode("ascii")


def decrypt_secret(secret_enc: str) -> str:
    s = (secret_enc or "")
    if not s:
        return ""
    try:
        raw = base64.urlsafe_b64decode(s.encode("ascii"))
        plain = _xor(raw, _KEY)
        return plain.decode("utf-8", errors="strict")
    except Exception as e:
        raise ValueError("secret_decrypt_failed") from e
