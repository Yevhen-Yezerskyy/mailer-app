# FILE: engine/common/crypto.py
# DATE: 2026-01-25
# PURPOSE: Encrypt/decrypt DB secrets (passwords/tokens) using SERENITY_PASS_KEY from env (loaded by config/load_keys.py). Format: v1:gcm:<base64url(nonce|ciphertext|tag)>.

from __future__ import annotations

import base64
import os
import re
import secrets
from typing import Optional, Union

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

PASS_ENV = "SERENITY_PASS_KEY"
PREFIX = "v1:gcm:"


def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def _b64d(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def _parse_key(env_name: str = PASS_ENV) -> bytes:
    v = (os.environ.get(env_name) or "").strip()
    if not v:
        raise RuntimeError(f"Missing {env_name} in environment.")

    # allow: "hex:<64hex>", "<64hex>", or base64/base64url
    if v.startswith("hex:"):
        raw = bytes.fromhex(v[4:].strip())
    elif re.fullmatch(r"[0-9a-fA-F]{64}", v):
        raw = bytes.fromhex(v)
    else:
        raw = _b64d(v)

    if len(raw) != 32:
        raise RuntimeError(f"{env_name} must be 32 bytes (got {len(raw)}).")
    return raw


def encrypt_secret(plaintext: Union[str, bytes, None], key: Optional[bytes] = None) -> str:
    """
    Encrypt secret for DB storage. Returns "" if plaintext is empty/None.
    """
    if plaintext is None or plaintext == "" or plaintext == b"":
        return ""
    pt = plaintext.encode("utf-8") if isinstance(plaintext, str) else plaintext
    k = key or _parse_key(PASS_ENV)
    nonce = secrets.token_bytes(12)  # AESGCM nonce
    ct = AESGCM(k).encrypt(nonce, pt, None)
    return PREFIX + _b64e(nonce + ct)


def decrypt_secret(ciphertext: Optional[str], key: Optional[bytes] = None) -> str:
    """
    Decrypt secret from DB storage. Returns "" if ciphertext is empty/None.
    """
    v = (ciphertext or "").strip()
    if not v:
        return ""
    if not v.startswith(PREFIX):
        raise RuntimeError("Secret has unknown format (missing v1:gcm: prefix).")

    blob = _b64d(v[len(PREFIX) :])
    if len(blob) < 12 + 16:
        raise RuntimeError("Secret blob is too short.")

    nonce, ct = blob[:12], blob[12:]
    k = key or _parse_key(PASS_ENV)
    pt = AESGCM(k).decrypt(nonce, ct, None)
    return pt.decode("utf-8")
