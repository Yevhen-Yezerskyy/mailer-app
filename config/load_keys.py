# FILE: config/load_keys.py
# DATE: 2026-01-25
# PURPOSE: Encrypt plaintext values in config/keys.py (decrypted->encrypted, decrypted cleared) and load all decrypted keys into os.environ; preserve comments/format via token-level rewrite.

from __future__ import annotations

import argparse
import base64
import io
import os
import re
import secrets
import shlex
import sys
import tokenize
from pathlib import Path
from typing import Dict, Tuple

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

MASTER_ENV = "SERENITY_KEYS_MASTER_KEY"
PREFIX = "v1:gcm:"


def _here() -> Path:
    return Path(__file__).resolve().parent


def _keys_file() -> Path:
    return _here() / "keys.py"


def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def _b64d(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def _parse_master_key(v: str) -> bytes:
    v = (v or "").strip()
    if not v:
        raise RuntimeError(f"Missing {MASTER_ENV} in environment.")
    if v.startswith("hex:"):
        v = v[4:].strip()
        raw = bytes.fromhex(v)
    elif re.fullmatch(r"[0-9a-fA-F]{64}", v):
        raw = bytes.fromhex(v)
    else:
        raw = _b64d(v)

    if len(raw) != 32:
        raise RuntimeError(f"{MASTER_ENV} must be 32 bytes (got {len(raw)}).")
    return raw


def _encrypt(key: bytes, plaintext: str) -> str:
    nonce = secrets.token_bytes(12)
    ct = AESGCM(key).encrypt(nonce, (plaintext or "").encode("utf-8"), None)
    return PREFIX + _b64e(nonce + ct)


def _decrypt(key: bytes, encrypted: str) -> str:
    v = (encrypted or "").strip()
    if not v:
        return ""
    if not v.startswith(PREFIX):
        raise RuntimeError("Encrypted value has unknown format (missing v1:gcm: prefix).")
    blob = _b64d(v[len(PREFIX) :])
    if len(blob) < 12 + 16:
        raise RuntimeError("Encrypted value is too short.")
    nonce, ct = blob[:12], blob[12:]
    pt = AESGCM(key).decrypt(nonce, ct, None)
    return pt.decode("utf-8")


def _exec_keys(src: str, filename: str) -> Dict[str, Dict[str, str]]:
    ns: Dict[str, object] = {}
    exec(compile(src, filename, "exec"), ns)
    keys = ns.get("KEYS")
    if not isinstance(keys, dict):
        raise RuntimeError("config/keys.py must define KEYS: dict")
    # shallow validate
    for k, v in keys.items():
        if not isinstance(k, str) or not isinstance(v, dict):
            raise RuntimeError("KEYS must be Dict[str, Dict[str, str]]")
        if "encrypted" not in v or "decrypted" not in v:
            raise RuntimeError(f"KEYS[{k!r}] must contain encrypted/decrypted")
    return keys  # type: ignore[return-value]


def _token_rewrite_keys_dict(src: str, updates: Dict[str, Tuple[str, str]]) -> str:
    """
    updates: key_name -> (new_encrypted, new_decrypted)
    Rewrites ONLY string literal values of encrypted/decrypted inside KEYS dict,
    preserving whitespace/comments/format by operating on tokens.
    """
    rdr = io.StringIO(src)
    tokens = list(tokenize.generate_tokens(rdr.readline))

    out = []
    i = 0

    # State: robust detect `KEYS: ... = {`
    saw_keys_name = False
    saw_equals = False

    in_keys = False
    brace_level = 0

    current_top_key: Optional[str] = None
    expecting_top_key = False

    in_item_dict = False
    item_level = 0

    while i < len(tokens):
        t = tokens[i]
        s = t.string

        # detect KEYS start even with annotation: KEYS : ... = {
        if not in_keys:
            if t.type == tokenize.NAME and s == "KEYS":
                saw_keys_name = True
                saw_equals = False
                out.append(t)
                i += 1
                continue

            if saw_keys_name:
                # wait until '=' then '{' (ignore annotations, types, spaces, comments, newlines)
                if s == "=":
                    saw_equals = True
                    out.append(t)
                    i += 1
                    continue
                if saw_equals and s == "{":
                    in_keys = True
                    brace_level = 1
                    expecting_top_key = True
                    current_top_key = None
                    in_item_dict = False
                    item_level = 0
                    saw_keys_name = False
                    saw_equals = False
                    out.append(t)
                    i += 1
                    continue

                # if we hit a NEWLINE without entering dict, reset (safety)
                if t.type == tokenize.NEWLINE:
                    saw_keys_name = False
                    saw_equals = False

                out.append(t)
                i += 1
                continue

            out.append(t)
            i += 1
            continue

        # inside KEYS dict: brace tracking
        if s == "{":
            brace_level += 1
            if current_top_key is not None and not in_item_dict and brace_level >= 2:
                in_item_dict = True
                item_level = 1
            elif in_item_dict:
                item_level += 1
            out.append(t)
            i += 1
            continue

        if s == "}":
            if in_item_dict:
                item_level -= 1
                if item_level <= 0:
                    in_item_dict = False
                    current_top_key = None
                    expecting_top_key = True
            brace_level -= 1
            out.append(t)
            i += 1
            if brace_level <= 0:
                in_keys = False
            continue

        # top-level key: "OPENAI_API_KEY": {...}
        if expecting_top_key and brace_level == 1 and t.type == tokenize.STRING:
            try:
                key_name = eval(s, {}, {})
            except Exception:
                key_name = None
            if isinstance(key_name, str):
                # lookahead for ':'
                j = i + 1
                while j < len(tokens) and tokens[j].type in (
                    tokenize.NL,
                    tokenize.NEWLINE,
                    tokenize.INDENT,
                    tokenize.DEDENT,
                    tokenize.COMMENT,
                ):
                    j += 1
                if j < len(tokens) and tokens[j].string == ":":
                    current_top_key = key_name
                    expecting_top_key = False
            out.append(t)
            i += 1
            continue

        # inside item dict: replace values
        if in_item_dict and current_top_key in updates and t.type == tokenize.STRING:
            try:
                field = eval(s, {}, {})
            except Exception:
                field = None

            if field in ("encrypted", "decrypted"):
                out.append(t)
                i += 1

                # copy until ':'
                while i < len(tokens):
                    out.append(tokens[i])
                    if tokens[i].string == ":":
                        i += 1
                        break
                    i += 1

                # replace next STRING token (the value)
                while i < len(tokens):
                    vt = tokens[i]
                    if vt.type == tokenize.STRING:
                        new_enc, new_dec = updates[current_top_key]
                        new_val = new_enc if field == "encrypted" else new_dec
                        repl = tokenize.TokenInfo(
                            type=tokenize.STRING,
                            string=repr(new_val),
                            start=vt.start,
                            end=vt.end,
                            line=vt.line,
                        )
                        out.append(repl)
                        i += 1
                        break
                    else:
                        out.append(vt)
                        i += 1
                continue

        out.append(t)
        i += 1

    return tokenize.untokenize(out)


# FILE: config/load_keys.py
# DATE: 2026-02-21
# PURPOSE: Unified key workflow with explicit modes:
# - seal: encrypt plaintext values from KEYS[*].decrypted into KEYS[*].encrypted and clear plaintext.
# - load: decrypt KEYS[*].encrypted into process env (or print shell exports).

def _resolve_master() -> bytes:
    master = _parse_master_key(os.environ.get(MASTER_ENV, ""))
    return master


def _seal_keys(master: bytes) -> None:
    path = _keys_file()
    src = path.read_text(encoding="utf-8")
    keys = _exec_keys(src, str(path))

    # Encrypt plaintext values (decrypted -> encrypted), then clear decrypted.
    updates: Dict[str, Tuple[str, str]] = {}
    for name, item in keys.items():
        dec = (item.get("decrypted") or "").strip()
        if dec:
            updates[name] = (_encrypt(master, dec), "")

    if updates:
        new_src = _token_rewrite_keys_dict(src, updates)
        if new_src != src:
            path.write_text(new_src, encoding="utf-8")


def _load_keys(master: bytes, print_keys: bool = False, print_export: bool = False) -> None:
    path = _keys_file()
    src = path.read_text(encoding="utf-8")
    keys = _exec_keys(src, str(path))

    # Decrypt and either export to shell or set into current process env.
    for name, item in keys.items():
        enc = (item.get("encrypted") or "").strip()
        if not enc:
            continue

        val = _decrypt(master, enc)

        if print_export:
            print(f"export {name}={shlex.quote(val)}")
        else:
            os.environ[name] = val
            if print_keys:
                print(f"{name}={val}")


def init_keys(
    print_keys: bool = False,
    print_export: bool = False,
    do_seal: bool = True,
    do_load: bool = True,
) -> None:
    master = _resolve_master()

    if do_seal:
        _seal_keys(master)

    if do_load:
        _load_keys(master, print_keys=print_keys, print_export=print_export)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--print", action="store_true", help="Print decrypted keys (process-only).")
    p.add_argument("--print-export", action="store_true", help="Print shell export lines (use with eval).")
    p.add_argument("--seal-only", action="store_true", help="Only seal plaintext KEYS[*].decrypted into encrypted.")
    p.add_argument("--load-only", action="store_true", help="Only decrypt encrypted keys and load/export.")
    args = p.parse_args()

    if args.seal_only and args.load_only:
        raise SystemExit("Use either --seal-only or --load-only, not both.")

    if args.seal_only:
        init_keys(do_seal=True, do_load=False)
    elif args.load_only:
        init_keys(print_keys=args.print, print_export=args.print_export, do_seal=False, do_load=True)
    else:
        init_keys(print_keys=args.print, print_export=args.print_export, do_seal=True, do_load=True)
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
