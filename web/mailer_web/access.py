# FILE: web/mailer_web/access.py  (обновлено — 2025-12-24)
# CHANGE:
# - add short obid codec (encode/decode) for int DB ids (UI/marketing, NOT security)
# - add centralized resolver: decode GET ?id=... -> pk, and ONLY if model has workspace_id:
#   verify record exists in current request.workspace_id, else redirect to clean URL (no GET)
# - if model is None OR model has no workspace_id: do NOT touch DB (existence is view's job)

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Type, TypeVar

from django.conf import settings
from django.http import HttpRequest, HttpResponseRedirect

T = TypeVar("T")

_BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_BASE = 62
_MOD32 = 1 << 32


def _b62_encode(n: int) -> str:
    if n <= 0:
        raise ValueError("n must be > 0")
    out: list[str] = []
    while n:
        n, r = divmod(n, _BASE)
        out.append(_BASE62[r])
    out.reverse()
    return "".join(out)


def _b62_decode(s: str) -> int:
    if not s:
        raise ValueError("empty")
    n = 0
    for ch in s:
        i = _BASE62.find(ch)
        if i < 0:
            raise ValueError("bad char")
        n = n * _BASE + i
    return n


@dataclass(frozen=True)
class ObIdCodec:
    """
    Short reversible ID codec for int PK (UI/marketing, NOT security).
    32-bit bijection (xor + mult mod 2^32) + base62.
    """
    mask: int
    mult: int  # must be odd (invertible mod 2^32)

    @property
    def _inv(self) -> int:
        return pow(self.mult & 0xFFFFFFFF, -1, _MOD32)

    def encode(self, pk: int) -> str:
        if not isinstance(pk, int) or pk <= 0:
            raise ValueError("pk must be positive int")
        if pk >= _MOD32:
            raise ValueError("pk too large for 32-bit codec")

        x = (pk ^ (self.mask & 0xFFFFFFFF)) & 0xFFFFFFFF
        y = (x * (self.mult & 0xFFFFFFFF)) % _MOD32
        return _b62_encode(y + 1)  # avoid empty/zero

    def decode(self, token: str) -> int:
        y1 = _b62_decode((token or "").strip())
        if y1 <= 0:
            raise ValueError("bad token")
        y = (y1 - 1) % _MOD32

        x = (y * self._inv) % _MOD32
        pk = (x ^ (self.mask & 0xFFFFFFFF)) & 0xFFFFFFFF
        if pk <= 0:
            raise ValueError("bad token")
        return pk


def _codec() -> ObIdCodec:
    mask = int(getattr(settings, "OBID_MASK", 0xA5A5A5A5))
    mult = int(getattr(settings, "OBID_MULT", 2654435761))  # нечётный
    return ObIdCodec(mask=mask, mult=mult)


def encode_id(pk: int) -> str:
    return _codec().encode(int(pk))


def decode_id(token: str) -> int:
    return int(_codec().decode(token))


def _redirect_clean(request: HttpRequest) -> HttpResponseRedirect:
    # your rule: redirect to same URL without any GET
    return HttpResponseRedirect(request.path)


def resolve_pk_or_redirect(
    request: HttpRequest,
    model: Optional[Type[T]] = None,
    *,
    param: str = "id",
) -> int | HttpResponseRedirect:
    """
    Central resolver you described:

    - reads encoded token from GET (?id=...)
    - decodes token -> pk:int
    - if model is None -> return pk (no DB checks)
    - else:
        - if model has attribute 'workspace_id' (field exists) -> enforce workspace:
            * requires request.workspace_id
            * requires record exists with pk and workspace_id
            * else redirect to clean URL
        - if model has NO 'workspace_id' -> return pk (no DB checks; view decides existence)

    Any decode / permission failure -> redirect to clean URL (no GET).
    """

    token = request.GET.get(param)
    if not token:
        return _redirect_clean(request)

    try:
        pk = decode_id(token)
    except Exception:
        return _redirect_clean(request)

    if model is None:
        return pk

    # enforce workspace ONLY if model is workspace-scoped
    if hasattr(model, "workspace_id"):
        ws_id = getattr(request, "workspace_id", None)
        if ws_id is None:
            return _redirect_clean(request)

        try:
            model.objects.only("pk").get(pk=pk, workspace_id=ws_id)
        except Exception:
            return _redirect_clean(request)

    return pk
