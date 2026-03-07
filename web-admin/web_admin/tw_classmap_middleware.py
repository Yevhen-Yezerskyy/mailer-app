# FILE: web-admin/web_admin/tw_classmap_middleware.py
# DATE: 2026-03-07
# PURPOSE: Post-render class map middleware for HTML responses in admin contour panel.

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


_FLAG_ATTR = "_tw_classmap_enabled"
_DEFAULT_MAP_FILENAME = "tw_classmap.txt"

_MARKER = "<!-- TW-CLASSMAP: enabled -->\n"
_DOCTYPE_RE = re.compile(r"(?is)^(?P<prefix>\s*<!doctype\s+html\s*>\s*)")
_CLASS_ATTR_RE = re.compile(r"""\bclass\s*=\s*(?P<q>["'])(?P<v>.*?)(?P=q)""", re.IGNORECASE | re.DOTALL)


@dataclass
class _MapCache:
    mtime_ns: int = -1
    mapping: Optional[Dict[str, str]] = None


def _load_map_file(map_path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    try:
        raw = map_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return mapping

    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or ":" not in s:
            continue
        key, val = s.split(":", 1)
        key = key.strip()
        val = val.strip()
        if key and val:
            mapping[key] = val
    return mapping


def _apply_mapping_to_class_value(class_value: str, mapping: Dict[str, str]) -> str:
    if not class_value or not mapping:
        return class_value
    tokens = class_value.split()
    out: list[str] = []
    for token in tokens:
        out.append(token)
        extra = mapping.get(token)
        if extra:
            out.extend(extra.split())
    return " ".join(out)


def _inject_marker(html: str) -> str:
    if _MARKER.strip() in html:
        return html
    m = _DOCTYPE_RE.search(html)
    if not m:
        return html
    return html[: m.end("prefix")] + _MARKER + html[m.end("prefix") :]


class TailwindClassMapMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        self._cache = _MapCache()
        self._map_path = Path(__file__).resolve().parent / _DEFAULT_MAP_FILENAME

    def __call__(self, request):
        response = self.get_response(request)

        if not getattr(request, _FLAG_ATTR, False):
            return response

        ctype = (response.get("Content-Type") or "").lower()
        if "text/html" not in ctype:
            return response

        try:
            body = response.content.decode(response.charset or "utf-8")
        except Exception:
            return response

        mapping = self._get_mapping()
        new_body = _inject_marker(body)

        if mapping:
            def _repl(m: re.Match) -> str:
                q = m.group("q")
                v = m.group("v")
                nv = _apply_mapping_to_class_value(v, mapping)
                return f'class={q}{nv}{q}'

            new_body = _CLASS_ATTR_RE.sub(_repl, new_body)

        if new_body == body:
            return response

        response.content = new_body.encode(response.charset or "utf-8")
        if response.has_header("Content-Length"):
            response["Content-Length"] = str(len(response.content))
        return response

    def _get_mapping(self) -> Dict[str, str]:
        try:
            st = os.stat(self._map_path)
            mtime_ns = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000)))
        except FileNotFoundError:
            self._cache = _MapCache(mtime_ns=-1, mapping={})
            return {}

        if self._cache.mapping is not None and self._cache.mtime_ns == mtime_ns:
            return self._cache.mapping

        mapping = _load_map_file(self._map_path)
        self._cache = _MapCache(mtime_ns=mtime_ns, mapping=mapping)
        return mapping
