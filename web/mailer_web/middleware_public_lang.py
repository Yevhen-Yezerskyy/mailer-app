# FILE: web/mailer_web/middleware_public_lang.py  (обновлено — 2026-03-07)
# PURPOSE:
# - Язык живет только в cookie (без языковых URL-префиксов и редиректов).
# - Приоритет выбора языка: POST language (setlang) -> django_language -> serenity_lang -> geo.
# - Если geo=UA и cookie нет/битые -> uk; иначе -> de.
# - При первом заходе сразу ставим обе cookie и дальше работаем только через них.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import geoip2.database
from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.utils import translation


@dataclass(frozen=True)
class _Cfg:
    cookie_name: str
    cookie_max_age: int
    public_langs: tuple[str, ...]
    default_lang: str
    geo_db_path: Path
    bypass_prefixes: tuple[str, ...]


_reader: Optional[geoip2.database.Reader] = None


def _cfg() -> _Cfg:
    return _Cfg(
        cookie_name=getattr(settings, "PUBLIC_LANG_COOKIE_NAME", "serenity_lang"),
        cookie_max_age=int(getattr(settings, "PUBLIC_LANG_COOKIE_MAX_AGE", 3600 * 24 * 365)),
        public_langs=tuple(getattr(settings, "PUBLIC_LANGS", ("ru", "de", "uk", "en"))),
        # по новой логике дефолт для всех, кроме UA
        default_lang="de",
        geo_db_path=Path(getattr(settings, "PUBLIC_GEOIP_DB_PATH", getattr(settings, "GEOIP_PATH", "")))
        / "GeoLite2-Country.mmdb",
        bypass_prefixes=tuple(
            getattr(
                settings,
                "PUBLIC_LANG_BYPASS_PREFIXES",
                ("/static/",),
            )
        ),
    )


def _django_lang_cookie_name() -> str:
    return getattr(settings, "LANGUAGE_COOKIE_NAME", "django_language")


def _is_valid_lang(cfg: _Cfg, lang: str | None) -> bool:
    return bool(lang) and (lang in cfg.public_langs)


def _get_client_ip(request: HttpRequest) -> str | None:
    xff = request.META.get("HTTP_X_FORWARDED_FOR")
    if xff:
        ip = xff.split(",")[0].strip()
        return ip or None
    return request.META.get("REMOTE_ADDR") or None


def _get_reader(db_path: Path) -> Optional[geoip2.database.Reader]:
    global _reader
    if _reader is not None:
        return _reader
    try:
        if not db_path.exists():
            return None
        _reader = geoip2.database.Reader(str(db_path))
        return _reader
    except Exception:
        return None


def _country_to_lang(country: str | None, default_lang: str) -> str:
    cc = (country or "").upper()
    if cc == "UA":
        return "uk"
    return default_lang


def _pick_geo_lang(cfg: _Cfg, request: HttpRequest) -> str:
    ip = _get_client_ip(request)
    reader = _get_reader(cfg.geo_db_path)
    country = None
    if reader and ip:
        try:
            country = reader.country(ip).country.iso_code
        except Exception:
            country = None
    lang = _country_to_lang(country, cfg.default_lang)
    return lang if _is_valid_lang(cfg, lang) else cfg.default_lang


def _activate(request: HttpRequest, lang: str) -> None:
    translation.activate(lang)
    request.LANGUAGE_CODE = lang

    meta = dict(getattr(settings, "UI_LANGUAGE_META", {}).get(lang, {}))
    request.ui_lang_code = lang
    request.ui_lang_name_en = str(meta.get("name_en") or lang)
    request.ui_lang_switch_label = str(meta.get("switch_label") or lang.upper())
    request.ui_lang_is_de = lang == "de"
    request.ui_lang = {
        "code": request.ui_lang_code,
        "name_en": request.ui_lang_name_en,
        "switch_label": request.ui_lang_switch_label,
        "is_de": request.ui_lang_is_de,
    }


def _set_lang_cookie(resp: HttpResponse, name: str, value: str, max_age: int) -> None:
    resp.set_cookie(name, value, max_age=max_age, samesite="Lax")


def _sync_cookies(
    resp: HttpResponse,
    cfg: _Cfg,
    *,
    lang: str,
    serenity_lang: str | None,
    django_lang: str | None,
) -> None:
    if not _is_valid_lang(cfg, lang):
        return

    if lang != (serenity_lang or ""):
        _set_lang_cookie(resp, cfg.cookie_name, lang, cfg.cookie_max_age)

    if lang != (django_lang or ""):
        _set_lang_cookie(resp, _django_lang_cookie_name(), lang, cfg.cookie_max_age)


def _pick_lang(
    cfg: _Cfg,
    request: HttpRequest,
    *,
    serenity_lang: str | None,
    django_lang: str | None,
) -> str:
    # set_language продолжает быть источником истины в момент POST
    post_lang = request.POST.get("language") if request.method == "POST" else None
    if _is_valid_lang(cfg, post_lang):
        return str(post_lang)
    if _is_valid_lang(cfg, django_lang):
        return str(django_lang)
    if _is_valid_lang(cfg, serenity_lang):
        return str(serenity_lang)
    return _pick_geo_lang(cfg, request)


class PublicLangMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        cfg = _cfg()
        path = request.path or "/"

        for pfx in cfg.bypass_prefixes:
            if path.startswith(pfx):
                return self.get_response(request)

        serenity_lang = request.COOKIES.get(cfg.cookie_name)
        django_lang = request.COOKIES.get(_django_lang_cookie_name())

        lang = _pick_lang(
            cfg,
            request,
            serenity_lang=serenity_lang,
            django_lang=django_lang,
        )
        if not _is_valid_lang(cfg, lang):
            lang = cfg.default_lang

        _activate(request, lang)
        try:
            resp = self.get_response(request)
            _sync_cookies(
                resp,
                cfg,
                lang=lang,
                serenity_lang=serenity_lang,
                django_lang=django_lang,
            )
            return resp
        finally:
            translation.deactivate()
