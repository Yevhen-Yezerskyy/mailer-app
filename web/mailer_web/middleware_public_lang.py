# FILE: web/mailer_web/middleware_public_lang.py  (обновлено — 2025-12-18)
# PURPOSE: Public language routing by cookie+geo + activate Django i18n:
# - выбираем язык (cookie/url/geo) и активируем translation.activate(lang)
# - кладём request.LANGUAGE_CODE
# - на ранних redirect обязательно translation.deactivate() чтобы не текло между запросами

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import geoip2.database
from django.conf import settings
from django.http import HttpRequest, HttpResponse, HttpResponseRedirect
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
        public_langs=tuple(getattr(settings, "PUBLIC_LANGS", ("ru", "de", "uk"))),
        default_lang=str(getattr(settings, "PUBLIC_LANG_DEFAULT", "de")),
        geo_db_path=Path(getattr(settings, "PUBLIC_GEOIP_DB_PATH", getattr(settings, "GEOIP_PATH", "")))
        / "GeoLite2-Country.mmdb",
        bypass_prefixes=tuple(
            getattr(
                settings,
                "PUBLIC_LANG_BYPASS_PREFIXES",
                ("/panel/", "/admin/", "/i18n/", "/static/"),
            )
        ),
    )


def _get_client_ip(request: HttpRequest) -> str | None:
    # nginx/proxy: first IP in X-Forwarded-For
    xff = request.META.get("HTTP_X_FORWARDED_FOR")
    if xff:
        ip = xff.split(",")[0].strip()
        return ip or None
    ip = request.META.get("REMOTE_ADDR")
    return ip or None


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
    # RU -> de (как ты сказал), DE -> de, всё остальное -> de
    return default_lang


def _split_lang_prefix(path: str, public_langs: tuple[str, ...]) -> tuple[str | None, str]:
    # returns (lang_prefix_or_none, rest_path_starting_with_slash)
    # "/de" or "/de/" treated as prefix
    parts = path.split("/", 2)  # ["", "de", "rest..."]
    if len(parts) >= 2:
        maybe = parts[1]
        if maybe in public_langs:
            rest = "/" + (parts[2] if len(parts) == 3 else "")
            if rest == "/":
                return maybe, "/"
            return maybe, rest
    return None, path


def _redirect(to_path: str) -> HttpResponseRedirect:
    return HttpResponseRedirect(to_path)


def _activate(request: HttpRequest, lang: str) -> None:
    translation.activate(lang)
    request.LANGUAGE_CODE = lang


class PublicLangMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        cfg = _cfg()
        path = request.path or "/"

        # bypass for panel/admin/static/i18n
        for pfx in cfg.bypass_prefixes:
            if path.startswith(pfx):
                return self.get_response(request)

        url_lang, url_rest = _split_lang_prefix(path, cfg.public_langs)
        cookie_lang = request.COOKIES.get(cfg.cookie_name)

        # CASE A: cookie отсутствует -> игнорируем URL, редирект по geo
        if not cookie_lang:
            ip = _get_client_ip(request)
            reader = _get_reader(cfg.geo_db_path)
            country = None
            if reader and ip:
                try:
                    country = reader.country(ip).country.iso_code
                except Exception:
                    country = None
            lang = _country_to_lang(country, cfg.default_lang)

            _activate(request, lang)

            # если URL уже был /xx/... — режем префикс и редиректим на geo-lang
            target = f"/{lang}{url_rest if url_lang else path}"
            resp = _redirect(target)
            resp.set_cookie(cfg.cookie_name, lang, max_age=cfg.cookie_max_age, samesite="Lax")

            translation.deactivate()
            return resp

        # CASE B: cookie есть
        # B1) пользователь пришел на /xx/... => это смена языка: обновить cookie и пропустить дальше
        if url_lang:
            _activate(request, url_lang)
            try:
                resp = self.get_response(request)
                if url_lang != cookie_lang:
                    resp.set_cookie(cfg.cookie_name, url_lang, max_age=cfg.cookie_max_age, samesite="Lax")
                return resp
            finally:
                translation.deactivate()

        # B2) URL без префикса => редирект на cookie-язык
        _activate(request, cookie_lang)

        target = f"/{cookie_lang}{path}"
        resp = _redirect(target)

        translation.deactivate()
        return resp
