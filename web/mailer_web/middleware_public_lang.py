# FILE: web/mailer_web/middleware_public_lang.py  (обновлено — 2026-02-10)
# PURPOSE:
# - Public: язык через /{lang}/... + cookie+geo редиректы (как было), но синхронизируем serenity_lang и django_language.
# - Panel (/panel/...): НИКАКИХ редиректов; activate(lang) по cookie. Приоритет: django_language (setlang) -> serenity_lang -> geo.
#   Если пользователь переключил язык через setlang (django_language), синхронизируем serenity_lang = django_language, чтобы всё было едино.
# - Admin (/admin/...): принудительно RU, без редиректов и без влияния переключалки.

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
        public_langs=tuple(getattr(settings, "PUBLIC_LANGS", ("ru", "de", "uk", "en"))),
        default_lang=str(getattr(settings, "PUBLIC_LANG_DEFAULT", "de")),
        geo_db_path=Path(getattr(settings, "PUBLIC_GEOIP_DB_PATH", getattr(settings, "GEOIP_PATH", "")))
        / "GeoLite2-Country.mmdb",
        # ВАЖНО: panel НЕ байпасим, иначе меню не переводится
        bypass_prefixes=tuple(
            getattr(
                settings,
                "PUBLIC_LANG_BYPASS_PREFIXES",
                ("/admin/", "/i18n/", "/static/"),
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


def _split_lang_prefix(path: str, public_langs: tuple[str, ...]) -> tuple[str | None, str]:
    parts = path.split("/", 2)  # ["", "de", "rest..."]
    if len(parts) >= 2:
        maybe = parts[1]
        if maybe in public_langs:
            rest = "/" + (parts[2] if len(parts) == 3 else "")
            return maybe, ("/" if rest == "/" else rest)
    return None, path


def _redirect(to_path: str) -> HttpResponseRedirect:
    return HttpResponseRedirect(to_path)


def _activate(request: HttpRequest, lang: str) -> None:
    translation.activate(lang)
    request.LANGUAGE_CODE = lang


def _set_lang_cookie(resp: HttpResponse, name: str, value: str, max_age: int) -> None:
    resp.set_cookie(name, value, max_age=max_age, samesite="Lax")


def _sync_cookies(
    resp: HttpResponse,
    cfg: _Cfg,
    *,
    lang: str,
    serenity_lang: str | None,
    django_lang: str | None,
    set_django: bool,
    set_serenity: bool,
) -> None:
    if not _is_valid_lang(cfg, lang):
        return

    if set_serenity and (lang != (serenity_lang or "")):
        _set_lang_cookie(resp, cfg.cookie_name, lang, cfg.cookie_max_age)

    if set_django and (lang != (django_lang or "")):
        _set_lang_cookie(resp, _django_lang_cookie_name(), lang, cfg.cookie_max_age)


class PublicLangMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        cfg = _cfg()
        path = request.path or "/"

        serenity_lang = request.COOKIES.get(cfg.cookie_name)
        django_lang = request.COOKIES.get(_django_lang_cookie_name())

        # --- PANEL: без редиректов, только activate ---
        if path.startswith("/panel/") or path == "/panel":
            try:
                # приоритет: django_language (setlang) -> serenity_lang -> geo
                lang = django_lang or serenity_lang or _pick_geo_lang(cfg, request)
                if not _is_valid_lang(cfg, lang):
                    lang = cfg.default_lang

                _activate(request, lang)
                resp = self.get_response(request)

                # Ключевое: если пользователь переключил через setlang (django_lang),
                # то дожимаем serenity_lang = django_lang.
                if _is_valid_lang(cfg, django_lang):
                    _sync_cookies(
                        resp,
                        cfg,
                        lang=str(django_lang),
                        serenity_lang=serenity_lang,
                        django_lang=django_lang,
                        set_django=False,   # setlang уже поставил
                        set_serenity=True,  # синхронизируем serenity
                    )
                else:
                    # если setlang не использовался, держим django в синке с serenity/geo,
                    # чтобы дальше в панели LocaleMiddleware не жил своей жизнью
                    _sync_cookies(
                        resp,
                        cfg,
                        lang=lang,
                        serenity_lang=serenity_lang,
                        django_lang=django_lang,
                        set_django=True,
                        set_serenity=True,
                    )

                return resp
            finally:
                translation.deactivate()

        # --- ADMIN: всегда RU, без редиректов/кук ---
        if path.startswith("/admin/") or path == "/admin":
            try:
                _activate(request, "ru")
                return self.get_response(request)
            finally:
                translation.deactivate()

        # --- bypass for static/i18n (admin уже обработали выше) ---
        for pfx in cfg.bypass_prefixes:
            if path.startswith(pfx):
                return self.get_response(request)

        url_lang, url_rest = _split_lang_prefix(path, cfg.public_langs)

        # CASE A: serenity_lang отсутствует/битый -> редирект по geo (как было), и ставим обе куки
        if not _is_valid_lang(cfg, serenity_lang):
            lang = _pick_geo_lang(cfg, request)
            target = f"/{lang}{url_rest if url_lang else path}"
            resp = _redirect(target)
            _sync_cookies(
                resp,
                cfg,
                lang=lang,
                serenity_lang=serenity_lang,
                django_lang=django_lang,
                set_django=True,
                set_serenity=True,
            )
            return resp

        # CASE B: serenity_lang есть
        # B1) пришли на /xx/... => это смена языка: обновить куки и пропустить дальше
        if url_lang:
            _activate(request, url_lang)
            try:
                resp = self.get_response(request)
                _sync_cookies(
                    resp,
                    cfg,
                    lang=url_lang,
                    serenity_lang=serenity_lang,
                    django_lang=django_lang,
                    set_django=True,
                    set_serenity=True,
                )
                return resp
            finally:
                translation.deactivate()

        # B2) URL без префикса => редирект на serenity_lang
        target = f"/{serenity_lang}{path}"
        resp = _redirect(target)
        return resp
