# FILE: web/mailer_web/format_contact.py  (updated: 2026-04-01)
# PURPOSE:
# - Contact-format helpers.
# - Category title on current interface language with request + memo + DB caching.

from __future__ import annotations

from typing import Any

from django.conf import settings
from django.db import connection
from django.http import HttpRequest

from engine.common.cache.client import memo
from engine.common.gpt import GPTClient


def get_category_title(category_id: Any, request: HttpRequest, single: bool = False) -> str:
    category_id = int(category_id)
    lang = request.ui_lang_code
    updated = False
    translated_once = bool(
        getattr(request, "_format_contact_category_titles_translated_once", False)
    )

    def _load_vocabulary(_query: Any) -> dict[int, dict[str, str]]:
        vocabulary = getattr(request, "_format_contact_category_titles_cache", None)
        if vocabulary is not None:
            return vocabulary
        return {}

    def _translate_category(payload: dict[str, str]) -> dict[str, str]:
        prompts = {
            "en": (
                "Translate this business category title from German business directories "
                "from German into English. Return only the translated category title."
            ),
            "ru": (
                "Translate this business category title from German business directories "
                "from German into Russian. Return only the translated category title."
            ),
            "uk": (
                "Translate this business category title from German business directories "
                "from German into Ukrainian. Return only the translated category title."
            ),
        }
        upsert_rows: list[tuple[int, str, str]] = []

        for target_lang in tuple(getattr(settings, "PUBLIC_LANGS", ())):
            if not target_lang or target_lang == "de" or payload.get(target_lang):
                continue
            prompt = prompts.get(target_lang)
            if not prompt:
                continue

            resp = GPTClient().ask(
                model="gpt-5.4",
                instructions=prompt,
                input=payload["de"],
                user_id=f"mailer_web.format_contact.category_translate.{target_lang}",
                service_tier="flex",
                use_cache=True,
                web_search=False,
            )
            translated_title = " ".join(str(resp.content or "").split()).strip()
            if not translated_title:
                continue

            payload[target_lang] = translated_title
            upsert_rows.append((category_id, target_lang, translated_title))

        if upsert_rows:
            with connection.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO public.branches_sys_langs (id, lang, branch_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id, lang)
                    DO UPDATE SET branch_name = EXCLUDED.branch_name
                    """,
                    upsert_rows,
                )

        return payload

    vocabulary = getattr(request, "_format_contact_category_titles_cache", None)
    if vocabulary is None:
        vocabulary = memo(
            ("format_contact:category_titles:v1",),
            _load_vocabulary,
            ttl=7 * 24 * 60 * 60,
            version="format_contact:category_titles:v1",
        )
        request._format_contact_category_titles_cache = vocabulary

    def _build_title(payload: dict[str, str]) -> str:
        title_de = payload.get("de") or ""
        title_lang = payload.get(lang) or ""
        if single:
            return title_lang or title_de or ""
        if lang == "de" or not title_lang or title_lang == title_de:
            return title_de or title_lang or ""
        return f"{title_de} - {title_lang}"

    payload = vocabulary.get(category_id)
    if payload is not None and payload.get(lang):
        return _build_title(payload)

    if payload is None:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                    bs.branch_name AS branch_name_de,
                    bsl.lang,
                    bsl.branch_name
                FROM public.branches_sys bs
                LEFT JOIN public.branches_sys_langs bsl
                  ON bsl.id = bs.id
                WHERE bs.id = %s
                """,
                [category_id],
            )
            rows = cur.fetchall() or []

        if not rows:
            raise Exception(f"CATEGORY_NOT_FOUND: {category_id}")

        payload = {}
        title_de = " ".join(str(rows[0][0] or "").split()).strip()
        if not title_de:
            raise Exception(f"CATEGORY_DE_TITLE_EMPTY: {category_id}")
        payload["de"] = title_de

        for _title_de, row_lang, row_title in rows:
            row_lang = str(row_lang or "").strip()
            row_title = " ".join(str(row_title or "").split()).strip()
            if row_lang and row_title:
                payload[row_lang] = row_title

        updated = True

    if payload.get(lang):
        vocabulary[category_id] = payload
        request._format_contact_category_titles_cache = vocabulary
        if updated:
            memo(
                ("format_contact:category_titles:v1",),
                _load_vocabulary,
                ttl=7 * 24 * 60 * 60,
                version="format_contact:category_titles:v1",
                update=True,
            )
        return _build_title(payload)

    if payload.get("de") and not translated_once:
        request._format_contact_category_titles_translated_once = True
        payload = _translate_category(dict(payload))
        updated = True

    vocabulary[category_id] = payload
    request._format_contact_category_titles_cache = vocabulary
    if updated:
        memo(
            ("format_contact:category_titles:v1",),
            _load_vocabulary,
            ttl=7 * 24 * 60 * 60,
            version="format_contact:category_titles:v1",
            update=True,
        )

    return _build_title(payload)


def get_city_title(plz_id: Any, request: HttpRequest, land: bool = False, plz: bool = False) -> str:
    plz_id = int(plz_id)
    updated = False

    def _load_vocabulary(_query: Any) -> dict[int, dict[str, str]]:
        vocabulary = getattr(request, "_format_contact_city_titles_cache", None)
        if vocabulary is not None:
            return vocabulary
        return {}

    vocabulary = getattr(request, "_format_contact_city_titles_cache", None)
    if vocabulary is None:
        vocabulary = memo(
            ("format_contact:city_titles:v1",),
            _load_vocabulary,
            ttl=7 * 24 * 60 * 60,
            version="format_contact:city_titles:v1",
        )
        request._format_contact_city_titles_cache = vocabulary

    payload = vocabulary.get(plz_id)
    if payload is None:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(ps.plz, '') AS plz,
                    COALESCE(cs.name, '') AS city_name,
                    COALESCE(cs.state_name, '') AS state_name
                FROM public.plz_sys ps
                LEFT JOIN public.__city__plz_map cpm
                  ON cpm.plz = ps.plz
                LEFT JOIN public.cities_sys cs
                  ON cs.id = cpm.city_id
                WHERE ps.id = %s
                LIMIT 1
                """,
                [plz_id],
            )
            row = cur.fetchone()

        if not row:
            raise Exception(f"PLZ_NOT_FOUND: {plz_id}")

        payload = {
            "plz": " ".join(str(row[0] or "").split()).strip(),
            "city": " ".join(str(row[1] or "").split()).strip(),
            "state": " ".join(str(row[2] or "").split()).strip(),
        }
        vocabulary[plz_id] = payload
        request._format_contact_city_titles_cache = vocabulary
        updated = True

    if updated:
        memo(
            ("format_contact:city_titles:v1",),
            _load_vocabulary,
            ttl=7 * 24 * 60 * 60,
            version="format_contact:city_titles:v1",
            update=True,
        )

    city_name = str(payload.get("city") or "").strip()
    city_parts = [part.strip() for part in city_name.split(",")]
    city_suffix = ", ".join([part for part in city_parts[1:] if part])
    if city_suffix in {"Stadt", "St", "GKSt", "M"}:
        city_name = city_parts[0] if city_parts else city_name
    state_name = str(payload.get("state") or "").strip()
    plz_value = str(payload.get("plz") or "").strip()

    if land and city_name and state_name:
        title = f"{city_name}, {state_name}"
    elif land and state_name:
        title = state_name
    else:
        title = city_name or state_name

    if plz and plz_value and title:
        return f"{plz_value} {title}"
    if plz and plz_value:
        return plz_value

    return title


__all__ = ["get_category_title", "get_city_title"]
