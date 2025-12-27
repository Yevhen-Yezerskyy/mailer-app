# FILE: engine/common/prompts/process.py  (обновлено — 2025-12-26)
# Смысл: переводы промптов/текста через GPT + получение/кеширование переводов gb_branches (DE — отдаём оригинал сразу; остальные языки: memo→DB→GPT→DB).

from pathlib import Path

from engine.common.cache.client import memo
from engine.common.db import execute, fetch_one
from engine.common.gpt import GPTClient


BASE_DIR = Path(__file__).resolve().parent

PROMPTS_TRANSLATE_KEY = "prompt_translate"

LANG_MAP = {
    "en": "English",
    "eng": "English",
    "de": "German",
    "deu": "German",
    "ru": "Russian",
    "rus": "Russian",
    "fr": "French",
    "es": "Spanish",
    "sv": "Swedish",
    "uk": "Ukrainian",
    "ukr": "Ukrainian",
}


def _lang_name(lang: str) -> str:
    key = (lang or "").strip().lower()
    return LANG_MAP.get(key, lang)


def _get_translate_instructions(lang: str) -> str:
    path = BASE_DIR / f"{PROMPTS_TRANSLATE_KEY}.txt"
    if not path.exists():
        return ""

    tpl = path.read_text(encoding="utf-8")
    if not tpl:
        return ""

    return tpl.replace("{LANG}", _lang_name(lang))


def get_prompt(key: str, lang: str = "en") -> str:
    """
    Берёт файл <key>.txt и переводит его на нужный язык через GPT.
    """
    try:
        path = BASE_DIR / f"{key}.txt"
        if not path.exists():
            return ""

        text = path.read_text(encoding="utf-8")
        if not text:
            return ""

        instructions = _get_translate_instructions(lang)
        if not instructions:
            return ""

        resp = GPTClient().ask(
            model="mini",
            service_tier="flex",
            user_id="SYSTEM",
            instructions=instructions,
            input=text,
            use_cache=True,
        )
        return (resp.content or "").strip()
    except Exception:
        return ""


def translate_text(text: str, lang: str = "en") -> str:
    """
    Переводит произвольный текст на указанный язык через GPT.
    """
    if not text:
        return ""

    try:
        instructions = _get_translate_instructions(lang)
        if not instructions:
            return ""

        resp = GPTClient().ask(
            model="mini",
            service_tier="flex",
            user_id="SYSTEM",
            instructions=instructions,
            input=text,
            use_cache=True,
        )
        return (resp.content or "").strip()
    except Exception:
        return ""


def _fetch_branch_name_de(branch_id: int) -> str:
    row = fetch_one("SELECT name FROM public.gb_branches WHERE id = %s", (branch_id,))
    return (row[0] or "").strip() if row else ""


def _memo_calc_branch_name(query):
    """
    query = ("gb_branch_name", branch_id, lang)
    """
    _, branch_id, lang = query

    # 1) i18n hit
    row = fetch_one(
        """
        SELECT name_trans
        FROM public.gb_branch_i18n
        WHERE branch_id = %s AND lang = %s
        """,
        (branch_id, lang),
    )
    if row and row[0]:
        return (row[0] or "").strip()

    # 2) original
    name_original = _fetch_branch_name_de(branch_id)
    if not name_original:
        return ""

    # 3) translate
    name_trans = (translate_text(name_original, lang) or "").strip()
    if not name_trans:
        return name_original

    # 4) upsert
    execute(
        """
        INSERT INTO public.gb_branch_i18n (branch_id, lang, name_original, name_trans)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (branch_id, lang)
        DO UPDATE SET
          name_original = EXCLUDED.name_original,
          name_trans = EXCLUDED.name_trans,
          updated_at = NOW()
        """,
        (branch_id, lang, name_original, name_trans),
    )
    return name_trans


def get_branch_name(branch_id: int, lang: str) -> str:
    """
    Возвращает имя бранча на нужном языке.
    - lang=de: сразу отдаём оригинал (без memo/кеша/i18n/GPT).
    - иначе: memo-cache -> DB -> GPT -> DB.
    """
    try:
        lang = (lang or "de").strip().lower()

        if lang == "de":
            return _fetch_branch_name_de(branch_id)

        return (memo(("gb_branch_name", int(branch_id), lang), _memo_calc_branch_name, version="gb_branch_name_v1") or "").strip()
    except Exception:
        return ""
