# FILE: engine/common/prompts/process.py  (обновлено — 2026-04-06)
# Смысл: переводы промптов/текста через GPT + получение/кеширование переводов gb_branches (DE — отдаём оригинал сразу; остальные языки: memo→DB→GPT→DB).

import hashlib
import json
import pickle
from pathlib import Path

from engine.common.cache.client import CLIENT, DEFAULT_TTL_SEC, memo
from engine.common.db import execute, fetch_one
from engine.common.gpt import GPTClient


BASE_DIR = Path(__file__).resolve().parent

PROMPTS_TRANSLATE_KEY = "prompt_translate"
TRANSLATION_VERIFY_SYSTEM = (
    "You compare two texts. "
    "Answer yes if the result is a faithful translation of the original, "
    "or if both texts have the same meaning in the same language. "
    "Answer no otherwise. "
    "Return only yes or no."
)
TRANSLATION_CACHE_VERSION = "translation.checked.v1"

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


def _translation_cache_key(*, model: str, instructions: str, input_text: str) -> str:
    query = (TRANSLATION_CACHE_VERSION, model, instructions, input_text)
    raw = pickle.dumps(query, protocol=pickle.HIGHEST_PROTOCOL)
    return "prompt_translate:" + hashlib.sha1(raw).hexdigest()


def _translation_cache_get(key: str) -> str | None:
    payload = CLIENT.get(key, ttl_sec=DEFAULT_TTL_SEC)
    if payload is None:
        return None
    try:
        value = pickle.loads(payload)
    except Exception:
        return None
    return value if isinstance(value, str) else None


def _translation_cache_set(key: str, value: str) -> None:
    try:
        payload = pickle.dumps(str(value), protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        return
    CLIENT.set(key, payload, ttl_sec=DEFAULT_TTL_SEC)


def _is_translation_valid(original_text: str, result_text: str) -> bool:
    original = str(original_text or "").strip()
    result = str(result_text or "").strip()
    if not original or not result:
        return False
    if original == result:
        return True

    payload = json.dumps(
        {
            "original_text": original_text,
            "result_text": result_text,
        },
        ensure_ascii=False,
    )
    try:
        resp = GPTClient().ask(
            model="gpt-5.4-nano",
            service_tier="flex",
            user_id="SYSTEM",
            instructions=TRANSLATION_VERIFY_SYSTEM,
            input=payload,
            use_cache=False,
            web_search=False,
        )
    except Exception:
        return False

    answer = str(resp.content or "").strip().lower()
    return answer == "yes"


def _translate_checked(*, model: str, instructions: str, input_text: str, fallback_text: str) -> str:
    cache_key = _translation_cache_key(model=model, instructions=instructions, input_text=input_text)
    cached = _translation_cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        resp = GPTClient().ask(
            model=model,
            service_tier="flex",
            user_id="SYSTEM",
            instructions=instructions,
            input=input_text,
            use_cache=False,
            web_search=False,
        )
    except Exception:
        return fallback_text

    translated = str(resp.content or "").strip()
    if not _is_translation_valid(input_text, translated):
        return fallback_text

    _translation_cache_set(cache_key, translated)
    return translated


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

        return _translate_checked(
            model="gpt-5.4",
            instructions=instructions,
            input_text=text,
            fallback_text=text,
        )
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

        return _translate_checked(
            model="gpt-5.4",
            instructions=instructions,
            input_text=text,
            fallback_text=text,
        )
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



def denormalize_branches_prompt(text: str) -> str:

    if not text:
        return ""

    try:
        src_path = BASE_DIR / "source_branches.txt"
        if not src_path.exists():
            return text

        branches_raw = src_path.read_text(encoding="utf-8").strip()
        if not branches_raw:
            return text

        system_instructions = get_prompt("denormalize_branches").strip()
        if not system_instructions:
            return text
        
        system_instructions = system_instructions + "\n\n EXCEPTIONS: \n" + branches_raw

        resp = GPTClient().ask(
            model="mini",
            service_tier="flex",
            user_id="SYSTEM",
            instructions=system_instructions,
            input=text,
            use_cache=True,
        )

        return (resp.content or "").strip() or text

    except Exception:
        return text
