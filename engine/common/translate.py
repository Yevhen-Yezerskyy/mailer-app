# FILE: engine/common/translate.py
# DATE: 2026-04-12
# PURPOSE: Global translation helpers for prompts/text: get_prompt and translate_text.

from __future__ import annotations

import hashlib
import json
import pickle
import random
from pathlib import Path
from typing import Callable

from engine.common.cache.client import CLIENT, DEFAULT_TTL_SEC
from engine.common.gpt import GPTClient

BASE_DIR = Path(__file__).resolve().parent / "prompts"
PROMPTS_TRANSLATE_KEY = "prompt_translate"
TRANSLATION_VERIFY_SYSTEM = (
    "You compare two texts. "
    "Answer yes if the result is a faithful translation of the original, "
    "or if both texts have the same meaning in the same language. "
    "Answer no otherwise. "
    "Return only yes or no."
)
TRANSLATION_CACHE_VERSION = "translation.checked.v1"
MIN_TRANSLATION_CACHE_DAYS = 7
MAX_TRANSLATION_CACHE_DAYS = 21

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


def _notify_on_gpt_error(on_gpt_error: Callable[[], None] | None) -> None:
    if on_gpt_error is None:
        return
    try:
        on_gpt_error()
    except Exception:
        return


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


def _random_translation_cache_ttl_sec() -> int:
    days = random.randint(MIN_TRANSLATION_CACHE_DAYS, MAX_TRANSLATION_CACHE_DAYS)
    return int(days * 24 * 60 * 60)


def _translation_cache_set(key: str, value: str) -> None:
    try:
        payload = pickle.dumps(str(value), protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        return
    CLIENT.set(key, payload, ttl_sec=_random_translation_cache_ttl_sec())


def _is_translation_valid(
    original_text: str,
    result_text: str,
    *,
    on_gpt_error: Callable[[], None] | None = None,
) -> bool:
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
            model="nano",
            service_tier="flex",
            user_id="SYSTEM",
            instructions=TRANSLATION_VERIFY_SYSTEM,
            input=payload,
            use_local_cache=False,
            web_search=False,
        )
    except Exception:
        _notify_on_gpt_error(on_gpt_error)
        return False

    if str(resp.status or "").strip().upper() != "OK":
        _notify_on_gpt_error(on_gpt_error)
        return False

    answer = str(resp.content or "").strip().lower()
    return answer == "yes"


def _translate_checked(
    *,
    model: str,
    instructions: str,
    input_text: str,
    fallback_text: str,
    on_gpt_error: Callable[[], None] | None = None,
) -> str:
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
            use_local_cache=False,
            web_search=False,
        )
    except Exception:
        _notify_on_gpt_error(on_gpt_error)
        return fallback_text

    if str(resp.status or "").strip().upper() != "OK":
        _notify_on_gpt_error(on_gpt_error)
        return fallback_text

    translated = str(resp.content or "").strip()
    original = str(input_text or "").strip()
    if translated != original and not _is_translation_valid(
        input_text,
        translated,
        on_gpt_error=on_gpt_error,
    ):
        return fallback_text

    _translation_cache_set(cache_key, translated)
    return translated


def get_prompt(key: str, lang: str = "en", on_gpt_error: Callable[[], None] | None = None) -> str:
    """
    Reads <key>.txt from engine/common/prompts and translates it via GPT.
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
            model="standard",
            instructions=instructions,
            input_text=text,
            fallback_text=text,
            on_gpt_error=on_gpt_error,
        )
    except Exception:
        return ""


def translate_text(text: str, lang: str = "en", on_gpt_error: Callable[[], None] | None = None) -> str:
    """
    Translates arbitrary text to target language via GPT.
    """
    if not text:
        return ""

    try:
        instructions = _get_translate_instructions(lang)
        if not instructions:
            return ""

        return _translate_checked(
            model="standard",
            instructions=instructions,
            input_text=text,
            fallback_text=text,
            on_gpt_error=on_gpt_error,
        )
    except Exception:
        return ""
