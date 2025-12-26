# FILE: engine/common/prompts/process.py  (обновлено — 2025-12-26)

from pathlib import Path

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
            model="gpt-5.1",
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
            model="gpt-5.1",
            service_tier="flex",
            user_id="SYSTEM",
            instructions=instructions,
            input=text,
            use_cache=True,
        )
        return (resp.content or "").strip()
    except Exception:
        return ""
