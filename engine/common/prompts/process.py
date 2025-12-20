# FILE: engine/common/prompts/__init__.py  (новое) 2025-12-20
# Минимальный prompt API: файл -> GPT-перевод через instructions с {LANG}

from pathlib import Path
from engine.common.gpt import GPTClient


BASE_DIR = Path(__file__).resolve().parent

PROMPTS_USER_ID = "SYSTEM"
PROMPTS_TRANSLATE_KEY = "prompt_translate"
PROMPTS_TRANSLATE_TIER = "mini"

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


def get_prompt(key: str, lang: str = "en") -> str:
    try:
        path = BASE_DIR / f"{key}.txt"
        if not path.exists():
            return ""
        text = path.read_text(encoding="utf-8")
        return set_lang_prompt(text, lang)
    except Exception:
        return ""


def set_lang_prompt(text: str, lang: str = "en") -> str:
    try:
        path = BASE_DIR / f"{PROMPTS_TRANSLATE_KEY}.txt"
        if not path.exists():
            return ""

        instr_tpl = path.read_text(encoding="utf-8")
        if not instr_tpl:
            return ""

        lang_key = (lang or "").strip().lower()
        lang_name = LANG_MAP.get(lang_key, lang)

        instructions = instr_tpl.replace("{LANG}", lang_name)

        client = GPTClient()

        resp = client.ask(
            tier=PROMPTS_TRANSLATE_TIER,
            user_id=PROMPTS_USER_ID,
            instructions=instructions,
            input=text,
            use_cache=True,
        )
        return (resp.content or "").strip()
    except Exception:
        return ""
