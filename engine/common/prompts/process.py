# FILE: engine/common/prompts/process.py  (новое) 2025-12-15
# Fix: добавлен простой debug-print (env PROMPTS_DEBUG=1) без влияния на логику.

"""
Хранилище и обработчик промптов.

Структура:
---------
engine/common/prompts/
    process.py
    state.json
    *.txt          — исходные промпты (любой язык)
    eng/*.txt      — английские версии (генерируются автоматически)

Логика:
-------
- Промпты храним только как .txt.
- Вся мета (mtime, debug, name/rate, EN-мета) живёт в state.json.
- Nano (оценка качества) выполняется ТОЛЬКО если файл реально изменился.
- Nano всегда обновляет name и rate (debug на это не влияет).
- Перевод на EN выполняется ТОЛЬКО если debug == "".
- Если debug != "" — EN-файл удаляется (если был).
- get_prompt(key) возвращает актуальный текст (EN, если свежий, иначе исходник),
  и НИКОГДА не кидает исключения, максимум "".

DEBUG PRINT:
------------
Включается переменной окружения PROMPTS_DEBUG=1
Печатает только ход обработки (что тронулось/пропущено/удалено), не печатает тексты промптов.

TODO:
-----
- Добавить checksum текста в state, чтобы ловить изменения даже при одинаковом mtime.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from engine.common.gpt import GPTClient

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
ENG_DIR = BASE_DIR / "eng"
STATE_PATH = BASE_DIR / "state.json"

GPT_WORKSPACE_ID = "prompts"
GPT_USER_ID = "process"

_DEBUG = os.getenv("PROMPTS_DEBUG", "").strip() in ("1", "true", "True", "yes", "YES", "on", "ON")


def _d(msg: str) -> None:
    if _DEBUG:
        try:
            print(f"[prompts] {msg}", flush=True)
        except Exception:
            pass


# ============================================================================
# state.json
# ============================================================================

def load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            logger.error("state.json поврежден — создаю пустой.")
            _d("state.json damaged -> using empty state")
    else:
        _d("state.json missing -> using empty state")
    return {}


def save_state(state: Dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    _d(f"state.json saved ({len(state)} keys)")


# ============================================================================
# PUBLIC API: get_prompt(key)
# ============================================================================

def get_prompt(key: str, lang: str = "same") -> str:
    """
    Вернуть АКТУАЛЬНЫЙ текст промпта:
    - если есть eng/<key>.txt и он не старее исходника → вернуть его
    - иначе вернуть <key>.txt
    - если ничего нет → вернуть ""

    Язык:
        lang = "same"  → ничего не добавляем, модель сама выбирает язык ответа.
        lang = "en"/"de"/"uk"/... → в конец промпта добавляется строка
                                   LANGUAGE OF THE RESPONSE: <lang>
    """
    try:
        state = load_state()
    except Exception:
        _d("load_state failed in get_prompt -> ''")
        return ""

    source_file = BASE_DIR / f"{key}.txt"
    if not source_file.exists():
        _d(f"get_prompt: missing {key}.txt -> ''")
        return ""

    # ---- читаем оригинал ----
    try:
        source_text = source_file.read_text(encoding="utf-8")
    except Exception:
        source_text = ""
        _d(f"get_prompt: read failed {key}.txt -> '' (fallback empty)")

    entry = state.get(key, {}) if isinstance(state, dict) else {}
    source_mtime = entry.get("source_mtime", 0)

    eng_file = ENG_DIR / f"{key}.txt"
    eng_mtime = entry.get("eng_mtime")

    # ---- выбираем актуальный текст ----
    if eng_file.exists() and isinstance(eng_mtime, (int, float)) and eng_mtime >= source_mtime:
        try:
            prompt_text = eng_file.read_text(encoding="utf-8")
            _d(f"get_prompt: {key} -> ENG (eng_mtime={eng_mtime}, source_mtime={source_mtime})")
        except Exception:
            prompt_text = source_text
            _d(f"get_prompt: {key} -> ENG read failed, fallback SOURCE")
    else:
        prompt_text = source_text
        _d(f"get_prompt: {key} -> SOURCE")

    if lang == "same":
        return prompt_text

    appendix = f"\n\nLANGUAGE OF THE RESPONSE: {lang}\n"
    return prompt_text.rstrip() + appendix


# ============================================================================
# GPT helpers
# ============================================================================

def nano_eval(text: str) -> tuple[str, int]:
    """
    Nano-оценка качества промпта: (name, rate).
    Вызывается ТОЛЬКО когда исходный файл реально изменился.
    """
    system_prompt = get_prompt("prompt_quality")
    if not system_prompt:
        _d("nano_eval: missing system prompt prompt_quality -> ('',100)")
        return "", 100

    client = GPTClient()

    try:
        resp = client.ask(
            tier="nano",
            workspace_id=GPT_WORKSPACE_ID,
            user_id=GPT_USER_ID,
            system=system_prompt,
            user=text,
            with_web=False,
            endpoint="prompt_quality",
        )
        raw = (resp.content or "").strip()
        data = json.loads(raw)
        name = str(data.get("name", "") or "")
        rate = int(data.get("rate", 100) or 100)
        _d(f"nano_eval: ok name='{name}' rate={rate}")
        return name, rate
    except Exception as e:
        logger.error("Nano eval error: %s", e)
        _d(f"nano_eval: error -> ('',100) ({e})")
        return "", 100


def translate_to_en(text: str) -> str:
    """
    Перевод промпта на английский (maxi / GPT-5.1).
    Выполняется только при debug == "".
    """
    system_prompt = get_prompt("prompt_translate")
    if not system_prompt:
        _d("translate_to_en: missing system prompt prompt_translate -> return source")
        return text

    client = GPTClient()

    try:
        resp = client.ask(
            tier="maxi",
            workspace_id=GPT_WORKSPACE_ID,
            user_id=GPT_USER_ID,
            system=system_prompt,
            user=text,
            with_web=False,
            endpoint="prompt_translate",
        )
        out = (resp.content or "").strip()
        _d(f"translate_to_en: ok (len={len(out)})")
        return out
    except Exception as e:
        logger.error("EN translation error: %s", e)
        _d(f"translate_to_en: error -> return source ({e})")
        return text


# ============================================================================
# Обработка одного промпта
# ============================================================================

def process_prompt(key: str, state: Dict[str, Any]) -> None:
    """
    Обрабатывает ОДИН промпт:
    - если файл удалён → зачистка state и EN
    - если файл НЕ менялся → вообще ничего не делаем
    - если файл изменился:
        * nano → name, rate
        * debug по умолчанию "YES", если не был задан
        * если debug == "" → перевод EN + nano по EN
        * если debug != "" → EN удаляем
    """
    source_file = BASE_DIR / f"{key}.txt"
    eng_file = ENG_DIR / f"{key}.txt"

    if not source_file.exists():
        _d(f"process_prompt: {key} deleted -> cleanup state + eng")
        state.pop(key, None)
        if eng_file.exists():
            try:
                eng_file.unlink()
                _d(f"process_prompt: eng/{key}.txt removed")
            except Exception:
                _d(f"process_prompt: eng/{key}.txt remove failed")
        return

    source_mtime = source_file.stat().st_mtime
    entry = state.get(key) or {}

    prev_mtime = entry.get("source_mtime")
    if isinstance(prev_mtime, (int, float)) and source_mtime <= prev_mtime:
        _d(f"process_prompt: {key} unchanged (mtime={source_mtime}) -> skip")
        return

    _d(f"process_prompt: {key} changed (prev={prev_mtime}, now={source_mtime}) -> eval")

    source_text = source_file.read_text(encoding="utf-8")
    debug = entry.get("debug", "YES")  # по умолчанию ВСЕГДА что-то

    # --- 1) nano-оценка (ВСЕГДА при изменении файла) ---
    name, rate = nano_eval(source_text)
    now_iso = datetime.utcnow().isoformat(timespec="seconds")

    entry["name"] = name
    entry["rate"] = rate
    entry["source_mtime"] = source_mtime
    entry["last_eval"] = now_iso
    entry.setdefault("debug", debug)

    _d(f"process_prompt: {key} -> name='{name}' rate={rate} debug='{entry.get('debug','')}'")

    # --- 2) EN-логика зависит только от debug ---
    if debug == "":
        _d(f"process_prompt: {key} debug=='' -> translate + nano_en + write eng")
        translated = translate_to_en(source_text)

        ENG_DIR.mkdir(exist_ok=True)
        eng_file.write_text(translated, encoding="utf-8")
        eng_mtime = eng_file.stat().st_mtime

        name_en, rate_en = nano_eval(translated)

        entry["name_en"] = name_en
        entry["rate_en"] = rate_en
        entry["eng_mtime"] = eng_mtime

        _d(f"process_prompt: {key} -> name_en='{name_en}' rate_en={rate_en} eng_mtime={eng_mtime}")
    else:
        _d(f"process_prompt: {key} debug!='' -> ensure eng deleted")
        if eng_file.exists():
            try:
                eng_file.unlink()
                _d(f"process_prompt: eng/{key}.txt removed")
            except Exception:
                _d(f"process_prompt: eng/{key}.txt remove failed")
        entry["name_en"] = ""
        entry["rate_en"] = 0
        entry["eng_mtime"] = None

    state[key] = entry


# ============================================================================
# Полный проход
# ============================================================================

def process_once(verbose: bool = True) -> None:
    """
    Один проход по всем *.txt в каталоге (кроме state.json).
    """
    state = load_state()

    keys = [p.stem for p in BASE_DIR.glob("*.txt") if p.name != "state.json"]

    _d(f"process_once: found {len(keys)} prompts")
    if verbose:
        logger.info("Найдено промптов: %s", keys)

    for key in keys:
        if verbose:
            logger.info("Обрабатываю промпт: %s", key)
        process_prompt(key, state)

    save_state(state)

    if verbose:
        logger.info("Готово.")
    _d("process_once: done")


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
    process_once(verbose=True)
