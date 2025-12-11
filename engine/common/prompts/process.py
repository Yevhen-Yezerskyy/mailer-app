# FILE: engine/common/prompts/process.py  (новое) 2025-12-11
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

TODO:
-----
- Добавить checksum текста в state, чтобы ловить изменения даже при одинаковом mtime.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from engine.common.gpt import GPTClient

try:
    from worker.tick import BaseProcess
except Exception:
    BaseProcess = object  # type: ignore[misc]

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
ENG_DIR = BASE_DIR / "eng"
STATE_PATH = BASE_DIR / "state.json"

GPT_WORKSPACE_ID = "prompts"
GPT_USER_ID = "process"


# ============================================================================
# state.json
# ============================================================================

def load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            logger.error("state.json поврежден — создаю пустой.")
    return {}


def save_state(state: Dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


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
        return ""

    source_file = BASE_DIR / f"{key}.txt"
    if not source_file.exists():
        return ""

    # ---- читаем оригинал ----
    try:
        source_text = source_file.read_text(encoding="utf-8")
    except Exception:
        source_text = ""

    entry = state.get(key, {})
    source_mtime = entry.get("source_mtime", 0)

    eng_file = ENG_DIR / f"{key}.txt"
    eng_mtime = entry.get("eng_mtime")

    # ---- выбираем актуальный текст ----
    if eng_file.exists() and isinstance(eng_mtime, (int, float)) and eng_mtime >= source_mtime:
        try:
            prompt_text = eng_file.read_text(encoding="utf-8")
        except Exception:
            prompt_text = source_text
    else:
        prompt_text = source_text

    # ---- язык ответа ----
    if lang == "same":
        # НИЧЕГО не добавляем — модель сама понимает язык ответа
        return prompt_text

    # ---- приклеиваем инструкцию ----
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
        # нет системного промпта — не падаем, просто плохая оценка
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
        return name, rate
    except Exception as e:
        logger.error("Nano eval error: %s", e)
        return "", 100


def translate_to_en(text: str) -> str:
    """
    Перевод промпта на английский (maxi / GPT-5.1).
    Выполняется только при debug == "".
    """
    system_prompt = get_prompt("prompt_translate")
    if not system_prompt:
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
        return (resp.content or "").strip()
    except Exception as e:
        logger.error("EN translation error: %s", e)
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
        # промпт удалён
        state.pop(key, None)
        if eng_file.exists():
            eng_file.unlink()
        return

    source_mtime = source_file.stat().st_mtime
    entry = state.get(key) or {}

    prev_mtime = entry.get("source_mtime")
    if isinstance(prev_mtime, (int, float)) and source_mtime <= prev_mtime:
        # файл НЕ менялся — ничего не делаем
        return

    # читаем исходный текст (для nano и перевода)
    source_text = source_file.read_text(encoding="utf-8")

    debug = entry.get("debug", "YES")  # по умолчанию ВСЕГДА что-то (обычно "YES")

    # --- 1) nano-оценка (ВСЕГДА при изменении файла) ---
    name, rate = nano_eval(source_text)
    now_iso = datetime.utcnow().isoformat(timespec="seconds")

    entry["name"] = name
    entry["rate"] = rate
    entry["source_mtime"] = source_mtime
    entry["last_eval"] = now_iso
    entry.setdefault("debug", debug)

    # --- 2) EN-логика зависит только от debug ---
    if debug == "":
        # перевод на английский и nano по EN
        translated = translate_to_en(source_text)

        ENG_DIR.mkdir(exist_ok=True)
        eng_file.write_text(translated, encoding="utf-8")
        eng_mtime = eng_file.stat().st_mtime

        name_en, rate_en = nano_eval(translated)

        entry["name_en"] = name_en
        entry["rate_en"] = rate_en
        entry["eng_mtime"] = eng_mtime
    else:
        # debug режим — EN не нужен, удаляем если есть
        if eng_file.exists():
            eng_file.unlink()
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

    if verbose:
        logger.info("Найдено промптов: %s", keys)

    for key in keys:
        if verbose:
            logger.info("Обрабатываю промпт: %s", key)
        process_prompt(key, state)

    save_state(state)

    if verbose:
        logger.info("Готово.")


# ============================================================================
# Интеграция с worker.tick
# ============================================================================

class PromptSyncProcess(BaseProcess):  # type: ignore[misc]
    name = "prompt-sync"
    interval_seconds = 300

    def tick(self) -> None:
        process_once(verbose=False)


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
