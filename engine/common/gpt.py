# FILE: engine/common/gpt.py  (новое — 2025-12-11)

"""
GPT connector for mailer-app.

Единственная точка общения с OpenAI:

- Три tier'а:
    nano -> gpt-5-nano
    mini -> gpt-5-mini
    maxi -> gpt-5.1
- ВСЕ вызовы идут через Responses API (НЕ chat.completions).
- Формат всегда простой: system (instructions) + user (input) → один ответ.
- Для tier='maxi' при with_web=True включается web_search_preview.
- Вся история (session_id) хранится только локально в объекте и
  НИКОГДА не отправляется в модель.

Добавлено:
- Файловый кеш по хешу запроса:
    ключ = hash(model_name, tier_name, with_web, system, user)
- Кеш хранится в PROJECT_ROOT/cache/gpt/<model>/<префикс>/<hash>.json
  с полем created_at для последующей чистки по экспирации внешним скриптом.
"""

from __future__ import annotations

import json
import os
import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict, Literal

# ---------- CONSTANTS & TYPES ----------

TierName = Literal["nano", "mini", "maxi"]
ServiceTier = Literal["flex", "standard", "priority"]

ALLOWED_TIERS: dict[TierName, str] = {
    "nano": "gpt-5-nano",
    "mini": "gpt-5-mini",
    "maxi": "gpt-5.1",
}

ALLOWED_SERVICE_TIERS: set[str] = {"flex", "standard", "priority"}

OPENAI_ENV_VAR = "OPENAI_API_KEY"

# engine/common/gpt.py → common → engine → mailer-app
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Base log directory: mailer-app/logs/gpt/
LOG_BASE_DIR = PROJECT_ROOT / "logs" / "gpt"
LOG_BASE_DIR.mkdir(parents=True, exist_ok=True)

# Base cache directory: mailer-app/cache/gpt/
CACHE_BASE_DIR = LOG_BASE_DIR / "cache"
CACHE_BASE_DIR.mkdir(parents=True, exist_ok=True)


class GptConfigError(RuntimeError):
    """Configuration error: missing API key or similar."""


class GptValidationError(ValueError):
    """Invalid input for GPT call (tier, workspace, user, prompt, etc.)."""


@dataclass
class GptUsage:
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    total_tokens: Optional[int] = None


@dataclass
class GptResponse:
    content: str
    raw: Dict[str, Any]
    usage: GptUsage


class Message(TypedDict):
    role: Literal["system", "user", "assistant"]
    content: str


# ---------- INTERNAL UTILS ----------


def _require_api_key() -> str:
    api_key = os.environ.get(OPENAI_ENV_VAR, "").strip()
    if not api_key:
        raise GptConfigError(
            f"OpenAI API key not found. Please set env var {OPENAI_ENV_VAR!r}."
        )
    return api_key


def _validate_not_empty(label: str, value: Any) -> str:
    if value is None:
        raise GptValidationError(f"{label} must not be None.")
    s = str(value).strip()
    if not s:
        raise GptValidationError(f"{label} must not be empty.")
    return s


def _make_log_dir(service_tier: ServiceTier, model_name: str) -> Path:
    d = LOG_BASE_DIR / service_tier / model_name
    d.mkdir(parents=True, exist_ok=True)
    return d


def _make_cache_path(model_name: str, cache_key: str) -> Path:
    """
    Возвращает путь к файлу кеша для данного model_name + cache_key.
    Кеш раскладывается по подпапкам по первым двум символам хеша,
    чтобы не было тысячи файлов в одной директории.
    """
    subdir = CACHE_BASE_DIR / model_name / cache_key[:2]
    subdir.mkdir(parents=True, exist_ok=True)
    return subdir / f"{cache_key}.json"


def _compute_cache_key(
    *,
    model_name: str,
    tier_name: TierName,
    with_web: Optional[bool],
    service_tier: ServiceTier,      # оставлено в сигнатуре для совместимости, но НЕ в ключе
    workspace_id: str,              # НЕ в ключе
    user_id: str,                   # НЕ в ключе
    system: str,
    user: str,
    endpoint: str,                  # НЕ в ключе
    extra_payload: Dict[str, Any],  # НЕ в ключе
) -> str:
    """
    Формирует хеш-ключ кеша.

    В хеш входят ТОЛЬКО:
        - model_name
        - tier_name
        - with_web
        - system (instructions)
        - user (input)

    Всё остальное НЕ влияет на кеш:
        - workspace_id, user_id
        - endpoint
        - extra_payload
        - service_tier
    """

    payload_for_hash = {
        "model": model_name,
        "tier": tier_name,
        "with_web": with_web,
        "system": system,
        "user": user,
    }

    serialized = json.dumps(
        payload_for_hash,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _log_call(
    *,
    model_name: str,
    tier_name: TierName,
    with_web: Optional[bool],
    service_tier: ServiceTier,
    workspace_id: Any,
    user_id: Any,
    system: str,
    user: str,
    endpoint: str,
    response: Dict[str, Any],
    usage: GptUsage,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    log_dir = _make_log_dir(service_tier, model_name)
    now = datetime.now()
    log_file = log_dir / f"{now.date().isoformat()}.log"

    ws = str(workspace_id) if workspace_id is not None else "-"
    user_str = str(user_id) if user_id is not None else "-"

    lines: List[str] = []

    header = (
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
        f"MODEL={model_name} TIER={tier_name} "
        f"SERVICE_TIER={service_tier} WITH_WEB={with_web} "
        f"WORKSPACE={ws} USER={user_str} "
        f"TOKENS(in={usage.prompt_tokens},out={usage.completion_tokens},"
        f"total={usage.total_tokens}) "
        f"STATUS={status} ENDPOINT={endpoint}"
    )
    lines.append(header)

    if error_message:
        lines.append(f"ERROR: {error_message}")

    lines.append(f"SYSTEM: {system}")
    lines.append(f"USER:   {user}")

    try:
        resp_str = json.dumps(response, ensure_ascii=False)
    except Exception:
        resp_str = f"<unserializable response type {type(response)!r}>"
    lines.append(f"RESPONSE: {resp_str}")

    lines.append("-" * 80)

    with log_file.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ---------- MAIN CLIENT ----------


class GPTClient:
    """
    Stateless-клиент GPT для проекта.

    ВАЖНО:
    - Внешний интерфейс: один вызов → один ответ (Q→A), без чата.
    - Внутри — только Responses API, без chat.completions.
    - session_id / use_history используются ТОЛЬКО для внутреннего хранения
      истории (self._sessions_history) и не влияют на запросы к модели.
    - Есть файловый кеш по хешу запроса (см. _compute_cache_key). Кеш можно
      отключить per-call (use_cache=False) или чистить отдельным скриптом
      по полю created_at в файлах кеша.
    """

    N_CALLS_BEFORE_RESET = 7

    def __init__(self) -> None:
        self._api_key: str = _require_api_key()
        self._client: Any = None
        self._calls_since_reset: int = 0

        # session_id -> list[Message] (чисто для себя, не в модель)
        self._sessions_history: dict[str, List[Message]] = {}

    # ----- PUBLIC API -----

    def ask(
        self,
        *,
        tier: TierName,
        workspace_id: Any,
        user_id: Any,
        system: str,
        user: str,
        with_web: Optional[bool] = None,
        service_tier: ServiceTier = "flex",
        endpoint: str = "generic",
        extra_payload: Optional[Dict[str, Any]] = None,
        session_id: Optional[str] = None,
        use_history: bool = False,
        use_cache: bool = True,
    ) -> GptResponse:
        """
        Один запрос к GPT (всегда одиночный ход, без контекста).

        - tier: 'nano' | 'mini' | 'maxi'
        - with_web:
            * для 'maxi' ОБЯЗАТЕЛЕН (True/False)
            * для 'nano'/'mini' НЕЛЬЗЯ ставить True (raise)
        - system → instructions
        - user   → input
        - use_cache:
            * True (по умолчанию): перед API-вызовом будет пробоваться файловый кеш;
              успешный ответ также будет записан в кеш.
            * False: кеш игнорируется и не пишется.
        """

        # --- validate tier & model ---
        if tier not in ALLOWED_TIERS:
            raise GptValidationError(
                f"Unsupported tier {tier!r}. Allowed: {list(ALLOWED_TIERS.keys())}."
            )
        model_name = ALLOWED_TIERS[tier]

        # --- validate service tier ---
        if service_tier not in ALLOWED_SERVICE_TIERS:
            raise GptValidationError(
                f"Unsupported service_tier {service_tier!r}. "
                f"Allowed: {sorted(ALLOWED_SERVICE_TIERS)}."
            )
        service_tier = service_tier  # type: ServiceTier

        # --- validate with_web rules ---
        if tier in ("nano", "mini") and with_web is True:
            raise GptValidationError(
                "Web search is only supported for tier='maxi'. "
                "Tier='nano' or 'mini' cannot use with_web=True."
            )

        if tier == "maxi" and with_web is None:
            raise GptValidationError(
                "Tier 'maxi' requires explicit with_web=True/False."
            )

        # --- validate workspace/user ---
        workspace_id_str = _validate_not_empty("workspace_id", workspace_id)
        user_id_str = _validate_not_empty("user_id", user_id)

        # --- validate prompts ---
        system_clean = _validate_not_empty("system", system)
        user_clean = _validate_not_empty("user", user)

        # --- history (только запоминаем, НО не шлём в модель) ---
        if use_history and session_id:
            history = self._sessions_history.setdefault(session_id, [])
            # логичнее: system → user
            history.append({"role": "system", "content": system_clean})
            history.append({"role": "user", "content": user_clean})

        extra_payload = extra_payload or {}

        # --- cache: попытка cache hit до любого обращения к API ---
        cache_key = _compute_cache_key(
            model_name=model_name,
            tier_name=tier,
            with_web=with_web,
            service_tier=service_tier,
            workspace_id=workspace_id_str,
            user_id=user_id_str,
            system=system_clean,
            user=user_clean,
            endpoint=endpoint,
            extra_payload=extra_payload,
        )
        cache_path = _make_cache_path(model_name, cache_key)

        if use_cache and cache_path.exists():
            try:
                with cache_path.open("r", encoding="utf-8") as f:
                    cached = json.load(f)

                raw_response = cached.get("response", {}) or {}
                usage = self._extract_usage(raw_response)
                status = "cache"
                error_message: Optional[str] = None

                if use_history and session_id:
                    content_for_history = self._extract_content(raw_response)
                    self._sessions_history.setdefault(session_id, []).append(
                        {"role": "assistant", "content": content_for_history}
                    )

                _log_call(
                    model_name=model_name,
                    tier_name=tier,
                    with_web=with_web,
                    service_tier=service_tier,
                    workspace_id=workspace_id_str,
                    user_id=user_id_str,
                    system=system_clean,
                    user=user_clean,
                    endpoint=endpoint,
                    response=raw_response,
                    usage=usage,
                    status=status,
                    error_message=error_message,
                )

                content = self._extract_content(raw_response)
                return GptResponse(
                    content=content,
                    raw=raw_response,
                    usage=usage,
                )
            except Exception:
                # Любая проблема с кешем → просто игнорируем и идём в реальный API.
                pass

        # --- perform request & log ---
        try:
            self._ensure_client()

            raw_response = self._perform_responses_request(
                model_name=model_name,
                system=system_clean,
                user=user_clean,
                with_web=with_web,
                extra_payload=extra_payload,
            )
            usage = self._extract_usage(raw_response)
            status = "ok"
            error_message: Optional[str] = None

            if use_history and session_id:
                # сохраним assistant-ответ в локальной истории
                content_for_history = self._extract_content(raw_response)
                self._sessions_history.setdefault(session_id, []).append(
                    {"role": "assistant", "content": content_for_history}
                )

            # --- успешный ответ можно положить в кеш ---
            if use_cache:
                try:
                    cache_record = {
                        "created_at": datetime.utcnow().isoformat() + "Z",
                        "request_fingerprint": cache_key,
                        "request_meta": {
                            "model": model_name,
                            "tier": tier,
                            "with_web": with_web,
                            "service_tier": service_tier,
                            "workspace_id": workspace_id_str,
                            "user_id": user_id_str,
                            "endpoint": endpoint,
                            "system": system_clean,
                            "user": user_clean,
                            "extra_payload": extra_payload,
                        },
                        "response": raw_response,
                    }
                    with cache_path.open("w", encoding="utf-8") as f:
                        json.dump(cache_record, f, ensure_ascii=False)
                except Exception:
                    # Ошибки кеша не должны ломать основной поток
                    pass

        except Exception as exc:
            raw_response = {
                "error": {
                    "type": type(exc).__name__,
                    "message": str(exc),
                }
            }
            usage = GptUsage()
            status = "error"
            error_message = str(exc)

        _log_call(
            model_name=model_name,
            tier_name=tier,
            with_web=with_web,
            service_tier=service_tier,
            workspace_id=workspace_id_str,
            user_id=user_id_str,
            system=system_clean,
            user=user_clean,
            endpoint=endpoint,
            response=raw_response,
            usage=usage,
            status=status,
            error_message=error_message,
        )

        content = self._extract_content(raw_response)

        return GptResponse(
            content=content,
            raw=raw_response,
            usage=usage,
        )

    # ----- INTERNAL HELPERS -----

    def _ensure_client(self) -> None:
        """Создаёт/ресетит OpenAI клиент каждые N_CALLS_BEFORE_RESET вызовов."""
        from openai import OpenAI  # локальный импорт

        if self._client is None or self._calls_since_reset >= self.N_CALLS_BEFORE_RESET:
            self._client = OpenAI(api_key=self._api_key)
            self._calls_since_reset = 0

    def _perform_responses_request(
        self,
        *,
        model_name: str,
        system: str,
        user: str,
        with_web: Optional[bool],
        extra_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Один вызов Responses API.

        - system → instructions
        - user   → input
        - web_search_preview включается, если with_web=True и tier='maxi'
        """

        # клиент уже создан в _ensure_client
        self._calls_since_reset += 1

        # Базовый payload
        payload: Dict[str, Any] = {
            "model": model_name,
            "instructions": system,
            "input": user,
        }

        # Поддержка web_search_preview
        tools = extra_payload.pop("tools", None)
        tool_choice = extra_payload.pop("tool_choice", None)

        if with_web:
            # если явно не передали tools в extra_payload — включаем web_search_preview
            if tools is None:
                tools = [{"type": "web_search_preview"}]
            if tool_choice is None:
                tool_choice = "auto"

        if tools is not None:
            payload["tools"] = tools
        if tool_choice is not None:
            payload["tool_choice"] = tool_choice

        payload.update(extra_payload)

        response = self._client.responses.create(**payload)

        # достанем text для удобства
        try:
            content_text = response.output_text
        except Exception:
            # fallback: через сырые данные
            try:
                tmp = response.model_dump()
            except AttributeError:
                tmp = json.loads(response.json())
            content_text = ""
            try:
                out_items = tmp.get("output") or []
                if out_items:
                    cont = out_items[0].get("content") or []
                    if cont:
                        content_text = cont[0].get("text", "") or ""
            except Exception:
                content_text = ""

        # raw dict
        try:
            raw = response.model_dump()
        except AttributeError:
            raw = json.loads(response.json())

        # эмулируем choices → message → content,
        # чтобы _extract_content работал одинаково везде
        raw.setdefault(
            "choices",
            [
                {
                    "message": {
                        "content": content_text,
                    }
                }
            ],
        )

        return raw

    # --- Parsing helpers ---

    @staticmethod
    def _extract_usage(raw: Dict[str, Any]) -> GptUsage:
        usage = raw.get("usage") or {}

        prompt_tokens = usage.get("prompt_tokens") or usage.get("input_tokens")
        completion_tokens = usage.get("completion_tokens") or usage.get(
            "output_tokens"
        )
        total_tokens = usage.get("total_tokens")

        return GptUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        )

    @staticmethod
    def _extract_content(raw: Dict[str, Any]) -> str:
        """
        Универсальный извлекатель текста:
        - сначала chat-подобный путь (choices[0].message.content),
        - если нет choices, то пытаемся достать из output[..].content[..].text,
        - если есть error — вернём строку с ошибкой.
        """
        try:
            choices = raw.get("choices") or []
            if choices:
                msg = choices[0].get("message") or {}
                content = msg.get("content") or ""
                return str(content)

            if "output" in raw:
                out_items = raw.get("output") or []
                if out_items:
                    first = out_items[0]
                    cont = first.get("content") or []
                    if cont:
                        text = cont[0].get("text") or ""
                        return str(text)

            if "error" in raw:
                return f"ERROR: {raw['error']}"

            return ""
        except Exception:
            return ""


__all__ = [
    "GPTClient",
    "GptConfigError",
    "GptValidationError",
    "GptUsage",
    "GptResponse",
]
