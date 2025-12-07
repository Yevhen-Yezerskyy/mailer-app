# FILE: engine/common/gpt.py

"""
GPT connector for mailer-app.

This module is the ONLY place that should talk to OpenAI GPT models.
It also handles validation, logging, and (optionally) history (by session_id).

PRICES (TEXT TOKENS, PER 1M TOKENS, FLEX TIER)
------------------------------------------------
MODEL        INPUT       CACHED INPUT     OUTPUT
gpt-5.1      $0.625      $0.0625          $5.00
gpt-5        $0.625      $0.0625          $5.00
gpt-5-mini   $0.125      $0.0125          $1.00
gpt-5-nano   $0.025      $0.0025          $0.20
o3           $1.00       $0.25            $4.00
o4-mini      $0.55       $0.138           $2.20

NOTES:
- We use ONLY these three models here: gpt-5-nano, gpt-5-mini, gpt-5.1.
- Default service tier is FLEX (other tiers: standard, priority) — мы их логируем,
  но реальный выбор Tier сейчас настраивается на аккаунте.
- API key MUST be provided via env var OPENAI_API_KEY. If missing, we raise.
"""

from __future__ import annotations

import json
import os
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
    Stateless (по смыслу) клиент GPT для всего проекта.

    ВАЖНО:
    - Этот объект НЕ привязан к конкретной модели, воркспейсу или пользователю.
    - Все важные параметры (tier, with_web, workspace, user, service_tier)
      передаются на каждый вызов ask().
    - Внутри есть только:
        - проверка конфигурации,
        - транспорт-клиент (OpenAI),
        - счётчик для auto-reset каждые N_CALLS_BEFORE_RESET,
        - опциональная история по session_id.
    """

    N_CALLS_BEFORE_RESET = 7

    def __init__(self) -> None:
        self._api_key: str = _require_api_key()
        self._client: Any = None
        self._calls_since_reset: int = 0

        # session_id -> list[Message]
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
    ) -> GptResponse:
        """
        Выполнить один запрос к GPT.

        Требования:
        - tier: 'nano' | 'mini' | 'maxi'
        - service_tier: 'flex' | 'standard' | 'priority' (по умолчанию 'flex')
        - workspace_id, user_id: обязательны и не пустые
        - system, user: строки, не пустые
        - with_web:
            * для 'maxi' ОБЯЗАТЕЛЕН (True/False)
            * для 'nano'/'mini' НЕЛЬЗЯ ставить True (raise)
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
        service_tier = service_tier  # type: ServiceTier  # for type-checkers

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

        # --- history (optional, by session_id) ---
        messages: List[Message] = []
        history_ref: Optional[List[Message]] = None

        if use_history and session_id:
            history_ref = self._sessions_history.setdefault(session_id, [])
            messages.extend(history_ref)

        messages.append({"role": "system", "content": system_clean})
        messages.append({"role": "user", "content": user_clean})

        # --- build payload ---
        payload: Dict[str, Any] = {
            "model": model_name,
            "messages": messages,
        }

        # Здесь позже можно будет аккуратно прокинуть параметры
        # для service_tier и web-search. Пока мы только валидируем
        # и логируем with_web, а объект OpenAI вызываем напрямую.
        #
        # Пример (когда решишь использовать Responses API с tools):
        #
        # if tier == "maxi":
        #     tools = []
        #     if with_web:
        #         tools.append({"type": "web_search"})
        #     payload["tools"] = tools
        #
        if extra_payload:
            payload.update(extra_payload)

        # --- perform request & log ---
        try:
            self._ensure_client()
            raw_response = self._perform_request(payload)
            usage = self._extract_usage(raw_response)
            status = "ok"
            error_message: Optional[str] = None

            # update history if needed
            if use_history and session_id and history_ref is not None:
                assistant_content = self._extract_content(raw_response)
                history_ref.append(
                    {"role": "assistant", "content": assistant_content}
                )
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

        # log call
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
        """
        Следит за тем, чтобы транспорт-клиент существовал и
        пересоздаётся каждые N_CALLS_BEFORE_RESET вызовов.
        """
        from openai import OpenAI  # локальный импорт, чтобы не падать без либы

        if self._client is None or self._calls_since_reset >= self.N_CALLS_BEFORE_RESET:
            self._client = OpenAI(api_key=self._api_key)
            self._calls_since_reset = 0

    def _perform_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Реальный запрос к OpenAI через Chat Completions API.

        Ожидает в payload как минимум:
        - model: str
        - messages: list[{"role": "...", "content": "..."}]
        """
        self._calls_since_reset += 1

        model = payload.get("model")
        messages = payload.get("messages") or []
        if not model:
            raise GptValidationError(
                "Missing 'model' in payload for _perform_request()."
            )
        if not messages:
            raise GptValidationError(
                "Missing 'messages' in payload for _perform_request()."
            )

        # TODO: сюда позже можно будет аккуратно прокинуть:
        # - reasoning_effort (none/low/medium/high/...)
        # - tools/web_search и т. д. для tier='maxi'
        # Пока — простой chat.completions.
        response = self._client.chat.completions.create(
            model=model,
            messages=messages,
        )

        # Превращаем объект SDK в обычный dict
        try:
            raw = response.model_dump()
        except AttributeError:
            raw = json.loads(response.json())

        return raw

    @staticmethod
    def _extract_usage(raw: Dict[str, Any]) -> GptUsage:
        usage = raw.get("usage") or {}
        return GptUsage(
            prompt_tokens=usage.get("prompt_tokens"),
            completion_tokens=usage.get("completion_tokens"),
            total_tokens=usage.get("total_tokens"),
        )

    @staticmethod
    def _extract_content(raw: Dict[str, Any]) -> str:
        try:
            choices = raw.get("choices") or []
            if not choices:
                if "error" in raw:
                    return f"ERROR: {raw['error']}"
                return ""
            msg = choices[0].get("message") or {}
            content = msg.get("content") or ""
            return str(content)
        except Exception:
            return ""


__all__ = [
    "GPTClient",
    "GptConfigError",
    "GptValidationError",
    "GptUsage",
    "GptResponse",
]
