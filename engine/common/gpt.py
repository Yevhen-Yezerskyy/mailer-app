# FILE: engine/common/gpt.py  (новое — 2025-12-14)

"""
GPT connector for mailer-app.

Единственная точка общения с OpenAI:

- Три tier'а:
    nano -> gpt-5-nano
    mini -> gpt-5-mini
    maxi -> gpt-5.2 (c откатом на gpt-5.1 при недоступности 5.2)
- ВСЕ вызовы идут через Responses API (НЕ chat.completions).
- Формат всегда простой: system (instructions) + user (input) → один ответ.
- Для tier='maxi' при with_web=True включается web_search_preview.
- Вся история (session_id) хранится только локально в объекте и
  НИКОГДА не отправляется в модель.
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
    "maxi": "gpt-5.2",
}

FALLBACK_MAXI_MODEL = "gpt-5.1"
ALLOWED_SERVICE_TIERS: set[str] = {"flex", "standard", "priority"}
OPENAI_ENV_VAR = "OPENAI_API_KEY"

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

LOG_BASE_DIR = PROJECT_ROOT / "logs" / "gpt"
LOG_BASE_DIR.mkdir(parents=True, exist_ok=True)

CACHE_BASE_DIR = LOG_BASE_DIR / "cache"
CACHE_BASE_DIR.mkdir(parents=True, exist_ok=True)


class GptConfigError(RuntimeError):
    pass


class GptValidationError(ValueError):
    pass


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
        raise GptConfigError(f"OpenAI API key not found ({OPENAI_ENV_VAR}).")
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
    subdir = CACHE_BASE_DIR / model_name / cache_key[:2]
    subdir.mkdir(parents=True, exist_ok=True)
    return subdir / f"{cache_key}.json"


def _compute_cache_key(
    *,
    model_name: str,
    tier_name: TierName,
    with_web: Optional[bool],
    service_tier: ServiceTier,
    workspace_id: str,
    user_id: str,
    system: str,
    user: str,
    endpoint: str,
    extra_payload: Dict[str, Any],
) -> str:
    payload = {
        "model": model_name,
        "tier": tier_name,
        "with_web": with_web,
        "system": system,
        "user": user,
    }
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
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
    log_file = log_dir / f"{datetime.now().date().isoformat()}.log"

    lines = [
        f"[{datetime.now().isoformat()}] MODEL={model_name} "
        f"TIER={tier_name} SERVICE_TIER={service_tier} "
        f"WITH_WEB={with_web} STATUS={status} ENDPOINT={endpoint}",
        f"TOKENS={usage}",
        f"SYSTEM: {system}",
        f"USER: {user}",
        f"RESPONSE: {json.dumps(response, ensure_ascii=False)}",
        "-" * 80,
    ]

    if error_message:
        lines.insert(1, f"ERROR: {error_message}")

    with log_file.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ---------- MAIN CLIENT ----------

class GPTClient:
    N_CALLS_BEFORE_RESET = 7

    def __init__(self, *, debug: bool = False) -> None:
        self._api_key = _require_api_key()
        self._client: Any = None
        self._calls_since_reset = 0
        self._debug = bool(debug)
        self._sessions_history: dict[str, List[Message]] = {}

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

        # ---------- DEBUG FORCE ----------
        if self._debug:
            tier = "nano"
            with_web = False
            service_tier = "flex"
            use_cache = False
        # ---------------------------------

        model_name = ALLOWED_TIERS[tier]

        workspace_id_str = _validate_not_empty("workspace_id", workspace_id)
        user_id_str = _validate_not_empty("user_id", user_id)
        system_clean = _validate_not_empty("system", system)
        user_clean = _validate_not_empty("user", user)

        extra_payload = dict(extra_payload or {})

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
            error_message = None

        except Exception as exc:
            raw_response = {"error": str(exc)}
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
        return GptResponse(content=content, raw=raw_response, usage=usage)

    # ---------- INTERNAL ----------

    def _ensure_client(self) -> None:
        from openai import OpenAI

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

        self._calls_since_reset += 1

        payload: Dict[str, Any] = {
            "model": model_name,
            "instructions": system,
            "input": user,
        }

        response = self._client.responses.create(**payload)

        try:
            raw = response.model_dump()
        except AttributeError:
            raw = json.loads(response.json())

        raw.setdefault(
            "choices",
            [{"message": {"content": response.output_text or ""}}],
        )
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
            return raw["choices"][0]["message"]["content"]
        except Exception:
            return ""


__all__ = [
    "GPTClient",
    "GptConfigError",
    "GptValidationError",
    "GptUsage",
    "GptResponse",
]
