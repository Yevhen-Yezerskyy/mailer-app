# FILE: engine/common/gpt.py  (обновлено — 2025-12-17)
# Смысл: единая точка общения с OpenAI (Responses API) + файловый кеш + логирование
# + fallback для maxi (5.2 → 5.1) + debug-force (всегда nano)
# Добавлено: tier 'maxi-51' — всегда gpt-5.1, без fallback

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, TypedDict

# ---------- CONSTANTS & TYPES ----------

TierName = Literal["nano", "mini", "maxi", "maxi-51"]
ServiceTier = Literal["flex", "standard", "priority"]

ALLOWED_TIERS: dict[TierName, str] = {
    "nano": "gpt-5-nano",
    "mini": "gpt-5-mini",
    "maxi": "gpt-5.2",     # основной жир
    "maxi-51": "gpt-5.1",  # фиксированный 5.1
}

# Фоллбек ТОЛЬКО для 'maxi'
FALLBACK_MAXI_MODEL = "gpt-5.1"

ALLOWED_SERVICE_TIERS: set[str] = {"flex", "standard", "priority"}

OPENAI_ENV_VAR = "OPENAI_API_KEY"

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

LOG_BASE_DIR = PROJECT_ROOT / "logs" / "gpt"
LOG_BASE_DIR.mkdir(parents=True, exist_ok=True)

CACHE_BASE_DIR = PROJECT_ROOT / "cache" / "gpt"
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
    lines.append(
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
        f"MODEL={model_name} TIER={tier_name} "
        f"SERVICE_TIER={service_tier} WITH_WEB={with_web} "
        f"WORKSPACE={ws} USER={user_str} "
        f"TOKENS(in={usage.prompt_tokens},out={usage.completion_tokens},"
        f"total={usage.total_tokens}) "
        f"STATUS={status} ENDPOINT={endpoint}"
    )

    if error_message:
        lines.append(f"ERROR: {error_message}")

    lines.append(f"SYSTEM: {system}")
    lines.append(f"USER: {user}")
    lines.append(f"RESPONSE: {json.dumps(response, ensure_ascii=False)}")
    lines.append("-" * 80)

    with log_file.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ---------- MAIN CLIENT ----------


class GPTClient:
    N_CALLS_BEFORE_RESET = 7

    def __init__(self, *, debug: bool = False) -> None:
        self._api_key: str = _require_api_key()
        self._client: Any = None
        self._calls_since_reset: int = 0
        self._debug: bool = bool(debug)
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

        # --- DEBUG FORCE ---
        if self._debug:
            tier = "nano"
            with_web = False
            use_cache = False

        if tier not in ALLOWED_TIERS:
            raise GptValidationError(f"Unsupported tier {tier!r}.")

        model_name = ALLOWED_TIERS[tier]

        cache_model_name = model_name
        if tier == "maxi":
            cache_model_name = "gpt-5-maxi"
        elif tier == "maxi-51":
            cache_model_name = "gpt-5-maxi-51"

        if tier in ("nano", "mini") and with_web is True:
            raise GptValidationError("Web search allowed only for maxi tiers.")

        if tier in ("maxi", "maxi-51") and with_web is None:
            raise GptValidationError(f"Tier '{tier}' requires with_web=True/False.")

        workspace_id_str = _validate_not_empty("workspace_id", workspace_id)
        user_id_str = _validate_not_empty("user_id", user_id)
        system_clean = _validate_not_empty("system", system)
        user_clean = _validate_not_empty("user", user)

        extra_payload = extra_payload or {}

        cache_key = _compute_cache_key(
            model_name=cache_model_name,
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
        cache_path = _make_cache_path(cache_model_name, cache_key)

        if use_cache and cache_path.exists():
            with cache_path.open("r", encoding="utf-8") as f:
                cached = json.load(f)
            raw = cached["response"]
            usage = self._extract_usage(raw)
            return GptResponse(
                content=self._extract_content(raw),
                raw=raw,
                usage=usage,
            )

        self._ensure_client()

        used_model = model_name
        try:
            raw = self._perform_responses_request(
                model_name=model_name,
                system=system_clean,
                user=user_clean,
                with_web=with_web,
                extra_payload=extra_payload,
            )
        except Exception as exc:
            if tier == "maxi":
                raw = self._perform_responses_request(
                    model_name=FALLBACK_MAXI_MODEL,
                    system=system_clean,
                    user=user_clean,
                    with_web=with_web,
                    extra_payload=extra_payload,
                )
                used_model = FALLBACK_MAXI_MODEL
            else:
                raise

        usage = self._extract_usage(raw)

        if use_cache:
            with cache_path.open("w", encoding="utf-8") as f:
                json.dump(
                    {
                        "created_at": datetime.utcnow().isoformat() + "Z",
                        "response": raw,
                    },
                    f,
                    ensure_ascii=False,
                )

        _log_call(
            model_name=used_model,
            tier_name=tier,
            with_web=with_web,
            service_tier=service_tier,
            workspace_id=workspace_id_str,
            user_id=user_id_str,
            system=system_clean,
            user=user_clean,
            endpoint=endpoint,
            response=raw,
            usage=usage,
            status="ok",
        )

        return GptResponse(
            content=self._extract_content(raw),
            raw=raw,
            usage=usage,
        )

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

        tools = extra_payload.pop("tools", None)
        tool_choice = extra_payload.pop("tool_choice", None)

        if with_web:
            tools = tools or [{"type": "web_search_preview"}]
            tool_choice = tool_choice or "auto"

        if tools is not None:
            payload["tools"] = tools
        if tool_choice is not None:
            payload["tool_choice"] = tool_choice

        payload.update(extra_payload)

        response = self._client.responses.create(**payload)

        raw = response.model_dump()
        raw.setdefault(
            "choices",
            [{"message": {"content": response.output_text}}],
        )
        return raw

    @staticmethod
    def _extract_usage(raw: Dict[str, Any]) -> GptUsage:
        usage = raw.get("usage") or {}
        return GptUsage(
            prompt_tokens=usage.get("prompt_tokens") or usage.get("input_tokens"),
            completion_tokens=usage.get("completion_tokens") or usage.get("output_tokens"),
            total_tokens=usage.get("total_tokens"),
        )

    @staticmethod
    def _extract_content(raw: Dict[str, Any]) -> str:
        try:
            return str(raw["choices"][0]["message"]["content"])
        except Exception:
            return ""
