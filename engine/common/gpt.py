# FILE: engine/common/gpt.py  (обновлено — 2025-12-20)
# Смысл: единая точка общения с OpenAI (Responses API) + dev IPC-cache через common/cache (daemon).
# Итоговая фиксация:
# - temperature УДАЛЁН полностью (GPT-5 / Responses API его не поддерживает)
# - user_id — стандартный параметр
# - service_tier задаётся при инициализации клиента
# - всё игнорируемое ранее — выпилено
# - кеш: ключ = (model, instructions, input), service_tier сознательно НЕ участвует
# - web автоматически включается для 5.1 / 5.2

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from engine.common.cache.client import memo as cache_memo

# ---------- CONSTANTS & TYPES ----------

TierName = Literal["nano", "mini", "maxi", "maxi-51"]  # legacy only
ServiceTier = Literal["flex", "standard", "priority"]

DEFAULT_TTL_SEC = 7 * 24 * 60 * 60

TIER_TO_MODEL: dict[str, str] = {
    "nano": "gpt-5-nano",
    "mini": "gpt-5-mini",
    "maxi": "gpt-5.1",
    "maxi-51": "gpt-5.1",
}

MODEL_ALIASES: dict[str, str] = {
    "nano": "gpt-5-nano",
    "mini": "gpt-5-mini",
    "maxi": "gpt-5.1",
    "maxi-51": "gpt-5.1",
}

MODEL_WEB_TOOL: dict[str, str] = {
    "gpt-5.1": "web_search",
    "gpt-5.2": "web_search",
    "maxi": "web_search",
    "maxi-51": "web_search",
}

ALLOWED_SERVICE_TIERS: set[str] = {"flex", "standard", "priority"}
OPENAI_ENV_VAR = "OPENAI_API_KEY"

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_BASE_DIR = PROJECT_ROOT / "logs" / "gpt"
LOG_BASE_DIR.mkdir(parents=True, exist_ok=True)


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


# ---------- INTERNAL UTILS ----------


def _require_api_key() -> str:
    api_key = os.environ.get(OPENAI_ENV_VAR, "").strip()
    if not api_key:
        raise GptConfigError(f"OpenAI API key not found. Please set env var {OPENAI_ENV_VAR!r}.")
    return api_key


def _optional_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _make_log_dir(service_tier: ServiceTier, model_name: str) -> Path:
    d = LOG_BASE_DIR / service_tier / model_name
    d.mkdir(parents=True, exist_ok=True)
    return d


def _short_hash(s: str, n_hex: int = 16) -> str:
    h = hashlib.sha1(s.encode("utf-8", errors="replace")).hexdigest()
    return h[: max(1, int(n_hex))]


def _log_call(
    *,
    model_name: str,
    service_tier: ServiceTier,
    user_id: str,
    instructions: str,
    input_text: str,
    response: Optional[Dict[str, Any]],
    usage: Optional[GptUsage],
    status: str,
    error_message: Optional[str] = None,
) -> None:
    log_dir = _make_log_dir(service_tier, model_name)
    now = datetime.now()
    log_file = log_dir / f"{now.date().isoformat()}.log"

    lines: List[str] = []
    lines.append(
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
        f"MODEL={model_name} SERVICE_TIER={service_tier} USER={user_id} "
        f"TOKENS(in={getattr(usage, 'prompt_tokens', None)},out={getattr(usage, 'completion_tokens', None)},"
        f"total={getattr(usage, 'total_tokens', None)}) "
        f"STATUS={status}"
    )

    if error_message:
        lines.append(f"ERROR: {error_message}")

    lines.append(f"INSTRUCTIONS: {instructions}")
    lines.append(f"INPUT: {input_text}")
    if response is not None:
        lines.append(f"RESPONSE: {json.dumps(response, ensure_ascii=False)}")
    lines.append("-" * 80)

    with log_file.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ---------- OPENAI CLIENT ----------

_OPENAI_CLIENT: Any = None


def _get_openai_client() -> Any:
    global _OPENAI_CLIENT
    if _OPENAI_CLIENT is None:
        from openai import OpenAI

        _OPENAI_CLIENT = OpenAI(api_key=_require_api_key())
    return _OPENAI_CLIENT


def _build_payload(
    *,
    model_name: str,
    instructions: str,
    input_text: str,
    service_tier: ServiceTier,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "model": model_name,
        "input": input_text,
        "service_tier": service_tier,
        "store": False,
    }

    if instructions:
        payload["instructions"] = instructions
        payload["prompt_cache_key"] = _short_hash(instructions, 16)

    web_tool = MODEL_WEB_TOOL.get(model_name)
    if web_tool:
        payload["tools"] = [{"type": web_tool}]
        payload["tool_choice"] = "auto"

    return payload


# ---------- MAIN CLIENT ----------


class GPTClient:
    def __init__(
        self,
        *,
        service_tier: ServiceTier = "flex",
    ) -> None:
        if service_tier not in ALLOWED_SERVICE_TIERS:
            raise GptValidationError(f"Unsupported service_tier {service_tier!r}.")
        self._service_tier: ServiceTier = service_tier
        _require_api_key()

    def ask(
        self,
        *,
        model: Optional[str] = None,
        instructions: Optional[str] = None,
        input: Optional[str] = None,
        override: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
        user_id: Any = "SET USER URGENTLY",

        #--- Legacy ---#
        tier: TierName = "nano",
        system: str = "",
        user: str = "",
        **kwargs,
    ) -> GptResponse:
        user_id_str = _optional_str(user_id) or "SET USER URGENTLY"

        if override is not None:
            if not isinstance(override, dict):
                raise GptValidationError("override must be a dict.")

            try:
                client = _get_openai_client()
                resp = client.responses.create(**override)
                raw = resp.model_dump()
                content = str(getattr(resp, "output_text", "") or "")
            except Exception as exc:
                _log_call(
                    model_name=str(override.get("model", "-")),
                    service_tier=self._service_tier,
                    user_id=user_id_str,
                    instructions=str(override.get("instructions", "")),
                    input_text=str(override.get("input", "")),
                    response=None,
                    usage=None,
                    status="error",
                    error_message=str(exc),
                )
                raise

            usage = self._extract_usage(raw)

            _log_call(
                model_name=str(override.get("model", "-")),
                service_tier=self._service_tier,
                user_id=user_id_str,
                instructions=str(override.get("instructions", "")),
                input_text=str(override.get("input", "")),
                response=raw,
                usage=usage,
                status="ok",
            )
            return GptResponse(content=content, raw=raw, usage=usage)

        instr = _optional_str(instructions) if instructions is not None else _optional_str(system)
        inp = _optional_str(input) if input is not None else _optional_str(user)

        model_in = _optional_str(model)
        if model_in:
            model_name = MODEL_ALIASES.get(model_in, model_in)
        else:
            model_name = TIER_TO_MODEL.get(str(tier), "gpt-5-nano")

        if not instr and not inp:
            _log_call(
                model_name=model_name,
                service_tier=self._service_tier,
                user_id=user_id_str,
                instructions="",
                input_text="",
                response=None,
                usage=None,
                status="empty request",
            )
            return GptResponse(content="", raw={}, usage=GptUsage())

        query = (model_name, instr, inp)

        def _fn(q: tuple[str, str, str]) -> str:
            m, ins, inpt = q
            payload = _build_payload(
                model_name=m,
                instructions=ins,
                input_text=inpt,
                service_tier=self._service_tier,
            )
            client = _get_openai_client()
            resp = client.responses.create(**payload)
            return str(getattr(resp, "output_text", "") or "")

        try:
            if use_cache:
                content = cache_memo(
                    query,
                    _fn,
                    ttl=DEFAULT_TTL_SEC,
                    version="gpt.content.v1",
                    update=False,
                )
                raw = {"cached_via": "daemon-memo"}
            else:
                content = _fn(query)
                raw = {"cached_via": "no-cache"}
        except Exception as exc:
            _log_call(
                model_name=model_name,
                service_tier=self._service_tier,
                user_id=user_id_str,
                instructions=instr,
                input_text=inp,
                response=None,
                usage=None,
                status="error",
                error_message=str(exc),
            )
            raise

        usage = GptUsage()

        _log_call(
            model_name=model_name,
            service_tier=self._service_tier,
            user_id=user_id_str,
            instructions=instr,
            input_text=inp,
            response=raw,
            usage=usage,
            status="ok",
        )

        return GptResponse(content=str(content or ""), raw=raw, usage=usage)

    @staticmethod
    def _extract_usage(raw: Dict[str, Any]) -> GptUsage:
        usage = raw.get("usage") or {}
        return GptUsage(
            prompt_tokens=usage.get("prompt_tokens") or usage.get("input_tokens"),
            completion_tokens=usage.get("completion_tokens") or usage.get("output_tokens"),
            total_tokens=usage.get("total_tokens"),
        )
