# FILE: engine/common/gpt.py  (обновлено — 2026-01-06)
# PURPOSE: Единая точка общения с OpenAI (Responses API) + IPC-cache через common/cache (daemon).
#          Изменение: добавлен режим debug-логирования (debug=True — полный лог как раньше; debug=False — только billing header).

from __future__ import annotations

import hashlib
import inspect
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

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
    # legacy (не обязательно, но пусть остаётся)
    "maxi": "web_search",
    "maxi-51": "web_search",
}

ALLOWED_SERVICE_TIERS: set[str] = {"flex", "standard", "priority"}
OPENAI_ENV_VAR = "OPENAI_API_KEY"

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "gpt.requests.log"


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


def _short_hash(s: str, n_hex: int = 16) -> str:
    h = hashlib.sha1(s.encode("utf-8", errors="replace")).hexdigest()
    return h[: max(1, int(n_hex))]


def _pretty_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return str(obj)


def _write_log_block(*lines: str) -> None:
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def _detect_call_origin() -> str:
    """
    Определяем источник: web или engine.
    Практическое правило: если в стеке есть '/web/' — считаем web, иначе engine.
    """
    try:
        for fr in inspect.stack()[2:]:
            p = (fr.filename or "").replace("\\", "/")
            if "/web/" in p:
                return "web"
        return "engine"
    except Exception:
        return "engine"


def _guard_tier_for_engine(service_tier: ServiceTier) -> None:
    """
    Если вызов из engine — запрещаем standard и priority (как ты просил).
    """
    if service_tier not in ("standard", "priority"):
        return
    if _detect_call_origin() == "engine":
        raise GptValidationError(
            f"service_tier={service_tier!r} запрещён для вызовов из engine."
        )


def _log_platform_call(
    *,
    now: datetime,
    model_name: str,
    service_tier: ServiceTier,
    user_id: str,
    instructions: str,
    input_text: str,
    usage: Optional[GptUsage],
    output_text: str,
    raw: Any,
    status: str,
    error_message: Optional[str] = None,
    debug: bool = True,
) -> None:
    head = (
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
        f"STATUS={status} MODEL={model_name} SERVICE_TIER={service_tier} USER={user_id} "
        f"TOKENS(in={getattr(usage, 'prompt_tokens', None)},"
        f"out={getattr(usage, 'completion_tokens', None)},"
        f"total={getattr(usage, 'total_tokens', None)})"
    )

    # debug=False: billing лог — только факт запроса/ответа + error (если есть)
    if not debug:
        lines: List[str] = [head]
        if error_message:
            lines.append(f"ERROR: {error_message}")
        lines.append("-" * 120)
        _write_log_block(*lines)
        return

    # debug=True: как раньше — полный лог
    lines = [head]
    if error_message:
        lines.append(f"ERROR: {error_message}")

    lines.append("INSTRUCTIONS:")
    lines.append(instructions or "")
    lines.append("INPUT:")
    lines.append(input_text or "")

    if isinstance(raw, (dict, list)):
        lines.append("RESPONSE_JSON:")
        lines.append(_pretty_json(raw))
    else:
        lines.append("OUTPUT_TEXT:")
        lines.append(output_text or "")

    lines.append("-" * 120)
    _write_log_block(*lines)


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
    def __init__(self, debug: bool = True) -> None:
        # Объект без состояния tier. Ключ проверим по месту вызова.
        _require_api_key()
        self.debug = bool(debug)

    def ask(
        self,
        *,
        model: Optional[str] = None,
        instructions: Optional[str] = None,
        input: Optional[str] = None,
        override: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
        user_id: Any = "SET USER URGENTLY",
        service_tier: Optional[ServiceTier] = None,

        # --- Legacy --- #
        tier: TierName = "nano",
        system: str = "",
        user: str = "",
        **kwargs,
    ) -> GptResponse:
        _require_api_key()

        user_id_str = _optional_str(user_id) or "SET USER URGENTLY"
        effective_tier: ServiceTier = service_tier or "flex"
        if effective_tier not in ALLOWED_SERVICE_TIERS:
            raise GptValidationError(f"Unsupported service_tier {effective_tier!r}.")

        # override: bypass всех guard'ов (как ты и хотел)
        if override is not None:
            if not isinstance(override, dict):
                raise GptValidationError("override must be a dict.")
            try:
                t0 = time.monotonic()
                client = _get_openai_client()
                resp = client.responses.create(**override)
                elapsed_ms = int((time.monotonic() - t0) * 1000)

                raw = resp.model_dump()
                content = str(getattr(resp, "output_text", "") or "")
                usage = self._extract_usage(raw)

                log_tier = (
                    str(override.get("service_tier")).strip()
                    if str(override.get("service_tier", "")).strip()
                    else effective_tier
                )
                _log_platform_call(
                    now=datetime.now(),
                    model_name=str(override.get("model", "-")),
                    service_tier=log_tier if log_tier in ALLOWED_SERVICE_TIERS else effective_tier,
                    user_id=user_id_str,
                    instructions=str(override.get("instructions", "")),
                    input_text=str(override.get("input", "")),
                    usage=usage,
                    output_text=content,
                    raw=raw,
                    status=f"ok ({elapsed_ms} ms)",
                    debug=self.debug,
                )
                return GptResponse(content=content, raw=raw, usage=usage)
            except Exception as exc:
                log_tier = (
                    str(override.get("service_tier")).strip()
                    if str(override.get("service_tier", "")).strip()
                    else effective_tier
                )
                _log_platform_call(
                    now=datetime.now(),
                    model_name=str(override.get("model", "-")),
                    service_tier=log_tier if log_tier in ALLOWED_SERVICE_TIERS else effective_tier,
                    user_id=user_id_str,
                    instructions=str(override.get("instructions", "")),
                    input_text=str(override.get("input", "")),
                    usage=None,
                    output_text="",
                    raw={},
                    status="error",
                    error_message=str(exc),
                    debug=self.debug,
                )
                raise

        instr = _optional_str(instructions) if instructions is not None else _optional_str(system)
        inp = _optional_str(input) if input is not None else _optional_str(user)

        model_in = _optional_str(model)
        if model_in:
            model_name = MODEL_ALIASES.get(model_in, model_in)
        else:
            model_name = TIER_TO_MODEL.get(str(tier), "gpt-5-nano")

        if not instr and not inp:
            # Пустой запрос — платформу не зовём, и лог не пишем.
            return GptResponse(content="", raw={}, usage=GptUsage())

        query: Tuple[str, str, str] = (model_name, instr, inp)

        last_raw: Optional[Dict[str, Any]] = None
        last_usage: Optional[GptUsage] = None

        def _fn(q: Tuple[str, str, str]) -> str:
            nonlocal last_raw, last_usage

            m, ins, inpt = q
            payload = _build_payload(
                model_name=m,
                instructions=ins,
                input_text=inpt,
                service_tier=effective_tier,
            )

            # Guard: только для НЕ-override вызовов, прямо перед платформой
            _guard_tier_for_engine(effective_tier)

            api_tier = "default" if effective_tier == "standard" else effective_tier
            payload["service_tier"] = api_tier

            try:
                t0 = time.monotonic()
                client = _get_openai_client()
                resp = client.responses.create(**payload)
                elapsed_ms = int((time.monotonic() - t0) * 1000)

                raw = resp.model_dump()
                usage = self._extract_usage(raw)
                out_text = str(getattr(resp, "output_text", "") or "")

                last_raw = raw
                last_usage = usage

                _log_platform_call(
                    now=datetime.now(),
                    model_name=m,
                    service_tier=effective_tier,
                    user_id=user_id_str,
                    instructions=ins,
                    input_text=inpt,
                    usage=usage,
                    output_text=out_text,
                    raw=raw,
                    status=f"ok ({elapsed_ms} ms)",
                    debug=self.debug,
                )
                return out_text
            except Exception as exc:
                _log_platform_call(
                    now=datetime.now(),
                    model_name=m,
                    service_tier=effective_tier,
                    user_id=user_id_str,
                    instructions=ins,
                    input_text=inpt,
                    usage=None,
                    output_text="",
                    raw={},
                    status="error",
                    error_message=str(exc),
                    debug=self.debug,
                )
                raise

        # cache-hit не вызывает _fn → логов не будет.
        if use_cache:
            content = cache_memo(
                query,
                _fn,
                ttl=DEFAULT_TTL_SEC,
                version="gpt.content.v1",
                update=False,
            )
            if last_raw is None:
                # cache-hit: ничего не логируем
                return GptResponse(content=str(content or ""), raw={"cached": True}, usage=GptUsage())
            return GptResponse(content=str(content or ""), raw=last_raw or {}, usage=last_usage or GptUsage())

        content = _fn(query)
        return GptResponse(content=str(content or ""), raw=last_raw or {}, usage=last_usage or GptUsage())

    @staticmethod
    def _extract_usage(raw: Dict[str, Any]) -> GptUsage:
        usage = raw.get("usage") or {}
        return GptUsage(
            prompt_tokens=usage.get("prompt_tokens") or usage.get("input_tokens"),
            completion_tokens=usage.get("completion_tokens") or usage.get("output_tokens"),
            total_tokens=usage.get("total_tokens"),
        )
