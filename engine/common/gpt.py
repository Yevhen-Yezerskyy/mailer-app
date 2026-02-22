# FILE: engine/common/gpt.py  (обновлено — 2026-02-22)
# PURPOSE: Единая точка общения с OpenAI (Responses API) + IPC-cache через common/cache (daemon).
#          Логи: host stream (все вызовы, включая cache), host errors (все ошибки), system short (только реальные API-вызовы).

from __future__ import annotations

import hashlib
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
    "gpt-5.1": "web_search_preview",
    "gpt-5-mini": "web_search_preview",
    "gpt-5-nano": "web_search_preview",
}

OPENAI_ENV_VAR = "OPENAI_API_KEY"

ALLOWED_SERVICE_TIERS: set[str] = {"flex", "standard", "priority"}

HOST_STREAM_FILE = Path("/host-logs/gpt/stream.log")
HOST_ERRORS_FILE = Path("/host-logs/gpt/errors.log")
SYS_REQUESTS_FILE = Path("/serenity-logs/gpt/requests.log")


class GptConfigError(RuntimeError):
    pass


class GptValidationError(ValueError):
    pass


class GptSoftError(RuntimeError):
    """Soft GPT failure that must NOT crash Django views (no cache write)."""

    def __init__(self, user_message: str, *, error_message: str = "") -> None:
        super().__init__(error_message or user_message)
        self.user_message = user_message
        self.error_message = error_message or user_message


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


def _soft_error_message(exc: Exception) -> str:
    # Keep message stable (English) for UI.
    # Do not leak internal request IDs to end users by default.
    s = str(exc) or exc.__class__.__name__
    if "server_error" in s or "Error code: 5" in s or "Error code: 500" in s:
        return "OpenAI internal server error. Try again later or change the request."
    if "Rate limit" in s or "429" in s:
        return "OpenAI rate limit reached. Try again later or change the request."
    if "timeout" in s.lower() or "timed out" in s.lower():
        return "OpenAI request timeout. Try again later or change the request."
    return "OpenAI request failed. Try again later or change the request."


def _is_openai_related_exception(exc: Exception) -> bool:
    mod = getattr(exc.__class__, "__module__", "") or ""
    if mod.startswith("openai"):
        return True
    s = str(exc) or ""
    return "help.openai.com" in s or "request ID req_" in s or "server_error" in s


def _require_api_key() -> str:
    api_key = os.environ.get(OPENAI_ENV_VAR, "").strip()
    if not api_key:
        raise GptConfigError(f"OpenAI API key not found. Please set env var {OPENAI_ENV_VAR!r}.")
    return api_key


def _optional_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    try:
        return str(value).strip()
    except Exception:
        return ""


def _short_hash(text: str, length: int = 16) -> str:
    if not text:
        return ""
    h = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return h[:length]


def _write_log_block(path: Path, *lines: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for line in lines:
            f.write(line)
            if not line.endswith("\n"):
                f.write("\n")


def _guard_tier_for_engine(service_tier: ServiceTier) -> None:
    """
    Guard только для корректного service_tier (может быть расширен позже).
    """
    # Сейчас просто оставляем как есть (guard может быть расширен).
    if service_tier not in ALLOWED_SERVICE_TIERS:
        raise GptValidationError(f"Unsupported service_tier {service_tier!r}.")


# ---------- LOGGING ----------


def _log_header(
    *,
    now: datetime,
    status: str,
    model_name: str,
    service_tier: ServiceTier,
    user_id: str,
    usage: Optional[GptUsage],
    cache_hit: bool,
    real_request: bool,
) -> str:
    return (
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
        f"STATUS={status} CACHE={'yes' if cache_hit else 'no'} REAL={'yes' if real_request else 'no'} "
        f"MODEL={model_name} SERVICE_TIER={service_tier} USER={user_id} "
        f"TOKENS(in={getattr(usage, 'prompt_tokens', None)},"
        f"out={getattr(usage, 'completion_tokens', None)},"
        f"total={getattr(usage, 'total_tokens', None)})"
    )


def _log_host_stream(
    *,
    now: datetime,
    status: str,
    model_name: str,
    service_tier: ServiceTier,
    user_id: str,
    instructions: str,
    input_text: str,
    output_text: str,
    usage: Optional[GptUsage],
    cache_hit: bool,
    real_request: bool,
    error_message: Optional[str] = None,
) -> None:
    lines: List[str] = [
        _log_header(
            now=now,
            status=status,
            model_name=model_name,
            service_tier=service_tier,
            user_id=user_id,
            usage=usage,
            cache_hit=cache_hit,
            real_request=real_request,
        ),
        "INSTRUCTIONS:",
        instructions or "",
        "INPUT:",
        input_text or "",
        "OUTPUT:",
        output_text or "",
    ]
    if error_message:
        lines.extend(["ERROR:", error_message])
    lines.append("-" * 120)
    _write_log_block(HOST_STREAM_FILE, *lines)


def _log_host_error(
    *,
    now: datetime,
    status: str,
    model_name: str,
    service_tier: ServiceTier,
    user_id: str,
    instructions: str,
    input_text: str,
    output_text: str,
    usage: Optional[GptUsage],
    cache_hit: bool,
    real_request: bool,
    error_message: str,
) -> None:
    lines: List[str] = [
        _log_header(
            now=now,
            status=status,
            model_name=model_name,
            service_tier=service_tier,
            user_id=user_id,
            usage=usage,
            cache_hit=cache_hit,
            real_request=real_request,
        ),
        "ERROR:",
        error_message or "",
        "INSTRUCTIONS:",
        instructions or "",
        "INPUT:",
        input_text or "",
        "OUTPUT:",
        output_text or "",
        "-" * 120,
    ]
    _write_log_block(HOST_ERRORS_FILE, *lines)


def _log_system_request(
    *,
    now: datetime,
    status: str,
    model_name: str,
    service_tier: ServiceTier,
    user_id: str,
    usage: Optional[GptUsage],
) -> None:
    line = (
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
        f"STATUS={status} MODEL={model_name} SERVICE_TIER={service_tier} USER={user_id} "
        f"TOKENS(in={getattr(usage, 'prompt_tokens', None)},"
        f"out={getattr(usage, 'completion_tokens', None)},"
        f"total={getattr(usage, 'total_tokens', None)})"
    )
    _write_log_block(SYS_REQUESTS_FILE, line)


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
    def __init__(self, debug: bool = False) -> None:
        # debug оставлен только для обратной совместимости вызовов.
        _require_api_key()
        _ = debug

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
                now = datetime.now()
                tier_for_log = log_tier if log_tier in ALLOWED_SERVICE_TIERS else effective_tier
                model_for_log = str(override.get("model", "-"))
                instructions_for_log = str(override.get("instructions", ""))
                input_for_log = str(override.get("input", ""))
                status_for_log = f"ok ({elapsed_ms} ms)"

                _log_host_stream(
                    now=now,
                    status=status_for_log,
                    model_name=model_for_log,
                    service_tier=tier_for_log,
                    user_id=user_id_str,
                    instructions=instructions_for_log,
                    input_text=input_for_log,
                    output_text=content,
                    usage=usage,
                    cache_hit=False,
                    real_request=True,
                )
                _log_system_request(
                    now=now,
                    status=status_for_log,
                    model_name=model_for_log,
                    service_tier=tier_for_log,
                    user_id=user_id_str,
                    usage=usage,
                )
                return GptResponse(content=content, raw=raw, usage=usage)
            except Exception as exc:
                log_tier = (
                    str(override.get("service_tier")).strip()
                    if str(override.get("service_tier", "")).strip()
                    else effective_tier
                )
                now = datetime.now()
                tier_for_log = log_tier if log_tier in ALLOWED_SERVICE_TIERS else effective_tier
                model_for_log = str(override.get("model", "-"))
                instructions_for_log = str(override.get("instructions", ""))
                input_for_log = str(override.get("input", ""))
                error_message = str(exc)
                output_for_log = _soft_error_message(exc) if _is_openai_related_exception(exc) else ""

                _log_host_stream(
                    now=now,
                    status="error",
                    model_name=model_for_log,
                    service_tier=tier_for_log,
                    user_id=user_id_str,
                    instructions=instructions_for_log,
                    input_text=input_for_log,
                    output_text=output_for_log,
                    usage=None,
                    cache_hit=False,
                    real_request=True,
                    error_message=error_message,
                )
                _log_host_error(
                    now=now,
                    status="error",
                    model_name=model_for_log,
                    service_tier=tier_for_log,
                    user_id=user_id_str,
                    instructions=instructions_for_log,
                    input_text=input_for_log,
                    output_text=output_for_log,
                    usage=None,
                    cache_hit=False,
                    real_request=True,
                    error_message=error_message,
                )
                _log_system_request(
                    now=now,
                    status="error",
                    model_name=model_for_log,
                    service_tier=tier_for_log,
                    user_id=user_id_str,
                    usage=None,
                )
                if _is_openai_related_exception(exc):
                    return GptResponse(content=_soft_error_message(exc), raw={"soft_error": True}, usage=GptUsage())
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

                now = datetime.now()
                status_for_log = f"ok ({elapsed_ms} ms)"
                _log_host_stream(
                    now=now,
                    status=status_for_log,
                    model_name=m,
                    service_tier=effective_tier,
                    user_id=user_id_str,
                    instructions=ins,
                    input_text=inpt,
                    usage=usage,
                    output_text=out_text,
                    cache_hit=False,
                    real_request=True,
                )
                _log_system_request(
                    now=now,
                    status=status_for_log,
                    model_name=m,
                    service_tier=effective_tier,
                    user_id=user_id_str,
                    usage=usage,
                )
                return out_text
            except Exception as exc:
                now = datetime.now()
                error_message = str(exc)
                output_for_log = _soft_error_message(exc) if _is_openai_related_exception(exc) else ""
                _log_host_stream(
                    now=now,
                    status="error",
                    model_name=m,
                    service_tier=effective_tier,
                    user_id=user_id_str,
                    instructions=ins,
                    input_text=inpt,
                    usage=None,
                    output_text=output_for_log,
                    cache_hit=False,
                    real_request=True,
                    error_message=error_message,
                )
                _log_host_error(
                    now=now,
                    status="error",
                    model_name=m,
                    service_tier=effective_tier,
                    user_id=user_id_str,
                    instructions=ins,
                    input_text=inpt,
                    output_text=output_for_log,
                    usage=None,
                    cache_hit=False,
                    real_request=True,
                    error_message=error_message,
                )
                _log_system_request(
                    now=now,
                    status="error",
                    model_name=m,
                    service_tier=effective_tier,
                    user_id=user_id_str,
                    usage=None,
                )
                if _is_openai_related_exception(exc):
                    raise GptSoftError(_soft_error_message(exc), error_message=str(exc))
                raise

        try:
            if use_cache:
                content = cache_memo(
                    query,
                    _fn,
                    ttl=DEFAULT_TTL_SEC,
                    version="gpt.content.v1",
                    update=False,
                )
                if last_raw is None:
                    _log_host_stream(
                        now=datetime.now(),
                        status="cache",
                        model_name=model_name,
                        service_tier=effective_tier,
                        user_id=user_id_str,
                        instructions=instr,
                        input_text=inp,
                        output_text=str(content or ""),
                        usage=None,
                        cache_hit=True,
                        real_request=False,
                    )
                    return GptResponse(content=str(content or ""), raw={"cached": True}, usage=GptUsage())
                return GptResponse(content=str(content or ""), raw=last_raw or {}, usage=last_usage or GptUsage())

            content = _fn(query)
            return GptResponse(content=str(content or ""), raw=last_raw or {}, usage=last_usage or GptUsage())

        except GptSoftError as exc:
            return GptResponse(content=exc.user_message, raw={"soft_error": True}, usage=GptUsage())

    @staticmethod
    def _extract_usage(raw: Dict[str, Any]) -> GptUsage:
        usage = raw.get("usage") or {}
        return GptUsage(
            prompt_tokens=usage.get("prompt_tokens") or usage.get("input_tokens"),
            completion_tokens=usage.get("completion_tokens") or usage.get("output_tokens"),
            total_tokens=usage.get("total_tokens"),
        )
