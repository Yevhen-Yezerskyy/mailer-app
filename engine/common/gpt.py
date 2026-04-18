# FILE: engine/common/gpt.py  (обновлено — 2026-02-22)
# PURPOSE: Единая точка общения с OpenAI (Responses API) + IPC-cache через common/cache (daemon).
#          Логи: host stream (все вызовы, включая cache), host errors (все ошибки), system short (только реальные API-вызовы).

from __future__ import annotations

import hashlib
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from engine.common.cache.client import CLIENT, memo as cache_memo
from engine.common.logs import log as host_log

# ---------- CONSTANTS & TYPES ----------

MIN_LOCAL_CACHE_DAYS = 7
MAX_LOCAL_CACHE_DAYS = 14

MODEL_PRESETS: dict[str, str] = {
    "standard": "gpt-5.4",
    "mini": "gpt-5.4-mini",
    "nano": "gpt-5.4-nano",
}

DEFAULT_MODEL_ALIAS = "standard"

OPENAI_ENV_VAR = "OPENAI_API_KEY"

GPT_LOG_FOLDER = "gpt"
HOST_STREAM_FILE = "stream.log"
HOST_ERRORS_FILE = "errors.log"
SYS_REQUESTS_FILE = "requests.log"

STATUS_OK = "OK"
STATUS_ERROR_TMP = "ERROR_TMP"
STATUS_ERROR_INT = "ERROR_INT"
STATUS_ERROR = "ERROR"

REQUEST_GATE_KEY = "gpt:request_gate"
REQUEST_GATE_TTL_SEC = 0.5
REQUEST_GATE_BACKOFF_SEC = 0.25
REQUEST_GATE_ATTEMPTS = 20

TMP_ERROR_BLOCK_KEY = "gpt:tmp_error_block"
TMP_ERROR_BLOCK_TTL_SEC = 5 * 60


class GptConfigError(RuntimeError):
    pass


class GptValidationError(ValueError):
    pass


class GptSoftError(RuntimeError):
    """Soft GPT failure that must NOT crash Django views (no cache write)."""

    def __init__(self, user_message: str, *, status: str, error_message: str = "") -> None:
        super().__init__(error_message or user_message)
        self.user_message = user_message
        self.status = status
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
    status: str = STATUS_OK


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


def _status_code_from_exception(exc: Exception) -> Optional[int]:
    code = getattr(exc, "status_code", None)
    if isinstance(code, int):
        return code

    s = str(exc) or ""
    m = re.search(r"(?:Error code|status code)\s*:\s*(\d{3})", s, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _is_tmp_http_status(status_code: Optional[int]) -> bool:
    return status_code == 429 or (isinstance(status_code, int) and status_code >= 500)


def _error_status_for_exception(exc: Exception) -> str:
    status_code = _status_code_from_exception(exc)
    if _is_tmp_http_status(status_code):
        return STATUS_ERROR_TMP
    if _is_openai_related_exception(exc):
        return STATUS_ERROR
    return STATUS_ERROR_INT


def _tmp_block_message() -> str:
    return "OpenAI temporary unavailable. Try again later."


def _internal_error_message() -> str:
    return "Internal GPT connector error. Try again later."


def _is_tmp_error_block_active() -> bool:
    state = CLIENT.lock_status(TMP_ERROR_BLOCK_KEY)
    return bool(state and bool(state.get("held")))


def _set_tmp_error_block() -> None:
    owner = f"gpt-tmp-block:{os.getpid()}:{int(time.time())}"
    try:
        CLIENT.lock_try(TMP_ERROR_BLOCK_KEY, ttl_sec=float(TMP_ERROR_BLOCK_TTL_SEC), owner=owner)
    except Exception:
        return


def _acquire_request_gate() -> bool:
    owner = f"gpt-gate:{os.getpid()}:{int(time.time() * 1000)}"
    for _ in range(int(REQUEST_GATE_ATTEMPTS)):
        info = CLIENT.lock_try(REQUEST_GATE_KEY, ttl_sec=float(REQUEST_GATE_TTL_SEC), owner=owner)
        if info and bool(info.get("acquired")):
            return True
        time.sleep(float(REQUEST_GATE_BACKOFF_SEC))
    return False


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


def _resolve_model_name(model: Optional[str]) -> str:
    model_raw = _optional_str(model)
    if not model_raw:
        return MODEL_PRESETS[DEFAULT_MODEL_ALIAS]
    model_key = model_raw.casefold()
    preset = MODEL_PRESETS.get(model_key)
    if preset:
        return preset
    return model_raw


def _random_local_cache_ttl_sec() -> int:
    days = random.randint(MIN_LOCAL_CACHE_DAYS, MAX_LOCAL_CACHE_DAYS)
    return days * 24 * 60 * 60


def _short_hash(text: str, length: int = 16) -> str:
    if not text:
        return ""
    h = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return h[:length]


def _write_log_block(log_file: str, *lines: str) -> None:
    host_log(log_file, folder=GPT_LOG_FOLDER, message="\n".join(lines))


# ---------- LOGGING ----------


def _log_header(
    *,
    now: datetime,
    status: str,
    model_name: str,
    request_tier: str,
    response_tier: str,
    user_id: str,
    usage: Optional[GptUsage],
    cache_hit: bool,
    real_request: bool,
) -> str:
    return (
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
        f"STATUS={status} CACHE={'yes' if cache_hit else 'no'} REAL={'yes' if real_request else 'no'} "
        f"MODEL={model_name} REQUEST_TIER={request_tier} RESPONSE_TIER={response_tier} USER={user_id} "
        f"TOKENS(in={getattr(usage, 'prompt_tokens', None)},"
        f"out={getattr(usage, 'completion_tokens', None)},"
        f"total={getattr(usage, 'total_tokens', None)})"
    )


def _log_host_stream(
    *,
    now: datetime,
    status: str,
    model_name: str,
    request_tier: str,
    response_tier: str,
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
            request_tier=request_tier,
            response_tier=response_tier,
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
    request_tier: str,
    response_tier: str,
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
            request_tier=request_tier,
            response_tier=response_tier,
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
    request_tier: str,
    response_tier: str,
    user_id: str,
    usage: Optional[GptUsage],
) -> None:
    line = (
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
        f"STATUS={status} MODEL={model_name} REQUEST_TIER={request_tier} RESPONSE_TIER={response_tier} USER={user_id} "
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
    service_tier: str,
    web_search: bool = False,
    use_gpt_cache: bool = True,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "model": model_name,
        "input": input_text,
        "service_tier": service_tier,
        "store": False,
    }

    if instructions:
        payload["instructions"] = instructions
        if use_gpt_cache:
            payload["prompt_cache_key"] = _short_hash(instructions, 16)

    _apply_web_search(payload, web_search=web_search)

    return payload


def _apply_web_search(
    payload: Dict[str, Any],
    *,
    web_search: bool,
) -> None:
    if not web_search:
        return

    tools_value = payload.get("tools")
    tools_list = tools_value if isinstance(tools_value, list) else []

    def _is_web_tool(tool: Any) -> bool:
        return isinstance(tool, dict) and str(tool.get("type") or "").strip() == "web_search"

    if not any(_is_web_tool(tool) for tool in tools_list):
        payload["tools"] = [*tools_list, {"type": "web_search"}]
    elif isinstance(tools_value, list):
        payload["tools"] = tools_list
    payload.setdefault("tool_choice", "auto")


# ---------- MAIN CLIENT ----------


class GPTClient:
    def __init__(self, debug: bool = False) -> None:
        # debug оставлен только для обратной совместимости вызовов.
        _ = debug

    @staticmethod
    def _forced_error_response(
        *,
        model_name: str,
        service_tier: str,
        user_id_str: str,
        instructions: str,
        input_text: str,
    ) -> GptResponse:
        message = _tmp_block_message()
        status = f"{STATUS_ERROR_TMP} (forced_always)"
        now = datetime.now()

        _log_host_stream(
            now=now,
            status=status,
            model_name=model_name,
            request_tier=service_tier,
            response_tier="-",
            user_id=user_id_str,
            instructions=instructions,
            input_text=input_text,
            output_text=message,
            usage=None,
            cache_hit=False,
            real_request=False,
            error_message="forced GPT failure for UI check",
        )
        _log_host_error(
            now=now,
            status=status,
            model_name=model_name,
            request_tier=service_tier,
            response_tier="-",
            user_id=user_id_str,
            instructions=instructions,
            input_text=input_text,
            output_text=message,
            usage=None,
            cache_hit=False,
            real_request=False,
            error_message="forced GPT failure for UI check",
        )
        _log_system_request(
            now=now,
            status=status,
            model_name=model_name,
            request_tier=service_tier,
            response_tier="-",
            user_id=user_id_str,
            usage=None,
        )

        return GptResponse(
            content=message,
            raw={"soft_error": True, "forced_error": True},
            usage=GptUsage(),
            status=STATUS_ERROR_TMP,
        )

    def _run_payload(
        self,
        *,
        payload: Dict[str, Any],
        user_id_str: str,
        default_tier: str,
    ) -> GptResponse:
        model_for_log = _optional_str(payload.get("model")) or "-"
        instructions_for_log = str(payload.get("instructions", ""))
        input_for_log = str(payload.get("input", ""))
        request_tier_for_log = _optional_str(payload.get("service_tier")) or default_tier

        if _is_tmp_error_block_active():
            message = _tmp_block_message()
            _log_host_stream(
                now=datetime.now(),
                status=f"{STATUS_ERROR_TMP} (global_lock_active)",
                model_name=model_for_log,
                request_tier=request_tier_for_log,
                response_tier="-",
                user_id=user_id_str,
                instructions=instructions_for_log,
                input_text=input_for_log,
                output_text=message,
                usage=None,
                cache_hit=False,
                real_request=False,
            )
            return GptResponse(content=message, raw={"soft_error": True, "blocked": True}, usage=GptUsage(), status=STATUS_ERROR_TMP)

        for api_attempt in range(2):
            if not _acquire_request_gate():
                message = _internal_error_message()
                _log_host_stream(
                    now=datetime.now(),
                    status=f"{STATUS_ERROR_INT} (gate_timeout)",
                    model_name=model_for_log,
                    request_tier=request_tier_for_log,
                    response_tier="-",
                    user_id=user_id_str,
                    instructions=instructions_for_log,
                    input_text=input_for_log,
                    output_text=message,
                    usage=None,
                    cache_hit=False,
                    real_request=False,
                )
                _log_host_error(
                    now=datetime.now(),
                    status=f"{STATUS_ERROR_INT} (gate_timeout)",
                    model_name=model_for_log,
                    request_tier=request_tier_for_log,
                    response_tier="-",
                    user_id=user_id_str,
                    instructions=instructions_for_log,
                    input_text=input_for_log,
                    output_text=message,
                    usage=None,
                    cache_hit=False,
                    real_request=False,
                    error_message="gpt request gate lock timeout",
                )
                return GptResponse(
                    content=message,
                    raw={"soft_error": True, "gate_timeout": True},
                    usage=GptUsage(),
                    status=STATUS_ERROR_INT,
                )

            try:
                t0 = time.monotonic()
                client = _get_openai_client()
                resp = client.responses.create(**payload)
                elapsed_ms = int((time.monotonic() - t0) * 1000)

                raw = resp.model_dump()
                content = str(getattr(resp, "output_text", "") or "")
                usage = self._extract_usage(raw)
                status_for_log = f"{STATUS_OK} ({elapsed_ms} ms)"

                _log_host_stream(
                    now=datetime.now(),
                    status=status_for_log,
                    model_name=model_for_log,
                    request_tier=request_tier_for_log,
                    response_tier=_optional_str(raw.get("service_tier")) or "-",
                    user_id=user_id_str,
                    instructions=instructions_for_log,
                    input_text=input_for_log,
                    output_text=content,
                    usage=usage,
                    cache_hit=False,
                    real_request=True,
                )
                _log_system_request(
                    now=datetime.now(),
                    status=status_for_log,
                    model_name=model_for_log,
                    request_tier=request_tier_for_log,
                    response_tier=_optional_str(raw.get("service_tier")) or "-",
                    user_id=user_id_str,
                    usage=usage,
                )
                return GptResponse(content=content, raw=raw, usage=usage, status=STATUS_OK)
            except Exception as exc:
                status_out = _error_status_for_exception(exc)
                http_code = _status_code_from_exception(exc)
                error_message = str(exc)
                if status_out == STATUS_ERROR_INT:
                    output_for_log = _internal_error_message()
                else:
                    output_for_log = _soft_error_message(exc)

                _log_host_stream(
                    now=datetime.now(),
                    status=f"{status_out}" if http_code is None else f"{status_out} ({http_code})",
                    model_name=model_for_log,
                    request_tier=request_tier_for_log,
                    response_tier="-",
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
                    now=datetime.now(),
                    status=f"{status_out}" if http_code is None else f"{status_out} ({http_code})",
                    model_name=model_for_log,
                    request_tier=request_tier_for_log,
                    response_tier="-",
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
                    now=datetime.now(),
                    status=f"{status_out}" if http_code is None else f"{status_out} ({http_code})",
                    model_name=model_for_log,
                    request_tier=request_tier_for_log,
                    response_tier="-",
                    user_id=user_id_str,
                    usage=None,
                )

                if status_out == STATUS_ERROR_TMP and api_attempt == 0:
                    time.sleep(float(REQUEST_GATE_BACKOFF_SEC))
                    continue
                if status_out == STATUS_ERROR_TMP:
                    _set_tmp_error_block()
                    return GptResponse(
                        content=_tmp_block_message(),
                        raw={"soft_error": True, "tmp_error_block": True},
                        usage=GptUsage(),
                        status=STATUS_ERROR_TMP,
                    )
                return GptResponse(
                    content=output_for_log,
                    raw={"soft_error": True},
                    usage=GptUsage(),
                    status=status_out,
                )

        return GptResponse(
            content=_internal_error_message(),
            raw={"soft_error": True, "unexpected_flow": True},
            usage=GptUsage(),
            status=STATUS_ERROR_INT,
        )

    def ask(
        self,
        *,
        model: Optional[str] = None,
        instructions: Optional[str] = None,
        input: Optional[str] = None,
        override: Optional[Dict[str, Any]] = None,
        use_local_cache: bool = False,
        use_gpt_cache: bool = True,
        user_id: Any = "SET USER URGENTLY",
        service_tier: Optional[str] = None,
        web_search: bool = False,
    ) -> GptResponse:
        user_id_str = _optional_str(user_id) or "SET USER URGENTLY"
        effective_tier: str = service_tier or "flex"

        # override: bypass всех guard'ов (как ты и хотел)
        if override is not None:
            if not isinstance(override, dict):
                raise GptValidationError("override must be a dict.")
            override_payload = dict(override)
            override_model = _optional_str(override_payload.get("model"))
            if override_model:
                override_payload["model"] = _resolve_model_name(override_model)
            override_instructions = _optional_str(override_payload.get("instructions"))
            if use_gpt_cache:
                if override_instructions and not _optional_str(override_payload.get("prompt_cache_key")):
                    override_payload["prompt_cache_key"] = _short_hash(override_instructions, 16)
            else:
                override_payload.pop("prompt_cache_key", None)
            if web_search:
                _apply_web_search(override_payload, web_search=True)
            return self._run_payload(
                payload=override_payload,
                user_id_str=user_id_str,
                default_tier=effective_tier,
            )

        instr = _optional_str(instructions)
        inp = _optional_str(input)
        model_name = _resolve_model_name(model)

        if not instr and not inp:
            # Пустой запрос — платформу не зовём, и лог не пишем.
            return GptResponse(content="", raw={}, usage=GptUsage(), status=STATUS_OK)

        query: Tuple[str, str, str, str, str] = (
            model_name,
            instr,
            inp,
            "web:on" if web_search else "web:off",
            "gptcache:on" if use_gpt_cache else "gptcache:off",
        )

        last_raw: Optional[Dict[str, Any]] = None
        last_usage: Optional[GptUsage] = None

        def _fn(q: Tuple[str, str, str, str, str]) -> str:
            nonlocal last_raw, last_usage

            m, ins, inpt, _, _ = q
            payload = _build_payload(
                model_name=m,
                instructions=ins,
                input_text=inpt,
                service_tier=effective_tier,
                web_search=web_search,
                use_gpt_cache=use_gpt_cache,
            )

            payload["service_tier"] = effective_tier

            resp = self._run_payload(
                payload=payload,
                user_id_str=user_id_str,
                default_tier=effective_tier,
            )
            if resp.status != STATUS_OK:
                raise GptSoftError(resp.content, status=resp.status, error_message=resp.content)

            last_raw = resp.raw
            last_usage = resp.usage
            return str(resp.content or "")

        try:
            if use_local_cache:
                local_cache_ttl_sec = _random_local_cache_ttl_sec()
                content = cache_memo(
                    query,
                    _fn,
                    ttl=local_cache_ttl_sec,
                    version="gpt.content.v2",
                    update=False,
                )
                if last_raw is None:
                    _log_host_stream(
                        now=datetime.now(),
                        status="cache",
                        model_name=model_name,
                        request_tier=effective_tier,
                        response_tier="-",
                        user_id=user_id_str,
                        instructions=instr,
                        input_text=inp,
                        output_text=str(content or ""),
                        usage=None,
                        cache_hit=True,
                        real_request=False,
                    )
                    return GptResponse(content=str(content or ""), raw={"cached": True}, usage=GptUsage(), status=STATUS_OK)
                return GptResponse(content=str(content or ""), raw=last_raw or {}, usage=last_usage or GptUsage(), status=STATUS_OK)

            content = _fn(query)
            return GptResponse(content=str(content or ""), raw=last_raw or {}, usage=last_usage or GptUsage(), status=STATUS_OK)
        except GptSoftError as exc:
            return GptResponse(content=exc.user_message, raw={"soft_error": True}, usage=GptUsage(), status=exc.status)

    @staticmethod
    def _extract_usage(raw: Dict[str, Any]) -> GptUsage:
        usage = raw.get("usage") or {}
        return GptUsage(
            prompt_tokens=usage.get("prompt_tokens") or usage.get("input_tokens"),
            completion_tokens=usage.get("completion_tokens") or usage.get("output_tokens"),
            total_tokens=usage.get("total_tokens"),
        )

    def ask_dialog(
        self,
        *,
        model: Optional[str] = None,
        input: str,
        instructions: str = "",
        use_gpt_cache: bool = True,
        user_id: Any = "SET USER URGENTLY",
        service_tier: Optional[str] = None,
        conversation: Optional[str] = None,
        previous_response_id: Optional[str] = None,
        web_search: bool = False,
    ) -> GptResponse:
        """
        Dialog branch (platform-managed context):
        - always store=True
        - no local cache/history
        - caller keeps only key ids (conversation/response)
        """
        model_name = _resolve_model_name(model)

        payload: Dict[str, Any] = {
            "model": model_name,
            "input": _optional_str(input),
            "store": True,
            "service_tier": (service_tier or "flex"),
        }

        instr = _optional_str(instructions)
        if instr:
            payload["instructions"] = instr

        conv = _optional_str(conversation)
        if conv:
            payload["conversation"] = conv

        prev = _optional_str(previous_response_id)
        if prev:
            payload["previous_response_id"] = prev

        if web_search:
            _apply_web_search(payload, web_search=True)

        resp = self.ask(
            override=payload,
            use_local_cache=False,
            use_gpt_cache=use_gpt_cache,
            user_id=user_id,
            service_tier=service_tier or "flex",
            web_search=web_search,
        )
        return resp
