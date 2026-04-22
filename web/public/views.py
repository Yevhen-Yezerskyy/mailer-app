# FILE: web/public/views.py
# DATE: 2026-04-09
# PURPOSE: public pages plus direct browser diagnostics for dev fingerprint checks.

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit

from django.http import HttpResponseRedirect, JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TESTING_LOG_DIR = PROJECT_ROOT / "logs" / "testing"
ECHO_LOG_PATH = TESTING_LOG_DIR / "public_diag_echo.jsonl"
FINGERPRINT_LOG_PATH = TESTING_LOG_DIR / "public_diag_fingerprint.jsonl"
DIAG_SESSION_COOKIE = "diag11880_sid"
DIAG_VISITOR_COOKIE = "diag11880_vid"
DIAG_HOPS_COOKIE = "diag11880_hops"
DIAG_CREATED_COOKIE = "diag11880_created"
DIAG_FLOW_COOKIE = "diag11880_flow"
DIAG_COOKIE_MAX_AGE_SEC = 12 * 60 * 60


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_headers_dict(request) -> dict[str, str]:
    return {key: value for key, value in request.headers.items()}


def _request_cookies_dict(request) -> dict[str, str]:
    return {key: value for key, value in request.COOKIES.items()}


def _request_meta_dict(request) -> dict[str, object]:
    keys: list[str] = []
    for key in sorted(request.META):
        if key.startswith(("HTTP_", "REMOTE_", "SERVER_", "REQUEST_", "CONTENT_", "wsgi.")):
            keys.append(key)
            continue
        if key in {"PATH_INFO", "QUERY_STRING", "SCRIPT_NAME"}:
            keys.append(key)
    out: dict[str, object] = {}
    for key in keys:
        value = request.META.get(key)
        if isinstance(value, (str, int, float, bool)) or value is None:
            out[key] = value
            continue
        out[key] = str(value)
    return out


def _request_socket_view(request) -> dict[str, object]:
    meta = request.META
    return {
        "remote_addr": meta.get("REMOTE_ADDR"),
        "remote_port": meta.get("REMOTE_PORT"),
        "server_name": meta.get("SERVER_NAME"),
        "server_port": meta.get("SERVER_PORT"),
        "server_protocol": meta.get("SERVER_PROTOCOL"),
        "request_method": meta.get("REQUEST_METHOD"),
        "path_info": meta.get("PATH_INFO"),
        "query_string": meta.get("QUERY_STRING"),
        "x_forwarded_for": meta.get("HTTP_X_FORWARDED_FOR"),
        "x_real_ip": meta.get("HTTP_X_REAL_IP"),
        "x_forwarded_proto": meta.get("HTTP_X_FORWARDED_PROTO"),
        "host_header": meta.get("HTTP_HOST"),
    }


def _cookie_specs_to_payload(specs: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for row in specs:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        out[name] = {
            "value": str(row.get("value") or ""),
            "max_age": int(row.get("max_age") or 0),
            "path": str(row.get("path") or "/"),
            "secure": bool(row.get("secure") is True),
            "httponly": bool(row.get("httponly") is True),
            "samesite": str(row.get("samesite") or ""),
        }
    return out


def _diag_flow_payload(request, *, default_step: str = "", flow_id: str = "") -> dict[str, object]:
    referer = str(request.META.get("HTTP_REFERER") or "").strip()
    referer_path = ""
    referer_same_origin = False
    if referer:
        try:
            parsed = urlsplit(referer)
            referer_path = str(parsed.path or "")
            referer_same_origin = bool(parsed.scheme == request.scheme and parsed.netloc == request.get_host())
        except Exception:
            referer_path = ""
            referer_same_origin = False
    query_flow_id = str(request.GET.get("diag_flow_id") or flow_id or "").strip()
    cookie_flow_id = str(request.COOKIES.get(DIAG_FLOW_COOKIE) or "").strip()
    effective_flow_id = query_flow_id or cookie_flow_id or uuid.uuid4().hex[:12]
    step = str(request.GET.get("diag_step") or default_step or "").strip()
    expected_prev_path = ""
    if step == "landing":
        expected_prev_path = reverse("public_diag_start")
    elif step in {"xhr", "light_check"}:
        expected_prev_path = reverse("public_diag_fingerprint")
    return {
        "flow_id": effective_flow_id,
        "step": step,
        "incoming_query_flow_id": query_flow_id,
        "incoming_cookie_flow_id": cookie_flow_id,
        "reused_flow_cookie": bool(cookie_flow_id and cookie_flow_id == effective_flow_id),
        "referer": referer,
        "referer_path": referer_path,
        "referer_same_origin": referer_same_origin,
        "expected_prev_path": expected_prev_path,
        "referer_matches_expected": bool(expected_prev_path and referer_path == expected_prev_path),
    }


def _diag_cookie_specs(
    request,
    diag_session: dict[str, object],
    diag_flow: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    issued = dict(diag_session.get("issued") or {})
    secure = bool(request.is_secure())
    common = {
        "max_age": DIAG_COOKIE_MAX_AGE_SEC,
        "path": "/",
        "secure": secure,
        "samesite": "Lax",
    }
    specs = [
        {
            "name": DIAG_SESSION_COOKIE,
            "value": str(issued.get("session_id") or ""),
            "httponly": True,
            **common,
        },
        {
            "name": DIAG_VISITOR_COOKIE,
            "value": str(issued.get("visitor_id") or ""),
            "httponly": False,
            **common,
        },
        {
            "name": DIAG_HOPS_COOKIE,
            "value": str(issued.get("request_ordinal") or 1),
            "httponly": False,
            **common,
        },
        {
            "name": DIAG_CREATED_COOKIE,
            "value": str(issued.get("created_at_unix") or 0),
            "httponly": False,
            **common,
        },
    ]
    if diag_flow is not None:
        specs.append(
            {
                "name": DIAG_FLOW_COOKIE,
                "value": str(diag_flow.get("flow_id") or ""),
                "httponly": False,
                **common,
            }
        )
    return specs


def _apply_cookie_specs(response, specs: list[dict[str, object]]) -> None:
    for row in specs:
        response.set_cookie(
            key=str(row.get("name") or ""),
            value=str(row.get("value") or ""),
            max_age=int(row.get("max_age") or 0),
            path=str(row.get("path") or "/"),
            secure=bool(row.get("secure") is True),
            httponly=bool(row.get("httponly") is True),
            samesite=str(row.get("samesite") or "Lax"),
        )


def _diag_session_payload(request) -> dict[str, object]:
    now_dt = datetime.now(timezone.utc)
    now_ts = int(now_dt.timestamp())
    incoming_session_id = str(request.COOKIES.get(DIAG_SESSION_COOKIE) or "").strip()
    incoming_visitor_id = str(request.COOKIES.get(DIAG_VISITOR_COOKIE) or "").strip()
    incoming_created_raw = str(request.COOKIES.get(DIAG_CREATED_COOKIE) or "").strip()
    incoming_hops_raw = str(request.COOKIES.get(DIAG_HOPS_COOKIE) or "").strip()
    try:
        incoming_created_ts = int(incoming_created_raw)
    except Exception:
        incoming_created_ts = now_ts
    try:
        incoming_hops = max(0, int(incoming_hops_raw))
    except Exception:
        incoming_hops = 0
    issued_session_id = incoming_session_id or uuid.uuid4().hex
    issued_visitor_id = incoming_visitor_id or uuid.uuid4().hex[:12]
    issued_created_ts = incoming_created_ts if incoming_session_id else now_ts
    issued_created_dt = datetime.fromtimestamp(issued_created_ts, timezone.utc)
    request_ordinal = incoming_hops + 1
    continuity = {
        "has_existing_session_cookie": bool(incoming_session_id),
        "has_existing_visitor_cookie": bool(incoming_visitor_id),
        "reused_existing_session_id": bool(incoming_session_id),
        "reused_existing_visitor_id": bool(incoming_visitor_id),
        "request_ordinal_before": incoming_hops,
        "request_ordinal_after": request_ordinal,
        "session_age_sec": max(0, now_ts - issued_created_ts),
        "cookie_names_seen": sorted(_request_cookies_dict(request).keys()),
    }
    return {
        "incoming": {
            "session_id": incoming_session_id,
            "visitor_id": incoming_visitor_id,
            "created_at_unix": incoming_created_ts if incoming_session_id else 0,
            "request_ordinal": incoming_hops,
        },
        "issued": {
            "session_id": issued_session_id,
            "visitor_id": issued_visitor_id,
            "created_at_unix": issued_created_ts,
            "created_at_utc": issued_created_dt.isoformat(),
            "request_ordinal": request_ordinal,
        },
        "continuity": continuity,
    }


def _base_request_payload(
    request,
    request_id: str,
    diag_session: dict[str, object] | None = None,
    diag_flow: dict[str, object] | None = None,
    response_cookies: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    payload = {
        "request_id": request_id,
        "ts_utc": _utc_now_iso(),
        "method": request.method,
        "path": request.path,
        "full_path": request.get_full_path(),
        "host": request.get_host(),
        "scheme": request.scheme,
        "remote_addr": request.META.get("REMOTE_ADDR"),
        "x_forwarded_for": request.META.get("HTTP_X_FORWARDED_FOR"),
        "user_agent": request.META.get("HTTP_USER_AGENT"),
        "query": {key: request.GET.getlist(key) for key in request.GET},
        "headers": _request_headers_dict(request),
        "cookies": _request_cookies_dict(request),
        "meta": _request_meta_dict(request),
        "socket_view": _request_socket_view(request),
    }
    if diag_session is not None:
        payload["diag_session"] = diag_session
    if diag_flow is not None:
        payload["diag_flow"] = diag_flow
    if response_cookies is not None:
        payload["response_cookies"] = response_cookies
    return payload


def _append_jsonl(path: Path, payload: dict[str, object]) -> None:
    TESTING_LOG_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        fh.write("\n")


def public_index(request):
    return render(request, "public/test.html")


def public_test(request):
    return render(request, "public/test.html")


def public_impressum(request):
    return render(request, "public/impressum.html")


def public_datenschutz(request):
    return render(request, "public/datenschutz.html")


def public_diag_start(request):
    request_id = str(uuid.uuid4())
    diag_session = _diag_session_payload(request)
    diag_flow = _diag_flow_payload(request, default_step="start")
    redirect_params: list[tuple[str, str]] = []
    for key in request.GET:
        if key in {"diag_flow_id", "diag_step"}:
            continue
        for value in request.GET.getlist(key):
            redirect_params.append((str(key), str(value)))
    redirect_params.append(("diag_flow_id", str(diag_flow.get("flow_id") or "")))
    redirect_params.append(("diag_step", "landing"))
    redirect_to = f"{reverse('public_diag_fingerprint')}?{urlencode(redirect_params, doseq=True)}"
    diag_flow = {
        **diag_flow,
        "redirect_to": redirect_to,
    }
    cookie_specs = _diag_cookie_specs(request, diag_session, diag_flow)
    payload = _base_request_payload(
        request,
        request_id,
        diag_session=diag_session,
        diag_flow=diag_flow,
        response_cookies=_cookie_specs_to_payload(cookie_specs),
    )
    _append_jsonl(FINGERPRINT_LOG_PATH, {**payload, "event": "warmup_start"})
    response = HttpResponseRedirect(redirect_to)
    _apply_cookie_specs(response, cookie_specs)
    return response


def public_diag_echo(request):
    request_id = str(uuid.uuid4())
    diag_session = _diag_session_payload(request)
    diag_flow = _diag_flow_payload(request, default_step="echo")
    cookie_specs = _diag_cookie_specs(request, diag_session, diag_flow)
    payload = _base_request_payload(
        request,
        request_id,
        diag_session=diag_session,
        diag_flow=diag_flow,
        response_cookies=_cookie_specs_to_payload(cookie_specs),
    )
    _append_jsonl(ECHO_LOG_PATH, payload)
    response = JsonResponse(payload, json_dumps_params={"ensure_ascii": False, "indent": 2})
    _apply_cookie_specs(response, cookie_specs)
    return response


@csrf_exempt
def public_diag_fingerprint(request):
    if request.method == "POST":
        request_id = request.headers.get("X-Diag-Request-Id") or str(uuid.uuid4())
        diag_session = _diag_session_payload(request)
        diag_flow = _diag_flow_payload(request, default_step="xhr")
        cookie_specs = _diag_cookie_specs(request, diag_session, diag_flow)
        raw_body = request.body.decode("utf-8", errors="replace")
        try:
            fingerprint = json.loads(raw_body) if raw_body else {}
        except json.JSONDecodeError:
            fingerprint = {"_raw_body": raw_body}
        payload = {
            **_base_request_payload(
                request,
                request_id,
                diag_session=diag_session,
                diag_flow=diag_flow,
                response_cookies=_cookie_specs_to_payload(cookie_specs),
            ),
            "client_fingerprint": fingerprint,
        }
        _append_jsonl(FINGERPRINT_LOG_PATH, payload)
        response = JsonResponse(
            {
                "ok": True,
                "request_id": request_id,
                "diag_session": diag_session,
                "diag_flow": diag_flow,
                "response_cookies": _cookie_specs_to_payload(cookie_specs),
            }
        )
        _apply_cookie_specs(response, cookie_specs)
        return response

    request_id = str(uuid.uuid4())
    diag_session = _diag_session_payload(request)
    diag_flow = _diag_flow_payload(request, default_step="landing")
    cookie_specs = _diag_cookie_specs(request, diag_session, diag_flow)
    light_check_url = f"{reverse('public_diag_echo')}?{urlencode({'diag_flow_id': str(diag_flow.get('flow_id') or ''), 'diag_step': 'light_check'})}"
    payload = _base_request_payload(
        request,
        request_id,
        diag_session=diag_session,
        diag_flow=diag_flow,
        response_cookies=_cookie_specs_to_payload(cookie_specs),
    )
    payload["note"] = "Open this URL directly on mailer-web:8000 for raw Django-side diagnostics."
    payload["diag_urls"] = {
        "start_url": reverse("public_diag_start"),
        "landing_url": request.get_full_path(),
        "light_check_url": light_check_url,
    }
    _append_jsonl(FINGERPRINT_LOG_PATH, {**payload, "event": "page_open"})
    response = render(
        request,
        "public/browser_diag.html",
        {
            "request_id": request_id,
            "post_url": request.path,
            "light_check_url": light_check_url,
            "server_request": payload,
        },
    )
    _apply_cookie_specs(response, cookie_specs)
    return response


def error_404(request, exception):
    return render(request, "404.html", status=404)
