# FILE: web/panel/aap_settings/views/mail_servers_api.py
# DATE: 2026-01-23
# PURPOSE: AJAX API для "Проверок" в Settings → Mail servers (SMTP/IMAP/DOMAIN).
# CHANGE: check_imap теперь делает ОДИН логин и сразу отдаёт папки (imap_check_and_log).

from __future__ import annotations

import json
from typing import Any, Dict, List

from django.http import JsonResponse
from django.views.decorators.http import require_POST

#from engine.common.mail.smtp_test import smtp_check_and_log
#from engine.common.mail.imap_test import imap_check_and_log
#from engine.common.mail.domain_checks_test import domain_tech_check_and_log, domain_reputation_check_and_log

from web.mailer_web.access import decode_id
from panel.aap_settings.models import Mailbox


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None
    return ws_id


def _pp(data: Any) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return str(data)


def _first_err(*vals: str) -> str:
    for v in vals:
        if v:
            return str(v)
    return ""


def _explain_dns_err(code: str) -> str:
    code = (code or "").strip()
    if code == "dig_failed":
        return "DNS-запрос не выполнен (утилита dig отсутствует или не запускается в контейнере)."
    if code == "dig_error":
        return "DNS-запрос завершился ошибкой (dig вернул ненулевой код)."
    if code == "domain_not_found":
        return "Домен для ящика не найден."
    if code:
        return f"DNS-ошибка: {code}"
    return "DNS-ошибка."


def _smtp_report(r) -> str:
    lines: List[str] = []
    lines.append("Проверка SMTP")
    lines.append(f"Статус: {r.status}")
    if r.user_message:
        lines.append(f"Сообщение: {r.user_message}")
    lines.append("")
    lines.append("Технические данные проверки SMTP:")
    lines.append(_pp(r.data or {}))
    return "\n".join(lines)


def _imap_report(r) -> str:
    lines: List[str] = []
    lines.append("Проверка IMAP")
    lines.append(f"Статус: {r.status}")
    if r.user_message:
        lines.append(f"Сообщение: {r.user_message}")

    folders = []
    if isinstance(r.data, dict):
        folders = r.data.get("folders") or []

    if folders:
        lines.append("")
        lines.append(f"Папки ({len(folders)}):")
        for f in folders[:200]:
            lines.append(f"- {f}")
        if len(folders) > 200:
            lines.append(f"... ещё {len(folders) - 200}")

    lines.append("")
    lines.append("Технические данные проверки IMAP:")
    lines.append(_pp(r.data or {}))
    return "\n".join(lines)


def _domain_report(r_tech, r_rep) -> str:
    tech = r_tech.data or {}
    rep = r_rep.data or {}

    domain = (tech.get("domain") or rep.get("domain") or "").strip()
    lines: List[str] = []
    lines.append("Проверка домена" + (f": {domain}" if domain else ""))

    tech_status = r_tech.status
    lines.append(f"Техническая часть: {tech_status}")
    if tech_status == "CHECK_FAILED":
        spf_err = str(tech.get("spf_err") or "")
        dmarc_err = str(tech.get("dmarc_err") or "")
        err = _first_err(spf_err, dmarc_err, str(tech.get("error") or ""))
        lines.append(_explain_dns_err(err))
        if spf_err or dmarc_err:
            lines.append(f"SPF: {spf_err or 'ok'}; DMARC: {dmarc_err or 'ok'}")

    rep_status = r_rep.status
    lines.append("")
    lines.append(f"Репутация (Spamhaus DBL/DQS): {rep_status}")
    if rep_status == "CHECK_FAILED":
        lines.append(_explain_dns_err(str(rep.get("error") or "")))

    links = rep.get("links") or []
    if links:
        lines.append("")
        lines.append("Ссылки для проверки:")
        for u in links:
            lines.append(f"- {u}")

    lines.append("")
    lines.append("Технические данные запроса DNS (SPF/DMARC):")
    lines.append(_pp(tech))

    lines.append("")
    lines.append("Технические данные проверки Spamhaus DBL/DQS:")
    lines.append(_pp(rep))

    return "\n".join(lines)


@require_POST
def mail_servers_api_view(request):
    ws_id = _guard(request)
    if not ws_id:
        return JsonResponse({"ok": False, "error": "auth"}, status=403)

    action = (request.POST.get("action") or "").strip()
    if action not in ("check_smtp", "check_imap", "check_domain"):
        return JsonResponse({"ok": False, "error": "bad_action"}, status=400)

    tok = (request.POST.get("id") or "").strip()
    if not tok:
        return JsonResponse({"ok": False, "error": "missing_id"}, status=400)

    try:
        mailbox_id = int(decode_id(tok))
    except Exception:
        return JsonResponse({"ok": False, "error": "bad_id"}, status=400)

    if not Mailbox.objects.filter(id=mailbox_id, workspace_id=ws_id).exists():
        return JsonResponse({"ok": False, "error": "not_found"}, status=404)

    if action == "check_smtp":
        r = smtp_check_and_log(mailbox_id)
        return JsonResponse({"ok": True, "action": "SMTP_CHECK", "status": "", "message": _smtp_report(r)})

    if action == "check_imap":
        r = imap_check_and_log(mailbox_id)
        return JsonResponse({"ok": True, "action": "IMAP_CHECK", "status": "", "message": _imap_report(r)})

    r_tech = domain_tech_check_and_log(mailbox_id)
    r_rep = domain_reputation_check_and_log(mailbox_id)
    return JsonResponse({"ok": True, "action": "DOMAIN_CHECK", "status": "", "message": _domain_report(r_tech, r_rep)})
