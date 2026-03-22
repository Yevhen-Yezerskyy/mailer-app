# FILE: web/panel/aap_audience/views/modal_insert_company.py
# DATE: 2026-03-22
# PURPOSE: Modal list of existing audience tasks with saved company descriptions for insertion into the flow.

from django.shortcuts import render
from django.utils.translation import gettext_lazy as _

from mailer_web.access import decode_id
from panel.aap_audience.models import AudienceTask


def _decode_task_id(token: str) -> int | None:
    if not token:
        return None
    try:
        return int(decode_id(token))
    except Exception:
        return None


def modal_insert_company_view(request):
    current_token = (request.GET.get("id") or "").strip()
    current_task_id = _decode_task_id(current_token)

    queryset = AudienceTask.objects.filter(
        workspace_id=request.workspace_id,
        archived=False,
    ).order_by("-updated_at")

    if current_task_id:
        queryset = queryset.exclude(id=current_task_id)

    items = []
    for task in queryset:
        company_value = (task.source_company or "").strip()
        if not company_value:
            continue
        title_value = (task.title or "").strip() or f"{_('Список рассылки')} #{int(task.id)}"
        items.append(
            {
                "id": int(task.id),
                "type": str(task.type or "").strip(),
                "title": title_value,
                "source_company": company_value,
            }
        )

    return render(
        request,
        "panels/aap_audience/modal_insert_company.html",
        {
            "items": items,
        },
    )
