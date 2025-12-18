# FILE: web/panel/aap_audience/views/status.py  (обновлено — 2025-12-18)
# CHANGE: исправлен импорт AudienceTask после переезда аппа под panel/,
#         логика и SQL без изменений.

from django.db import connection
from django.shortcuts import render

from panel.aap_audience.models import AudienceTask


def status_view(request):
    ws_id = request.workspace_id
    user = request.user

    tasks = list(AudienceTask.objects.filter(workspace_id=ws_id, user=user))
    task_ids = [t.id for t in tasks]

    counts = {}     # {task_id: {"city": int, "branch": int}}
    companies = {}  # {task_id: int}

    if task_ids:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT task_id, type, COUNT(*)::int
                FROM crawl_tasks
                WHERE workspace_id = %s
                  AND user_id = %s
                  AND task_id = ANY(%s)
                GROUP BY task_id, type
                """,
                [str(ws_id), int(user.id), task_ids],
            )
            for task_id, typ, cnt in cur.fetchall():
                d = counts.setdefault(int(task_id), {"city": 0, "branch": 0})
                if typ in ("city", "branch"):
                    d[typ] = int(cnt)

            cur.execute(
                """
                SELECT qs.task_id, COUNT(rc.id)::int
                FROM queue_sys qs
                JOIN raw_contacts_gb rc
                  ON rc.cb_crawler_id = qs.cb_crawler_id
                WHERE qs.status = 'collected'
                  AND qs.task_id = ANY(%s)
                GROUP BY qs.task_id
                """,
                [task_ids],
            )
            for task_id, cnt in cur.fetchall():
                companies[int(task_id)] = int(cnt)

    return render(
        request,
        "panels/aap_audience/status.html",
        {
            "title": "Статус подбора",
            "tasks": tasks,
            "counts": counts,
            "companies": companies,
        },
    )
