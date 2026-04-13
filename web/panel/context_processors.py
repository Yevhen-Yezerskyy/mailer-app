# FILE: web/panel/context_processors.py  (обновлено — 2025-12-18)
# Смысл: подготовка меню панели + вычисление active/open + page_title

from django.urls import reverse, NoReverseMatch
from django.db import connection
from django.utils.translation import gettext as _

from mailer_web.access import encode_id
from .menu import PANEL_MENU


def _safe_reverse(name):
    try:
        return reverse(name)
    except NoReverseMatch:
        return "#"


def _starts_with(path, prefixes):
    return any(path.startswith(p) for p in prefixes)


def panel_context(request):
    path = request.path or ""
    page_title = None

    menu = []
    for section in PANEL_MENU:
        sec = dict(section)
        sec["open"] = _starts_with(path, sec.get("open_prefixes", []))
        if sec.get("url"):
            sec["url"] = sec["url"]
        elif sec.get("url_name"):
            sec["url"] = _safe_reverse(sec["url_name"])

        items = []
        for item in sec["items"]:
            it = dict(item)
            if it.get("url"):
                it["url"] = it["url"]
            else:
                it["url"] = _safe_reverse(it["url_name"])
            it["active"] = _starts_with(path, it.get("active_prefixes", []))

            if it["active"] and not page_title:
                page_title = it.get("page_title")

            items.append(it)

        if sec.get("dynamic_stats_campaigns"):
            ws_id = getattr(request, "workspace_id", None)
            if ws_id:
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                          c.id,
                          c.title,
                          MAX(s.created_at) AS last_sent_at
                        FROM public.campaigns_campaigns c
                        LEFT JOIN public.mailbox_sent s ON s.campaign_id = c.id
                        WHERE c.workspace_id = %s::uuid
                        GROUP BY c.id, c.title, c.created_at
                        ORDER BY last_sent_at DESC NULLS LAST, c.created_at DESC
                        """,
                        [ws_id],
                    )
                    for cid, title, _last_sent_at in cur.fetchall() or []:
                        title_txt = (title or "").strip() or f"#{int(cid)}"
                        uid = encode_id(int(cid))
                        item_path = f"/panel/stats/campaign/{uid}/"
                        is_active = _starts_with(path, [item_path])
                        items.append(
                            {
                                "title": title_txt,
                                "page_title": f"{_('Статистика')} - {title_txt}",
                                "url": item_path,
                                "active_prefixes": [item_path],
                                "active": is_active,
                            }
                        )
                        if is_active and not page_title:
                            page_title = f"{_('Статистика')} - {title_txt}"

        sec["items"] = items
        sec["open"] = sec.get("open") or any(bool(it.get("active")) for it in items)
        menu.append(sec)

    return {
        "panel_menu": menu,
        "page_title": page_title or _("SERENITY PANEL"),
    }
