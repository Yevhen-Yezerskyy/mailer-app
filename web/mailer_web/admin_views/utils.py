# FILE: web/mailer_web/admin_views/utils.py  (новое — 2026-02-10)
# PURPOSE: Общие утилиты для кастомных админ-вью: регистрация URL, рендер с admin context.

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from django.contrib import admin
from django.http import HttpRequest, HttpResponse
from django.template.response import TemplateResponse
from django.urls import path


ViewFn = Callable[[HttpRequest], HttpResponse]


@dataclass(frozen=True)
class AdminPage:
    route: str                  # без leading slash, относительный к /admin/
    name: str                   # name в namespace admin:...
    title: str                  # заголовок страницы
    nav_section: str            # заголовок секции на admin/index.html
    nav_label: str              # текст ссылки на admin/index.html
    view: ViewFn                # view-функция


def render_admin(
    request: HttpRequest,
    *,
    template: str,
    context: dict,
    admin_site: admin.sites.AdminSite = admin.site,
) -> TemplateResponse:
    ctx = admin_site.each_context(request)
    ctx.update(context or {})
    return TemplateResponse(request, template, ctx)


def register_admin_pages(
    admin_site: admin.sites.AdminSite,
    pages: list[AdminPage],
) -> None:
    orig_get_urls = admin_site.get_urls

    def get_urls():
        urls = orig_get_urls()
        custom = [
            path(
                p.route,
                admin_site.admin_view(p.view),
                name=p.name,
            )
            for p in pages
        ]
        return custom + urls

    admin_site.get_urls = get_urls  # type: ignore[assignment]
