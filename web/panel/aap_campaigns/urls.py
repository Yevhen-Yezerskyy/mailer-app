# FILE: web/panel/aap_campaigns/urls.py
# DATE: 2026-01-14
# PURPOSE: URLs для campaigns/templates + API для Quill.
# CHANGE: Добавлены 2 GET endpoint'а: render-user-html и render-user-css.

from __future__ import annotations

from django.urls import path

from panel.aap_campaigns.views.templates import templates_view
from panel.aap_campaigns.views.templates_api import (
    templates_render_user_html_view,
    templates_render_user_css_view,
)

app_name = "campaigns"

urlpatterns = [
    path("templates/", templates_view, name="templates"),
    path("templates/render-user-html/", templates_render_user_html_view, name="templates_render_user_html"),
    path("templates/render-user-css/", templates_render_user_css_view, name="templates_render_user_css"),
]
