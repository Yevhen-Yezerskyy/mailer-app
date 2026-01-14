# FILE: web/panel/aap_campaigns/urls.py  (новое)
# DATE: 2026-01-14
# PURPOSE: URLs для campaigns/templates + server-api для редактора.

from __future__ import annotations

from django.urls import path

from panel.aap_campaigns.views.templates import templates_view
from panel.aap_campaigns.views.templates_api import (
    templates_normalize_view,
    templates_preview_view,
    templates_render_user_view,
)

app_name = "aap_campaigns"

urlpatterns = [
    path("templates/", templates_view, name="templates"),
    path("templates/_render_user/", templates_render_user_view, name="templates_render_user"),
    path("templates/_normalize/", templates_normalize_view, name="templates_normalize"),
    path("templates/_preview/", templates_preview_view, name="templates_preview"),
]
