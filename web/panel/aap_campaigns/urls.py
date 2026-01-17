# FILE: web/panel/aap_campaigns/urls.py  (обновлено — 2026-01-17)
# PURPOSE: URLs для campaigns/templates + API (Tiny switch + preview-modal).
# CHANGE: Preview теперь возвращает СРАЗУ HTML модалки (GET by id; POST from editor).

from __future__ import annotations

from django.urls import path

from panel.aap_campaigns.views.templates import templates_view
from panel.aap_campaigns.views.templates_api import (
    templates__render_user_html_view,
    templates__render_user_css_view,
    templates__parse_editor_html_view,
    templates__render_editor_html_view,
    templates__preview_modal_by_id_view,
    templates__preview_modal_from_editor_view,
)

app_name = "campaigns"

urlpatterns = [
    path("templates/", templates_view, name="templates"),
    path("templates/_render-user-html/", templates__render_user_html_view, name="templates__render_user_html"),
    path("templates/_render-user-css/", templates__render_user_css_view, name="templates__render_user_css"),
    path("templates/_parse-editor-html/", templates__parse_editor_html_view, name="templates__parse_editor_html"),
    path("templates/_render-editor-html/", templates__render_editor_html_view, name="templates__render_editor_html"),
    path("templates/preview/modal/", templates__preview_modal_by_id_view, name="templates__preview_modal_by_id"),
    path("templates/preview/modal-from-editor/", templates__preview_modal_from_editor_view, name="templates__preview_modal_from_editor"),
]
