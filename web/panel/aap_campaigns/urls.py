# FILE: web/panel/aap_campaigns/urls.py
# DATE: 2026-01-19
# PURPOSE: URLs для campaigns/templates + campaigns (одна страница, state в GET) + preview-modal для письма кампании.
# CHANGE: Добавлены /campaigns/ + /campaigns/preview/modal(+from-editor).

from __future__ import annotations

from django.urls import path

from panel.aap_campaigns.views.campaigns import campaigns_view
from panel.aap_campaigns.views.campaigns_api import (
    campaigns__preview_modal_by_id_view,
    campaigns__preview_modal_from_editor_view,
)
from panel.aap_campaigns.views.templates import templates_view
from panel.aap_campaigns.views.templates_api import (
    templates__render_user_html_view,
    templates__render_user_css_view,
    templates__parse_editor_html_view,
    templates__render_editor_html_view,
    templates__preview_modal_by_id_view,
    templates__preview_modal_from_editor_view,
    templates__global_style_css_view,
)

app_name = "campaigns"

urlpatterns = [
    # campaigns (one page)
    path("campaigns/", campaigns_view, name="campaigns"),
    path("campaigns/preview/modal/", campaigns__preview_modal_by_id_view, name="campaigns__preview_modal_by_id"),
    path(
        "campaigns/preview/modal-from-editor/",
        campaigns__preview_modal_from_editor_view,
        name="campaigns__preview_modal_from_editor",
    ),
    # templates (existing)
    path("templates/", templates_view, name="templates"),
    path("templates/_render-user-html/", templates__render_user_html_view, name="templates__render_user_html"),
    path("templates/_render-user-css/", templates__render_user_css_view, name="templates__render_user_css"),
    path("templates/_parse-editor-html/", templates__parse_editor_html_view, name="templates__parse_editor_html"),
    path("templates/_render-editor-html/", templates__render_editor_html_view, name="templates__render_editor_html"),
    path("templates/preview/modal/", templates__preview_modal_by_id_view, name="templates__preview_modal_by_id"),
    path("templates/preview/modal-from-editor/", templates__preview_modal_from_editor_view, name="templates__preview_modal_from_editor"),
    path("templates/_global-style-css/", templates__global_style_css_view, name="templates__global_style_css"),
]
