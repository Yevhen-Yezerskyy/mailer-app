# FILE: web/panel/aap_campaigns/urls.py
# DATE: 2026-01-22
# PURPOSE: Добавить letter-editor API (extract/render) для campaigns (как в templates).
# CHANGE:
# - New paths: campaigns/letter/_extract-content/ + campaigns/letter/_render-editor-html/
# - New path: campaigns/letter/_buttons-by-template/ (GlobalTemplate.buttons)

from __future__ import annotations

from django.urls import path

from panel.aap_campaigns.views.campaigns import (
    campaigns_flow_campaign_view,
    campaigns_flow_letter_view,
    campaigns_flow_template_view,
    campaigns_view,
)
from panel.aap_campaigns.views.campaigns_api import (
    campaigns__letter_extract_content_view,
    campaigns__letter_buttons_by_template_view,
    campaigns__letter_guard_modal_view,
    campaigns__template_choose_warning_modal_view,
    campaigns__letter_render_editor_html_view,
    campaigns__preview_modal_by_id_view,
    campaigns__preview_modal_from_editor_view,
)
from panel.aap_campaigns.views.templates import templates_view
from panel.aap_campaigns.views.templates_api import (
    templates__global_style_css_view,
    templates__parse_editor_html_view,
    templates__preview_modal_by_id_view,
    templates__preview_modal_from_editor_view,
    templates__render_editor_html_view,
    templates__render_user_css_view,
    templates__render_user_html_view,
)

app_name = "campaigns"

urlpatterns = [
    path("campaigns/", campaigns_view, name="campaigns"),
    path("campaigns/flow/", campaigns_flow_campaign_view, name="campaigns_flow"),
    path("campaigns/flow/campaign/", campaigns_flow_campaign_view, name="campaigns_flow_campaign"),
    path("campaigns/flow/campaign/<str:item_id>/", campaigns_flow_campaign_view, name="campaigns_flow_campaign_id"),
    path("campaigns/flow/template/", campaigns_flow_template_view, name="campaigns_flow_template"),
    path("campaigns/flow/template/<str:item_id>/", campaigns_flow_template_view, name="campaigns_flow_template_id"),
    path("campaigns/flow/letter/", campaigns_flow_letter_view, name="campaigns_flow_letter"),
    path("campaigns/flow/letter/<str:item_id>/", campaigns_flow_letter_view, name="campaigns_flow_letter_id"),
    path("campaigns/preview/modal/", campaigns__preview_modal_by_id_view, name="campaigns__preview_modal_by_id"),
    path(
        "campaigns/preview/modal-from-editor/",
        campaigns__preview_modal_from_editor_view,
        name="campaigns__preview_modal_from_editor",
    ),

    # NEW: letter editor API (python-only HTML ops)
    path("campaigns/letter/_extract-content/", campaigns__letter_extract_content_view, name="campaigns__letter_extract_content"),
    path("campaigns/letter/_render-editor-html/", campaigns__letter_render_editor_html_view, name="campaigns__letter_render_editor_html"),
    path("campaigns/letter/_buttons-by-template/", campaigns__letter_buttons_by_template_view, name="campaigns__letter_buttons_by_template"),
    path("campaigns/letter/modal/guard/", campaigns__letter_guard_modal_view, name="campaigns__letter_guard_modal"),
    path("campaigns/template/modal/choose-warning/", campaigns__template_choose_warning_modal_view, name="campaigns__template_choose_warning_modal"),

    # templates
    path("templates/", templates_view, name="templates"),
    path("templates/_render-user-html/", templates__render_user_html_view, name="templates__render_user_html"),
    path("templates/_render-user-css/", templates__render_user_css_view, name="templates__render_user_css"),
    path("templates/_parse-editor-html/", templates__parse_editor_html_view, name="templates__parse_editor_html"),
    path("templates/_render-editor-html/", templates__render_editor_html_view, name="templates__render_editor_html"),
    path("templates/preview/modal/", templates__preview_modal_by_id_view, name="templates__preview_modal_by_id"),
    path("templates/preview/modal-from-editor/", templates__preview_modal_from_editor_view, name="templates__preview_modal_from_editor"),
    path("templates/_global-style-css/", templates__global_style_css_view, name="templates__global_style_css"),
]
