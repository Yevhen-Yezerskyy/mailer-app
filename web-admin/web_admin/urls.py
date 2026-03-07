# FILE: web-admin/web_admin/urls.py
# DATE: 2026-03-07
# PURPOSE: URL routing for admin contour with custom login/dashboard and links to Django admin.

from django.contrib import admin
from django.urls import path
from django.contrib.auth.views import LogoutView
from django.urls import include
from functools import wraps

from .views import (
    company_add_view,
    company_delete_view,
    company_edit_view,
    company_modal_view,
    company_restore_view,
    companies_view,
    dashboard_view,
    index_view,
    login_view,
    user_add_view,
    user_edit_view,
    user_modal_view,
    users_view,
)
from .views_settings_mail_template import (
    system_mail_template_view,
    system_templates__global_style_css_view,
    system_templates__parse_editor_html_view,
    system_templates__preview_modal_by_id_view,
    system_templates__preview_modal_from_editor_view,
    system_templates__render_editor_html_view,
    system_templates__render_user_css_view,
    system_templates__render_user_html_view,
)
from .views_settings_mail_letters import (
    letters__buttons_by_template_view,
    letters__extract_content_view,
    letters__preview_modal_from_editor_view,
    letters__render_editor_html_view,
)

_FLAG_ATTR = "_tw_classmap_enabled"


def _flag_view(view_func):
    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        setattr(request, _FLAG_ATTR, True)
        return view_func(request, *args, **kwargs)

    return _wrapped


urlpatterns = [
    path("i18n/", include("django.conf.urls.i18n")),
    path("settings/", include(("web_admin.urls_settings", "settings"), namespace="settings")),
    path("", index_view, name="index"),
    path("login/", login_view, name="login"),
    path("logout/", LogoutView.as_view(next_page="login"), name="logout"),
    path("dashboard/", _flag_view(dashboard_view), name="dashboard"),
    path("companies/", _flag_view(companies_view), name="companies"),
    path("companies/add/", _flag_view(company_add_view), name="company_add"),
    path("companies/<uuid:pk>/delete/", _flag_view(company_delete_view), name="company_delete"),
    path("companies/<uuid:pk>/restore/", _flag_view(company_restore_view), name="company_restore"),
    path("companies/<uuid:pk>/modal/", _flag_view(company_modal_view), name="company_modal"),
    path("companies/<uuid:pk>/", _flag_view(company_edit_view), name="company_edit"),
    path("users/", _flag_view(users_view), name="users"),
    path("users/add/", _flag_view(user_add_view), name="user_add"),
    path("users/<int:pk>/modal/", _flag_view(user_modal_view), name="user_modal"),
    path("users/<int:pk>/", _flag_view(user_edit_view), name="user_edit"),
    # campaign_templates JS compatibility aliases (admin domain)
    path("panel/campaigns/templates/", _flag_view(system_mail_template_view)),
    path("panel/campaigns/templates/_render-user-html/", _flag_view(system_templates__render_user_html_view)),
    path("panel/campaigns/templates/_render-user-css/", _flag_view(system_templates__render_user_css_view)),
    path("panel/campaigns/templates/_parse-editor-html/", _flag_view(system_templates__parse_editor_html_view)),
    path("panel/campaigns/templates/_render-editor-html/", _flag_view(system_templates__render_editor_html_view)),
    path("panel/campaigns/templates/preview/modal/", _flag_view(system_templates__preview_modal_by_id_view)),
    path("panel/campaigns/templates/preview/modal-from-editor/", _flag_view(system_templates__preview_modal_from_editor_view)),
    path("panel/campaigns/templates/_global-style-css/", _flag_view(system_templates__global_style_css_view)),
    path("panel/campaigns/campaigns/letter/_extract-content/", _flag_view(letters__extract_content_view)),
    path("panel/campaigns/campaigns/letter/_render-editor-html/", _flag_view(letters__render_editor_html_view)),
    path("panel/campaigns/campaigns/letter/_buttons-by-template/", _flag_view(letters__buttons_by_template_view)),
    path("panel/campaigns/campaigns/preview/modal-from-editor/", _flag_view(letters__preview_modal_from_editor_view)),
    path("admin/", admin.site.urls),
]
