# FILE: web/panel/urls.py  (обновлено — 2026-02-06)
# PURPOSE: /panel/ редирект на /panel/overview/; /panel/overview/ рендерит dashboard (таблица stats).

from __future__ import annotations

from importlib import import_module
from functools import wraps

from django.urls import path, include
from django.views.generic import RedirectView
from django.urls.resolvers import URLPattern, URLResolver

from panel.views import (
    dashboard,
    overview_live_stats,
    stats_view,
    stats_clicks_view,
    stats_sending_view,
    switch_user_modal_view,
    switch_user_login_view,
    contact_modal_view,
)


_FLAG_ATTR = "_tw_classmap_enabled"


def _flag_view(view_func):
    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        setattr(request, _FLAG_ATTR, True)
        return view_func(request, *args, **kwargs)

    return _wrapped


def _flag_urlpatterns(urlpatterns):
    out = []
    for p in urlpatterns:
        if isinstance(p, URLPattern):
            out.append(URLPattern(p.pattern, _flag_view(p.callback), p.default_args, p.name))
        elif isinstance(p, URLResolver):
            out.append(URLResolver(p.pattern, _flag_urlpatterns(p.url_patterns), p.default_kwargs, p.app_name, p.namespace))
        else:
            out.append(p)
    return out


def include_flagged(module_path: str):
    mod = import_module(module_path)
    patterns = _flag_urlpatterns(getattr(mod, "urlpatterns", []))
    app_name = getattr(mod, "app_name", None)
    return include((patterns, app_name), namespace=app_name)


urlpatterns = [
    path("", RedirectView.as_view(url="overview/", permanent=False), name="dashboard"),
    path("overview/", _flag_view(dashboard), name="overview"),
    path("overview/live-stats/", overview_live_stats, name="overview_live_stats"),
    path("stats/", _flag_view(stats_view), name="stats"),
    path("stats/clicks/", _flag_view(stats_clicks_view), name="stats_clicks"),
    path("stats/sending/", _flag_view(stats_sending_view), name="stats_sending"),
    path("switch-user/modal/", _flag_view(switch_user_modal_view), name="switch_user_modal"),
    path("switch-user/login/", _flag_view(switch_user_login_view), name="switch_user_login"),
    path("contact/modal/", _flag_view(contact_modal_view), name="contact_modal"),

    path("audience/", include_flagged("panel.aap_audience.urls")),
    path("settings/", include_flagged("panel.aap_settings.urls")),
    path("campaigns/", include_flagged("panel.aap_campaigns.urls")),
]
