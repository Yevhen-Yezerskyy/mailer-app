# FILE: web/panel/urls.py  (обновлено — 2025-12-21)
# PURPOSE: корень панели /panel/ редиректит на /panel/audience/
#          + включаем флаг request._tw_classmap_enabled для всего дерева panel-urls.

from __future__ import annotations

from importlib import import_module
from functools import wraps

from django.urls import path, include
from django.views.generic import RedirectView
from django.urls.resolvers import URLPattern, URLResolver


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
    # сохраним namespace как app_name (как и было)
    return include((patterns, app_name), namespace=app_name)


urlpatterns = [
    path("", RedirectView.as_view(url="audience/how/", permanent=False), name="dashboard"),

    path("audience/", include_flagged("panel.aap_audience.urls")),
    path("lists/", include_flagged("panel.aap_lists.urls")),
    path("settings/", include_flagged("panel.aap_settings.urls")),
]
