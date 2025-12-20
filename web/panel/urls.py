# FILE: web/panel/urls.py  (обновлено — 2025-12-19)
# PURPOSE: корень панели /panel/ редиректит на /panel/audience/

from django.urls import path, include
from django.views.generic import RedirectView

urlpatterns = [
    path("", RedirectView.as_view(url="audience/how/", permanent=False), name="dashboard"),
    path("audience/", include("panel.aap_audience.urls")),
    path("settings/", include("panel.aap_settings.urls")),
]