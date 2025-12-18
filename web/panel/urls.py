# FILE: web/panel/urls.py  (новое — 2025-12-18)
# PURPOSE: корень панели = dashboard, остальное — старые аппы (без console)

from django.urls import path, include
from .views import dashboard

urlpatterns = [
    path("", dashboard, name="dashboard"),          # /panel/
    path("audience/", include("panel.aap_audience.urls")),
    path("settings/", include("panel.aap_settings.urls")),
]
