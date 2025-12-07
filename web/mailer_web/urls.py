# FILE: web/mailer_web/urls.py

from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

from aap_console.views import dashboard  # одна страница → остаётся здесь

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", include("accounts.urls")),

    # Консоль (одна страница — остаётся здесь)
    path("panel/", dashboard, name="dashboard"),

    # Настройки (несколько страниц → у аппа свой urls.py)
    path("panel/settings/", include("aap_settings.urls")),

    # Подбор адресатов
    path("panel/audience/", include("aap_audience.urls")), 
]

if settings.DEBUG:
    urlpatterns += staticfiles_urlpatterns()
