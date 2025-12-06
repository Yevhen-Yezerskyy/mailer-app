# FILE: web/mailer_web/urls.py

from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

from aap_console.views import dashboard  # 👈 импортируем сразу view

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", include("accounts.urls")),
    path("panel/", dashboard, name="dashboard"),  # 👈 панель живёт здесь
]

if settings.DEBUG:
    urlpatterns += staticfiles_urlpatterns()
