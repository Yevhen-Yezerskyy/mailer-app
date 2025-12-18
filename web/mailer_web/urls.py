# FILE: web/mailer_web/urls.py
# DATE: 2025-12-18
# CHANGE: public + panel split; public under i18n, panel without language prefix

from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from django.conf.urls.i18n import i18n_patterns

urlpatterns = [
    path("admin/", admin.site.urls),

    # language switch (cookie)
    path("i18n/", include("django.conf.urls.i18n")),

    # panel — только cookie, без /ru|de|uk
    path("panel/", include("panel.urls")),
]

# публичка — ВСЁ до логина
urlpatterns += i18n_patterns(
    path("", include("public.urls")),
)

if settings.DEBUG:
    urlpatterns += staticfiles_urlpatterns()
