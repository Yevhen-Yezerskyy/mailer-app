# FILE: web/mailer_web/urls.py
# DATE: 2025-12-18
# CHANGE: public + panel split; public under i18n, panel without language prefix

from django.urls import path, include
from django.conf import settings
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from django.views.generic import RedirectView

urlpatterns = [
    # compatibility: old language roots -> start
    path("ru/", RedirectView.as_view(url="/start/", permanent=False)),
    path("de/", RedirectView.as_view(url="/start/", permanent=False)),
    path("uk/", RedirectView.as_view(url="/start/", permanent=False)),
    path("en/", RedirectView.as_view(url="/start/", permanent=False)),

    # language switch (cookie)
    path("i18n/", include("django.conf.urls.i18n")),

    # panel — только cookie, без /ru|de|uk
    path("panel/", include("panel.urls")),

    # public without language prefixes
    path("", include("public.urls")),
]

if settings.DEBUG:
    urlpatterns += staticfiles_urlpatterns()


handler404 = "public.views.error_404"
