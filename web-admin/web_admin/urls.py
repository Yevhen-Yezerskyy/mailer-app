# FILE: web-admin/web_admin/urls.py
# DATE: 2026-02-22
# PURPOSE: URL routing for standalone admin contour.

from django.contrib import admin
from django.urls import path


urlpatterns = [
    path("admin/", admin.site.urls),
]

