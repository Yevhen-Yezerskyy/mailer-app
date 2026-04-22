# FILE: web/public/urls.py
from django.urls import path, include

from .views import (
    public_datenschutz,
    public_diag_echo,
    public_diag_fingerprint,
    public_diag_start,
    public_impressum,
    public_index,
    public_test,
)

urlpatterns = [
    path("_diag/start/", public_diag_start, name="public_diag_start"),
    path("_diag/echo/", public_diag_echo, name="public_diag_echo"),
    path("_diag/fingerprint/", public_diag_fingerprint, name="public_diag_fingerprint"),
    path("test/", public_test, name="public_test"),
    path("impressum/", public_impressum, name="public_impressum"),
    path("datenschutz/", public_datenschutz, name="public_datenschutz"),
    path("", public_index, name="public_index"),
    path("start/", public_index, name="public_start"),
    path("", include("public.aap_auth.urls")),  # ← ВАЖНО
]
