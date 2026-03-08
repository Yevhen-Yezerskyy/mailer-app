# FILE: web/public/urls.py
from django.urls import path, include

from .views import public_index

urlpatterns = [
    path("", public_index, name="public_index"),
    path("start/", public_index, name="public_start"),
    path("", include("public.aap_auth.urls")),  # ← ВАЖНО
]
