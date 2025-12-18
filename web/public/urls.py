# FILE: web/public/urls.py
from django.urls import path, include

from .views import public_index

urlpatterns = [
    path("", public_index, name="public_index"),
    path("", include("public.aap_auth.urls")),  # ← ВАЖНО
]
