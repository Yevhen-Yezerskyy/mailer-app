# FILE: web/public/aap_auth/urls.py  (обновлено — 2025-12-18)
# PURPOSE: only custom login view

from django.urls import path
from .views import login_view
from django.contrib.auth.views import LogoutView

urlpatterns = [
    path("login/", login_view, name="login"),
    path("logout/", LogoutView.as_view(next_page="public_index"), name="logout"),
]
