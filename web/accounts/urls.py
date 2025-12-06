from django.urls import path
from django.contrib.auth import views as auth_views

from . import views

urlpatterns = [
    path("", views.landing, name="landing"),

    path("register/", views.register, name="register"),
    path("login/", views.login_view, name="login"),

    path("dashboard/", views.dashboard, name="dashboard"),

    # страница подтверждения выхода
    path("logout/", views.logout_confirm, name="logout"),

    # реальный logout (POST), после него – страница "вы вышли"
    path(
        "logout/confirm/",
        auth_views.LogoutView.as_view(next_page="logout_done"),
        name="logout_do",
    ),

    path("logout/done/", views.logout_done, name="logout_done"),
]
