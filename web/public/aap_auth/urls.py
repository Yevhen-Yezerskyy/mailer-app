# FILE: web/public/aap_auth/urls.py  (обновлено — 2025-12-18)
# PURPOSE: only custom login view

from django.urls import path
from .views import (
    login_view,
    login_error_view,
    register_view,
    email_pending_view,
    resend_email_confirm_view,
    confirm_email_view,
    edit_registration_view,
    password_reset_request_view,
    password_reset_done_view,
    password_reset_confirm_view,
)
from django.contrib.auth.views import LogoutView

urlpatterns = [
    path("login/", login_view, name="login"),
    path("login/error/<str:code>/<str:uid>/", login_error_view, name="login_error"),
    path("register/", register_view, name="register"),
    path("email-pending/<str:uid>/", email_pending_view, name="email_pending"),
    path("resend-email-confirm/", resend_email_confirm_view, name="resend_email_confirm"),
    path("confirm-email/<str:token>/", confirm_email_view, name="confirm_email"),
    path("edit-registration/", edit_registration_view, name="edit_registration"),
    path("password-reset/", password_reset_request_view, name="password_reset_request"),
    path("password-reset/done/", password_reset_done_view, name="password_reset_done"),
    path("password-reset/confirm/<str:token>/", password_reset_confirm_view, name="password_reset_confirm"),
    path("logout/", LogoutView.as_view(next_page="public_index"), name="logout"),
]
