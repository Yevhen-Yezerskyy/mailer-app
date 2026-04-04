# FILE: web/public/aap_auth/views.py
# DATE: 2026-03-07
# PURPOSE: public auth flow: login/register + unified email-pending page + email confirm.

from django.contrib import messages
from django.contrib.auth import login as auth_login
from django.contrib.auth.forms import AuthenticationForm
from django.db import transaction
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext as _

from mailer_web.access import decode_id, encode_id
from mailer_web.letter_sender import LetterSenderError, send_letter_by_slug
from mailer_web.models import ClientUser, Workspace

from .forms import (
    RegistrationForm,
    PasswordResetRequestForm,
    PasswordResetConfirmForm,
)
from .token_service import (
    ACTION_EMAIL_CONFIRM,
    ACTION_PASSWORD_RESET,
    consume_token,
    get_active_token,
    issue_token,
)


_LOGIN_ERROR_MESSAGES = {
    "login_blocked": _("Логин заблокирован"),
    "login_retired": _("Логин больше не обслуживается"),
    "invalid_credentials": _("Неверный логин или пароль"),
}


def _next_workspace_billing_day() -> int:
    today = timezone.localdate()
    billing_days = tuple(int(day) for day, _ in Workspace._meta.get_field("billing_day").choices)

    for day in billing_days:
        if day > today.day:
            return day

    return billing_days[0]


def _redirect_login_error(code: str, user: ClientUser | None):
    if user is None:
        return redirect("login")
    uid = encode_id(int(user.id))
    return redirect("login_error", code=code, uid=uid)


def _resolve_user_by_masked_uid(uid: str) -> ClientUser | None:
    try:
        user_id = int(decode_id(uid))
    except Exception:
        return None
    return ClientUser.objects.filter(id=user_id).select_related("workspace").first()


def _send_email_confirm(request, user: ClientUser) -> bool:
    token_obj = issue_token(user=user, action=ACTION_EMAIL_CONFIRM, ttl_hours=48)
    confirm_link = request.build_absolute_uri(
        reverse("confirm_email", kwargs={"token": token_obj.token})
    )
    workspace = user.workspace
    context = {
        "company_name": (workspace.company_name if workspace else "") or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "email": user.email or "",
        "link": confirm_link,
    }
    try:
        send_letter_by_slug(
            slug="email_confirm",
            to_email=user.email,
            lang=(getattr(request, "LANGUAGE_CODE", "") or "de"),
            context=context,
        )
        return True
    except LetterSenderError:
        return False
    except Exception:
        return False


def _send_password_reset(request, user: ClientUser) -> bool:
    token_obj = issue_token(user=user, action=ACTION_PASSWORD_RESET, ttl_hours=24)
    reset_link = request.build_absolute_uri(
        reverse("password_reset_confirm", kwargs={"token": token_obj.token})
    )
    workspace = user.workspace
    context = {
        "company_name": (workspace.company_name if workspace else "") or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "email": user.email or "",
        "link": reset_link,
    }
    try:
        send_letter_by_slug(
            slug="password_reset",
            to_email=user.email,
            lang=(getattr(request, "LANGUAGE_CODE", "") or "de"),
            context=context,
        )
        return True
    except LetterSenderError:
        return False
    except Exception:
        return False


def login_view(request):
    auth_error = ""
    auth_error_code = ""

    if request.method == "POST":
        form = AuthenticationForm(request, data=request.POST)
        if form.is_valid():
            user = form.get_user()
            workspace = user.workspace

            if workspace and workspace.access_type == "closed":
                return _redirect_login_error("login_blocked", user)
            if (workspace and workspace.archived) or user.archived:
                return _redirect_login_error("login_retired", user)
            if not user.email_confirmed:
                return redirect("email_pending", uid=encode_id(int(user.id)))

            auth_login(request, user)
            return redirect("/panel/")

        username = (request.POST.get("username") or "").strip().lower()
        login_user = ClientUser.objects.filter(email__iexact=username).select_related("workspace").first()
        if login_user:
            workspace = login_user.workspace
            if workspace and workspace.access_type == "closed":
                return _redirect_login_error("login_blocked", login_user)
            if (workspace and workspace.archived) or login_user.archived:
                return _redirect_login_error("login_retired", login_user)
            if not login_user.email_confirmed:
                return redirect("email_pending", uid=encode_id(int(login_user.id)))

        auth_error = _LOGIN_ERROR_MESSAGES["invalid_credentials"]
        auth_error_code = "invalid_credentials"
    else:
        form = AuthenticationForm(request)

    form.fields["username"].label = _("Email")
    return render(
        request,
        "public/login.html",
        {
            "form": form,
            "auth_error": auth_error,
            "auth_error_code": auth_error_code,
        },
    )


def login_error_view(request, code: str, uid: str):
    user = _resolve_user_by_masked_uid(uid)
    if not user:
        return redirect("login")
    if code == "email_unconfirmed":
        return redirect("email_pending", uid=uid)

    auth_error_code = code if code in _LOGIN_ERROR_MESSAGES else "invalid_credentials"
    auth_error = _LOGIN_ERROR_MESSAGES[auth_error_code]

    form = AuthenticationForm(request)
    form.fields["username"].label = _("Email")
    return render(
        request,
        "public/login.html",
        {
            "form": form,
            "auth_error": auth_error,
            "auth_error_code": auth_error_code,
        },
    )


def register_view(request):
    if request.method == "POST":
        form = RegistrationForm(request.POST)
        if form.is_valid():
            with transaction.atomic():
                registration_date = timezone.now()
                workspace = Workspace.objects.create(
                    company_name=form.cleaned_data["company_name"].strip(),
                    company_address=form.cleaned_data["company_address"].strip(),
                    access_type="test",
                    registration_date=registration_date,
                    billing_day=_next_workspace_billing_day(),
                )
                user = ClientUser.objects.create_user(
                    email=form.cleaned_data["email"],
                    password=form.cleaned_data["password"],
                    first_name=form.cleaned_data["first_name"].strip(),
                    last_name=form.cleaned_data["last_name"].strip(),
                    phone=form.cleaned_data["phone"].strip(),
                    position=form.cleaned_data.get("position", "").strip(),
                    email_confirmed=False,
                    workspace=workspace,
                    role="main",
                )
            return redirect("email_pending", uid=encode_id(int(user.id)))
    else:
        form = RegistrationForm()

    return render(request, "public/register.html", {"form": form})


def email_pending_view(request, uid: str):
    user = _resolve_user_by_masked_uid(uid)
    if not user:
        return redirect("login")
    if user.email_confirmed:
        return redirect("login")
    return render(
        request,
        "public/register_email_pending.html",
        {
            "user": user,
            "user_uid": uid,
            "email_sent": request.GET.get("sent") == "1",
        },
    )


def resend_email_confirm_view(request):
    if request.method == "POST":
        uid = (request.POST.get("uid") or "").strip()
        user = _resolve_user_by_masked_uid(uid)
        if user and not user.email_confirmed:
            if _send_email_confirm(request, user):
                return redirect(f"{reverse('email_pending', kwargs={'uid': uid})}?sent=1")
            else:
                messages.error(request, _("Не удалось отправить письмо"))
            return redirect("email_pending", uid=uid)
    return redirect("login")


def confirm_email_view(request, token: str):
    token_obj = get_active_token(token=token, action=ACTION_EMAIL_CONFIRM)
    if not token_obj:
        messages.error(request, _("Ссылка подтверждения недействительна или устарела"))
        return redirect("login")

    user = token_obj.user
    if not user.email_confirmed:
        user.email_confirmed = True
        user.save(update_fields=["email_confirmed"])
    consume_token(token_obj)
    workspace = user.workspace
    full_name = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip()
    return render(
        request,
        "public/confirm_email_done.html",
        {
            "company_name": (workspace.company_name if workspace else "") or "",
            "full_name": full_name or (user.email or ""),
            "email": user.email or "",
        },
    )


def edit_registration_view(request):
    uid = (request.GET.get("uid") or request.POST.get("uid") or "").strip()
    user = _resolve_user_by_masked_uid(uid)
    if not user or user.email_confirmed:
        return redirect("login")

    workspace = user.workspace
    if not workspace:
        messages.error(request, _("Рабочее пространство не найдено"))
        return redirect("email_pending", uid=uid)

    if request.method == "POST":
        form = RegistrationForm(
            request.POST,
            existing_user=user,
            is_edit=True,
        )
        if form.is_valid():
            with transaction.atomic():
                workspace.company_name = form.cleaned_data["company_name"].strip()
                workspace.company_address = form.cleaned_data["company_address"].strip()
                workspace.save(update_fields=["company_name", "company_address", "updated_at"])

                user.first_name = form.cleaned_data["first_name"].strip()
                user.last_name = form.cleaned_data["last_name"].strip()
                user.phone = form.cleaned_data["phone"].strip()
                user.position = form.cleaned_data.get("position", "").strip()
                user.email = form.cleaned_data["email"]
                if form.cleaned_data.get("password"):
                    user.set_password(form.cleaned_data["password"])
                user.save()
            messages.info(request, _("Данные обновлены"))
            return redirect("email_pending", uid=uid)
    else:
        form = RegistrationForm(
            initial={
                "company_name": workspace.company_name or "",
                "company_address": workspace.company_address or "",
                "last_name": user.last_name or "",
                "first_name": user.first_name or "",
                "phone": user.phone or "",
                "position": user.position or "",
                "email": user.email or "",
            },
            existing_user=user,
            is_edit=True,
        )

    return render(
        request,
        "public/register.html",
        {
            "form": form,
            "edit_uid": uid,
        },
    )


def password_reset_request_view(request):
    sent = False
    if request.method == "POST":
        form = PasswordResetRequestForm(request.POST)
        if form.is_valid():
            email = (form.cleaned_data.get("email") or "").strip().lower()
            user = (
                ClientUser.objects.filter(email__iexact=email)
                .select_related("workspace")
                .first()
            )
            if user and not user.archived:
                _send_password_reset(request, user)
            return redirect("password_reset_done")
    else:
        form = PasswordResetRequestForm()
    return render(
        request,
        "public/password_reset_request.html",
        {"form": form, "sent": sent},
    )


def password_reset_done_view(request):
    return render(request, "public/password_reset_done.html")


def password_reset_confirm_view(request, token: str):
    token_obj = get_active_token(token=token, action=ACTION_PASSWORD_RESET)
    if not token_obj:
        return render(
            request,
            "public/password_reset_confirm.html",
            {
                "form": None,
                "token_invalid": True,
                "saved": False,
                "company_name": "",
                "full_name": "",
                "email": "",
            },
        )

    user = token_obj.user
    workspace = user.workspace
    full_name = f"{(user.last_name or '').strip()} {(user.first_name or '').strip()}".strip()
    base_ctx = {
        "company_name": (workspace.company_name if workspace else "") or "",
        "full_name": full_name or (user.email or ""),
        "email": user.email or "",
    }
    if request.method == "POST":
        form = PasswordResetConfirmForm(request.POST)
        if form.is_valid():
            user.set_password(form.cleaned_data["password"])
            user.save(update_fields=["password"])
            consume_token(token_obj)
            return render(request, "public/password_reset_confirm.html", {"form": None, "token_invalid": False, "saved": True, **base_ctx})
    else:
        form = PasswordResetConfirmForm()

    return render(
        request,
        "public/password_reset_confirm.html",
        {"form": form, "token_invalid": False, "saved": False, **base_ctx},
    )
