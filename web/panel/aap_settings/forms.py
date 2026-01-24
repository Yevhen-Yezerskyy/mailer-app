# FILE: web/panel/aap_settings/forms.py
# DATE: 2026-01-24
# PURPOSE: Settings → Mail servers: формы для Mailbox / SMTP / IMAP + отдельная страница SMTP.
# CHANGE:
# - Сохранены существующие формы: MailboxAddForm / SmtpConnForm / ImapConnForm.
# - Добавлена SmtpServerForm: отдельная страница SMTP (LOGIN + OAuth2-заглушки) + пресеты (только host/port/security).
# - Убрано дублирование AUTH_CHOICES: берём choices из модели AuthType.

from __future__ import annotations

from typing import List, Tuple

from django import forms
from django.utils.translation import gettext_lazy as _

from panel.aap_settings.models import Mailbox, AuthType


SECURITY_CHOICES = [
    ("starttls", "STARTTLS"),
    ("ssl", "SSL / TLS"),
    ("none", "None"),
]

AUTH_CHOICES = AuthType.choices


class MailboxAddForm(forms.Form):
    email = forms.EmailField(
        label=_("Email"),
        required=True,
        widget=forms.TextInput(attrs={"class": "YY-INPUT", "placeholder": "name@domain.tld"}),
    )

    def __init__(self, *args, workspace_id=None, mailbox_id: int | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.workspace_id = workspace_id
        self.mailbox_id = int(mailbox_id) if mailbox_id is not None else None

    def clean(self):
        cleaned = super().clean()
        email = (cleaned.get("email") or "").strip().lower()
        if not email:
            self.add_error(None, _("Заполните Email."))
            return cleaned

        q = Mailbox.objects.filter(email=email)
        if self.mailbox_id is not None:
            q = q.exclude(id=self.mailbox_id)
        if q.exists():
            self.add_error(None, _("Этот Email уже используется."))
            return cleaned

        return cleaned


class SmtpConnForm(forms.Form):
    preset_code = forms.ChoiceField(
        label=_("Пресет провайдера"),
        required=False,
        widget=forms.Select(attrs={"class": "YY-INPUT !mb-0", "id": "yyPresetSelect"}),
    )

    auth_type = forms.ChoiceField(
        label=_("Auth"),
        choices=AUTH_CHOICES,
        required=True,
        widget=forms.Select(attrs={"class": "YY-INPUT !px-1", "id": "yyAuthType"}),
    )

    host = forms.CharField(label="SMTP host", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT", "id": "yyHost"}))
    port = forms.IntegerField(label="SMTP port", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT !w-28", "id": "yyPort"}))
    security = forms.ChoiceField(
        label="SMTP security",
        choices=SECURITY_CHOICES,
        required=False,
        widget=forms.Select(attrs={"class": "YY-INPUT !px-1", "id": "yySecurity"}),
    )
    username = forms.CharField(label="SMTP username", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT", "id": "yyUsername"}))
    secret = forms.CharField(
        label="SMTP password / token",
        required=False,
        widget=forms.PasswordInput(attrs={"class": "YY-INPUT", "autocomplete": "off", "id": "yySecret"}, render_value=True),
    )

    from_name = forms.CharField(
        label=_("Отправитель:"),
        required=False,
        widget=forms.TextInput(attrs={"class": "YY-INPUT", "placeholder": _("Отправитель"), "id": "yyFromName"}),
    )

    limit_hour_sent = forms.IntegerField(
        label=_("Лимит/час"),
        required=False,
        initial=50,
        widget=forms.TextInput(
            attrs={
                "class": "YY-INPUT",
                "inputmode": "numeric",
                "pattern": r"[0-9]*",
                "maxlength": "3",
                "min": "1",
                "max": "300",
                "autocomplete": "off",
                "id": "yyLimitHour",
            }
        ),
    )

    def __init__(
        self,
        *args,
        preset_choices: List[Tuple[str, str]] | None = None,
        require_secret: bool = True,
        secret_masked: bool = False,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.fields["preset_code"].choices = [("", "—")] + (preset_choices or [])

        self.require_secret = bool(require_secret)
        if not self.require_secret:
            self.fields["secret"].required = False

        if secret_masked:
            self.fields["secret"].widget.attrs.update({"readonly": "readonly", "data-yy-masked": "1", "oncopy": "return false;"})

    def clean(self):
        cleaned = super().clean()

        auth_type = (cleaned.get("auth_type") or "").strip()
        if auth_type in ("google_oauth2", "microsoft_oauth2"):
            return cleaned

        required = ["host", "port", "security", "username", "from_name", "limit_hour_sent"]
        if self.require_secret:
            required.append("secret")

        missing = []
        for f in required:
            v = cleaned.get(f)
            if v is None or (isinstance(v, str) and not v.strip()):
                missing.append(f)

        if missing:
            self.add_error(None, _("Заполните все поля SMTP."))
            return cleaned

        try:
            lim = int(cleaned.get("limit_hour_sent") or 0)
        except Exception:
            lim = 0
        if lim < 1:
            self.add_error(None, _("Лимит должен быть не меньше 1 письма в час."))
            return cleaned
        if lim > 300:
            self.add_error(None, _("Максимум 300 писем в час."))
            return cleaned

        return cleaned


class ImapConnForm(forms.Form):
    preset_code = forms.ChoiceField(
        label=_("Пресет провайдера"),
        required=False,
        widget=forms.Select(attrs={"class": "YY-INPUT !mb-0", "id": "yyPresetSelect"}),
    )

    auth_type = forms.ChoiceField(
        label=_("Auth"),
        choices=AUTH_CHOICES,
        required=True,
        widget=forms.Select(attrs={"class": "YY-INPUT !px-1", "id": "yyAuthType"}),
    )

    host = forms.CharField(label="IMAP host", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT", "id": "yyHost"}))
    port = forms.IntegerField(label="IMAP port", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT !w-28", "id": "yyPort"}))
    security = forms.ChoiceField(
        label="IMAP security",
        choices=SECURITY_CHOICES,
        required=False,
        widget=forms.Select(attrs={"class": "YY-INPUT !px-1", "id": "yySecurity"}),
    )
    username = forms.CharField(label="IMAP username", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT", "id": "yyUsername"}))
    secret = forms.CharField(
        label="IMAP password / token",
        required=False,
        widget=forms.PasswordInput(attrs={"class": "YY-INPUT", "autocomplete": "off", "id": "yySecret"}, render_value=True),
    )

    def __init__(
        self,
        *args,
        preset_choices: List[Tuple[str, str]] | None = None,
        require_secret: bool = False,
        secret_masked: bool = False,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.fields["preset_code"].choices = [("", "—")] + (preset_choices or [])

        self.require_secret = bool(require_secret)
        if not self.require_secret:
            self.fields["secret"].required = False

        if secret_masked:
            self.fields["secret"].widget.attrs.update({"readonly": "readonly", "data-yy-masked": "1", "oncopy": "return false;"})

    def clean(self):
        cleaned = super().clean()

        auth_type = (cleaned.get("auth_type") or "").strip()
        if auth_type in ("google_oauth2", "microsoft_oauth2"):
            return cleaned

        any_val = False
        for k in ("host", "port", "security", "username", "secret"):
            v = cleaned.get(k)
            if v is not None and str(v).strip():
                any_val = True
                break

        if not any_val:
            return cleaned

        missing = []
        required = ["host", "port", "security", "username"]
        if self.require_secret:
            required.append("secret")
        for f in required:
            v = cleaned.get(f)
            if v is None or (isinstance(v, str) and not v.strip()):
                missing.append(f)

        if missing:
            self.add_error(None, _("Заполните все поля IMAP."))
            return cleaned

        return cleaned


class SmtpServerForm(forms.Form):
    preset_code = forms.ChoiceField(
        label=_("Пресет провайдера"),
        required=False,
        widget=forms.Select(attrs={"class": "YY-INPUT !mb-0", "id": "yyPresetSelect"}),
    )

    auth_type = forms.ChoiceField(
        label="Auth",
        choices=AUTH_CHOICES,
        required=True,
        widget=forms.HiddenInput(attrs={"id": "yyAuthType"}),
    )

    sender_name = forms.CharField(
        label=_("Отправитель"),
        required=True,
        widget=forms.TextInput(attrs={"class": "YY-INPUT", "id": "yySenderName"}),
    )

    email = forms.EmailField(
        label=_("Email"),
        required=True,
        widget=forms.EmailInput(attrs={"class": "YY-INPUT", "autocomplete": "off", "id": "yyEmail"}),
    )

    limit_hour_sent = forms.IntegerField(
        label=_("Лимит/час"),
        required=True,
        initial=50,
        widget=forms.TextInput(
            attrs={
                "class": "YY-INPUT",
                "inputmode": "numeric",
                "pattern": r"[0-9]*",
                "maxlength": "3",
                "min": "1",
                "max": "300",
                "autocomplete": "off",
                "id": "yyLimitHour",
            }
        ),
    )

    host = forms.CharField(required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT", "id": "yyHost"}))
    port = forms.IntegerField(required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT !w-24", "id": "yyPort"}))
    security = forms.ChoiceField(
        choices=SECURITY_CHOICES,
        required=False,
        widget=forms.Select(attrs={"class": "YY-INPUT !px-1", "id": "yySecurity"}),
    )
    username = forms.CharField(required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT", "id": "yyUsername"}))
    password = forms.CharField(
        required=False,
        widget=forms.PasswordInput(attrs={"class": "YY-INPUT", "autocomplete": "off", "id": "yyPassword"}, render_value=True),
    )

    def __init__(
        self,
        *args,
        preset_choices: List[Tuple[str, str]] | None = None,
        require_password: bool = True,
        mailbox_email: str = "",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.fields["preset_code"].choices = [("", "—")] + (preset_choices or [])
        self.require_password = bool(require_password)
        self.mailbox_email = (mailbox_email or "").strip().lower()

    def clean(self):
        cleaned = super().clean()

        try:
            lim = int(cleaned.get("limit_hour_sent") or 0)
        except Exception:
            lim = 0
        if lim < 1:
            self.add_error(None, _("Лимит должен быть не меньше 1 письма в час."))
            return cleaned
        if lim > 300:
            self.add_error(None, _("Максимум 300 писем в час."))
            return cleaned

        auth_type = (cleaned.get("auth_type") or "").strip()
        if auth_type in ("google_oauth2", "microsoft_oauth2"):
            return cleaned

        required = ["email", "host", "port", "security", "sender_name", "limit_hour_sent"]
        if self.require_password:
            required.append("password")

        for f in required:
            v = cleaned.get(f)
            if v is None or (isinstance(v, str) and not v.strip()):
                self.add_error(None, _("Заполните все поля SMTP (LOGIN)."))
                return cleaned

        u = (cleaned.get("username") or "").strip()
        if not u:
            cleaned["username"] = (cleaned.get("email") or "").strip()

        if not (cleaned.get("username") or "").strip():
            self.add_error(None, _("Заполните логин SMTP."))
            return cleaned

        return cleaned
