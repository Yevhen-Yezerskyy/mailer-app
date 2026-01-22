# FILE: web/panel/aap_settings/forms.py
# DATE: 2026-01-22
# PURPOSE: Form "Почтовые серверы".
# CHANGE:
# - Убрано поле name (Mailbox.name удалён)
# - from_name сделан обязательным
# - Удалена проверка уникальности name в рамках workspace; осталась только уникальность email

from __future__ import annotations

from typing import List, Tuple

from django import forms
from django.utils.translation import gettext_lazy as _

from panel.aap_settings.models import Mailbox


SECURITY_CHOICES = [
    ("starttls", "STARTTLS"),
    ("ssl", "SSL / TLS"),
    ("none", "None"),
]

AUTH_CHOICES = [
    ("login", "LOGIN"),
    ("oauth2", "OAuth2"),
]


class MailServerForm(forms.Form):
    email = forms.EmailField(
        label=_("Email"),
        required=True,
        widget=forms.TextInput(attrs={"class": "YY-INPUT", "placeholder": "name@domain.tld"}),
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
            }
        ),
    )

    preset_code = forms.ChoiceField(
        label=_("Пресет провайдера"),
        required=False,
        widget=forms.Select(attrs={"class": "YY-INPUT !mb-0"}),
    )

    smtp_host = forms.CharField(label="SMTP host", required=True, widget=forms.TextInput(attrs={"class": "YY-INPUT"}))
    smtp_port = forms.IntegerField(
        label="SMTP port", required=True, widget=forms.TextInput(attrs={"class": "YY-INPUT !w-24"})
    )
    smtp_security = forms.ChoiceField(
        label="SMTP security", choices=SECURITY_CHOICES, required=True, widget=forms.Select(attrs={"class": "YY-INPUT !px-1"})
    )
    smtp_auth_type = forms.ChoiceField(
        label="SMTP auth", choices=AUTH_CHOICES, required=True, widget=forms.Select(attrs={"class": "YY-INPUT !px-1"})
    )
    smtp_username = forms.CharField(label="SMTP username", required=True, widget=forms.TextInput(attrs={"class": "YY-INPUT"}))
    smtp_secret = forms.CharField(
        label="SMTP password / token",
        required=True,
        widget=forms.PasswordInput(attrs={"class": "YY-INPUT", "autocomplete": "off"}, render_value=True),
    )

    from_name = forms.CharField(
        label=_("Отправитель:"),
        required=True,
        widget=forms.TextInput(attrs={"class": "YY-INPUT", "placeholder": _("Отправитель")}),
    )

    imap_host = forms.CharField(label="IMAP host", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT"}))
    imap_port = forms.IntegerField(label="IMAP port", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT !w-24"}))
    imap_security = forms.ChoiceField(
        label="IMAP security", choices=SECURITY_CHOICES, required=False, widget=forms.Select(attrs={"class": "YY-INPUT !px-1"})
    )
    imap_auth_type = forms.ChoiceField(
        label="IMAP auth", choices=AUTH_CHOICES, required=False, widget=forms.Select(attrs={"class": "YY-INPUT !px-1"})
    )
    imap_username = forms.CharField(label="IMAP username", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT"}))
    imap_secret = forms.CharField(
        label="IMAP password / token",
        required=False,
        widget=forms.PasswordInput(attrs={"class": "YY-INPUT", "autocomplete": "off"}, render_value=True),
    )

    def __init__(
        self,
        *args,
        preset_choices: List[Tuple[str, str]] | None = None,
        require_smtp_secret: bool = True,
        smtp_masked: bool = False,
        imap_masked: bool = False,
        workspace_id=None,
        mailbox_id: int | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.fields["preset_code"].choices = [("", "—")] + (preset_choices or [])

        self.require_smtp_secret = bool(require_smtp_secret)
        if not self.require_smtp_secret:
            self.fields["smtp_secret"].required = False

        if smtp_masked:
            self.fields["smtp_secret"].widget.attrs.update(
                {"readonly": "readonly", "data-yy-masked": "1", "oncopy": "return false;"}
            )

        if imap_masked:
            self.fields["imap_secret"].widget.attrs.update(
                {"readonly": "readonly", "data-yy-masked": "1", "oncopy": "return false;"}
            )

        self.workspace_id = workspace_id
        self.mailbox_id = int(mailbox_id) if mailbox_id is not None else None

    def clean(self):
        cleaned = super().clean()

        # apply_preset — БЕЗ валидации вообще
        if self.data.get("action") == "apply_preset":
            return cleaned

        missing = []
        required_fields = [
            "email",
            "limit_hour_sent",
            "smtp_host",
            "smtp_port",
            "smtp_security",
            "smtp_auth_type",
            "smtp_username",
            "from_name",
        ]
        if self.require_smtp_secret:
            required_fields.append("smtp_secret")

        for f in required_fields:
            v = cleaned.get(f)
            if v is None or (isinstance(v, str) and not v.strip()):
                missing.append(f)

        if missing:
            self.add_error(None, _("Заполните все поля."))
            return cleaned  # поля-ошибки НЕ ставим

        # limit_hour_sent — обязателен, 1..300 (ошибка только non-field)
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

        # email — глобально уникален (только non-field error)
        email = (cleaned.get("email") or "").strip().lower()
        if email:
            q = Mailbox.objects.filter(email=email)
            if self.mailbox_id is not None:
                q = q.exclude(id=self.mailbox_id)
            if q.exists():
                self.add_error(None, _("Этот Email уже используется."))
                return cleaned

        return cleaned
