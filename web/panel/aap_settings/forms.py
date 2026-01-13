# FILE: web/panel/aap_settings/forms.py
# DATE: 2026-01-13
# PURPOSE: Form "Почтовые серверы".
# CHANGE:
# - IMAP поля НЕ обязательные и не “додавливаются” clean()
# - пароли не пропадают при перерисовке: PasswordInput(render_value=True)

from __future__ import annotations

from typing import List, Tuple

from django import forms
from django.utils.translation import gettext_lazy as _


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
    name = forms.CharField(
        label=_("Название"),
        required=True,
        widget=forms.TextInput(attrs={"class": "YY-INPUT", "placeholder": _("Для какой рассылки, компании?")}),
    )

    email = forms.EmailField(
        label=_("Email"),
        required=True,
        widget=forms.TextInput(attrs={"class": "YY-INPUT", "placeholder": "name@domain.tld"}),
    )

    preset_code = forms.ChoiceField(
        label=_("Пресет провайдера"),
        required=False,
        widget=forms.Select(attrs={"class": "YY-INPUT !mb-0"}),
    )

    smtp_host = forms.CharField(label="SMTP host", required=True, widget=forms.TextInput(attrs={"class": "YY-INPUT"}))
    smtp_port = forms.IntegerField(label="SMTP port", required=True, widget=forms.TextInput(attrs={"class": "YY-INPUT !w-24"}))
    smtp_security = forms.ChoiceField(label="SMTP security", choices=SECURITY_CHOICES, required=True, widget=forms.Select(attrs={"class": "YY-INPUT !px-1"}))
    smtp_auth_type = forms.ChoiceField(label="SMTP auth", choices=AUTH_CHOICES, required=True, widget=forms.Select(attrs={"class": "YY-INPUT !px-1"}))
    smtp_username = forms.CharField(label="SMTP username", required=True, widget=forms.TextInput(attrs={"class": "YY-INPUT"}))
    smtp_secret = forms.CharField(
        label="SMTP password / token",
        required=True,
        widget=forms.PasswordInput(attrs={"class": "YY-INPUT"}, render_value=True),
    )

    from_name = forms.CharField(
        label=_("Отправитель:"),
        required=False,
        widget=forms.TextInput(attrs={"class": "YY-INPUT", "placeholder": _("Отправитель")}),
    )

    imap_host = forms.CharField(label="IMAP host", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT"}))
    imap_port = forms.IntegerField(label="IMAP port", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT !w-24"}))
    imap_security = forms.ChoiceField(label="IMAP security", choices=SECURITY_CHOICES, required=False, widget=forms.Select(attrs={"class": "YY-INPUT !px-1"}))
    imap_auth_type = forms.ChoiceField(label="IMAP auth", choices=AUTH_CHOICES, required=False, widget=forms.Select(attrs={"class": "YY-INPUT !px-1"}))
    imap_username = forms.CharField(label="IMAP username", required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT"}))
    imap_secret = forms.CharField(
        label="IMAP password / token",
        required=False,
        widget=forms.PasswordInput(attrs={"class": "YY-INPUT"}, render_value=True),
    )

    def __init__(self, *args, preset_choices: List[Tuple[str, str]] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["preset_code"].choices = [("", "—")] + (preset_choices or [])

    def clean(self):
        cleaned = super().clean()

        missing = []
        for f in ["name", "email", "smtp_host", "smtp_port", "smtp_security", "smtp_auth_type", "smtp_username", "smtp_secret"]:
            if not (cleaned.get(f) or "").__str__().strip():
                missing.append(f)

        if missing:
            self.add_error(None, _("Заполните все поля."))
            for f in missing:
                self.add_error(f, "")

        return cleaned
