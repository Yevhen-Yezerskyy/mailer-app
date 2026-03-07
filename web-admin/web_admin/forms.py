# FILE: web-admin/web_admin/forms.py
# DATE: 2026-03-07
# PURPOSE: Forms for editing companies (workspaces) and client users in custom admin panel.

from django import forms

from mailer_web.models import MailLetter
from mailer_web.models_accounts import ClientUser, Workspace


class WorkspaceForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _apply_panel_styles(self)
        self.fields["company_name"].required = True

    class Meta:
        model = Workspace
        fields = [
            "company_name",
            "company_address",
            "company_phone",
            "company_email",
            "access_type",
        ]
        labels = {
            "company_name": "Название компании",
            "company_address": "Адрес компании",
            "company_phone": "Телефон компании",
            "company_email": "Электронная почта компании",
            "access_type": "Вид доступа",
        }
        widgets = {
            "company_address": forms.Textarea(attrs={"rows": 4}),
        }


class ClientUserForm(forms.ModelForm):
    new_password1 = forms.CharField(
        label="Новый пароль",
        required=False,
        widget=forms.PasswordInput,
    )
    new_password2 = forms.CharField(
        label="Повторите новый пароль",
        required=False,
        widget=forms.PasswordInput,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _apply_panel_styles(self)
        self.fields["workspace"].queryset = Workspace.objects.filter(archived=False).order_by("company_name", "created_at")
        self.fields["email"].required = True
        if not (self.instance and self.instance.pk):
            self.fields["new_password1"].required = True
            self.fields["new_password2"].required = True

    class Meta:
        model = ClientUser
        fields = [
            "workspace",
            "first_name",
            "last_name",
            "position",
            "email",
            "phone",
            "role",
        ]
        labels = {
            "workspace": "Компания",
            "first_name": "Имя",
            "last_name": "Фамилия",
            "position": "Должность",
            "email": "Мыло / логин",
            "phone": "Телефон",
            "role": "Роль",
        }

    def clean(self):
        cleaned = super().clean()
        email = (cleaned.get("email") or "").strip().lower()

        if not email:
            self.add_error("email", "Укажите электронную почту.")
            return cleaned
        cleaned["email"] = email

        p1 = cleaned.get("new_password1") or ""
        p2 = cleaned.get("new_password2") or ""
        if p1 or p2:
            if p1 != p2:
                self.add_error("new_password2", "Пароли не совпадают.")
        elif not (self.instance and self.instance.pk):
            self.add_error("new_password1", "Укажите пароль.")
            self.add_error("new_password2", "Повторите пароль.")

        return cleaned

    def save(self, commit=True):
        user = super().save(commit=False)
        password = (self.cleaned_data.get("new_password1") or "").strip()
        if password:
            user.set_password(password)
        if commit:
            user.save()
        return user


class MailLetterForm(forms.ModelForm):
    class Meta:
        model = MailLetter
        fields = [
            "name",
            "slug",
        ]
        labels = {
            "name": "Название письма",
            "slug": "Слаг",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _apply_panel_styles(self)


def _apply_panel_styles(form: forms.ModelForm) -> None:
    for name, field in form.fields.items():
        widget = field.widget
        attrs = dict(widget.attrs or {})

        if isinstance(widget, forms.CheckboxInput):
            attrs.setdefault("class", "h-5 w-5 align-middle accent-[#007c09]")
        elif isinstance(widget, forms.Textarea):
            attrs.setdefault("class", "YY-TEXTAREA")
        else:
            attrs.setdefault("class", "YY-INPUT")

        if name in ("company_name", "first_name", "last_name", "position"):
            attrs.setdefault("maxlength", "255")

        widget.attrs = attrs
