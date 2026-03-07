# FILE: web/mailer_web/admin.py
# DATE: 2026-03-07
# PURPOSE: Django admin registration for client users and company workspaces with safe password handling.

from django import forms
from django.contrib import admin

from .models_accounts import ClientUser, Workspace


class ClientUserCreationForm(forms.ModelForm):
    password1 = forms.CharField(label="Password", widget=forms.PasswordInput)
    password2 = forms.CharField(label="Password confirmation", widget=forms.PasswordInput)

    class Meta:
        model = ClientUser
        fields = ("workspace", "email", "first_name", "last_name", "position", "phone", "role")

    def clean_password2(self):
        password1 = self.cleaned_data.get("password1")
        password2 = self.cleaned_data.get("password2")
        if password1 != password2:
            raise forms.ValidationError("Passwords do not match.")
        return password2

    def save(self, commit=True):
        user = super().save(commit=False)
        user.set_password(self.cleaned_data["password1"])
        if commit:
            user.save()
        return user


class ClientUserChangeForm(forms.ModelForm):
    new_password1 = forms.CharField(
        label="New password",
        widget=forms.PasswordInput,
        required=False,
    )
    new_password2 = forms.CharField(
        label="New password confirmation",
        widget=forms.PasswordInput,
        required=False,
    )

    class Meta:
        model = ClientUser
        fields = ("workspace", "email", "first_name", "last_name", "position", "phone", "role")

    def clean(self):
        cleaned_data = super().clean()
        password1 = cleaned_data.get("new_password1")
        password2 = cleaned_data.get("new_password2")
        if password1 or password2:
            if password1 != password2:
                raise forms.ValidationError("New passwords do not match.")
        return cleaned_data

    def save(self, commit=True):
        user = super().save(commit=False)
        password = self.cleaned_data.get("new_password1")
        if password:
            user.set_password(password)
        if commit:
            user.save()
        return user


@admin.register(ClientUser)
class ClientUserAdmin(admin.ModelAdmin):
    form = ClientUserChangeForm
    add_form = ClientUserCreationForm

    list_display = ("id", "email", "workspace", "role", "phone", "date_joined")
    list_filter = ("role", "workspace")
    search_fields = ("email", "first_name", "last_name", "position", "phone", "workspace__company_name")
    ordering = ("id",)
    readonly_fields = ("date_joined",)

    def get_form(self, request, obj=None, **kwargs):
        kwargs["form"] = self.add_form if obj is None else self.form
        return super().get_form(request, obj, **kwargs)

    def get_fieldsets(self, request, obj=None):
        if obj is None:
            return (
                (
                    None,
                    {
                        "fields": (
                            "workspace",
                            "email",
                            "first_name",
                            "last_name",
                            "position",
                            "phone",
                            "role",
                            "password1",
                            "password2",
                        )
                    },
                ),
            )
        return (
            (
                None,
                {"fields": ("workspace", "email", "first_name", "last_name", "position", "phone", "role")},
            ),
            ("Password", {"fields": ("new_password1", "new_password2")}),
            ("Important dates", {"fields": ("date_joined",)}),
        )


@admin.register(Workspace)
class WorkspaceAdmin(admin.ModelAdmin):
    list_display = ("id", "company_name", "company_email", "company_phone", "access_type", "created_at")
    list_filter = ("access_type",)
    search_fields = ("id", "company_name", "company_email", "company_phone", "company_address")
    ordering = ("-created_at",)
