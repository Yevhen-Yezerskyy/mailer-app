# accounts/admin.py
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.models import User

from .models import UserWorkspace


class UserWorkspaceInline(admin.StackedInline):
    model = UserWorkspace
    can_delete = True
    extra = 0


class CustomUserAdmin(UserAdmin):
    inlines = [UserWorkspaceInline]


admin.site.unregister(User)
admin.site.register(User, CustomUserAdmin)


@admin.register(UserWorkspace)
class UserWorkspaceAdmin(admin.ModelAdmin):
    list_display = ("user", "workspace_id")
    search_fields = ("user__username", "user__email", "workspace_id")
    list_filter = ()
