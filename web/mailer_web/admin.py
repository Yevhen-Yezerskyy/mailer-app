# FILE: web/mailer_web/admin.py  (обновлено — 2026-02-10)
# CHANGE: Подключены кастомные админ-страницы через единый реестр (без правок при добавлении новых страниц).

from django.contrib import admin

from .models_accounts import FrontUser, UserWorkspace
from .admin_views import ADMIN_PAGES
from .admin_views.utils import register_admin_pages


@admin.register(FrontUser)
class FrontUserAdmin(admin.ModelAdmin):
    list_display = ("id", "user")
    search_fields = ("user__username", "user__email")


@admin.register(UserWorkspace)
class UserWorkspaceAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "workspace_id")
    search_fields = ("user__username", "user__email", "workspace_id")

register_admin_pages(admin.site, ADMIN_PAGES)
