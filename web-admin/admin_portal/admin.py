# FILE: web-admin/admin_portal/admin.py
# DATE: 2026-02-22
# PURPOSE: Admin portal bootstrap; registers custom Serenity admin pages.

from django.contrib import admin

from mailer_web.admin_views import ADMIN_PAGES
from mailer_web.admin_views.utils import register_admin_pages


register_admin_pages(admin.site, ADMIN_PAGES)

admin.site.site_header = "Serenity Admin"
admin.site.site_title = "Serenity Admin"
admin.site.index_title = "Serenity Administration"

