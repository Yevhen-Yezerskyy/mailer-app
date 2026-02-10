# FILE: web/mailer_web/admin_views/__init__.py  (обновлено — 2026-02-10)
# CHANGE: Добавлены страницы SYS (list/edit/new) в реестр кастомных админ-страниц.

from .utils import AdminPage

from .branches_11880 import page as branches_11880_page
from .branches_gs import page as branches_gs_page
from .branches_sys import page as branches_sys_page, page_edit as branches_sys_edit_page, page_new as branches_sys_new_page

ADMIN_PAGES: list[AdminPage] = [
    branches_11880_page,
    branches_gs_page,
    branches_sys_page,
    branches_sys_new_page,
    branches_sys_edit_page,
]