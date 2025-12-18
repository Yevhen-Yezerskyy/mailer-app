# FILE: web/mailer_web/middleware.py  (обновлено — 2025-12-18)
# Fix: закрываем /panel/* для anon (редирект на login), импорт UserWorkspace из mailer_web.models_accounts.

from django.shortcuts import redirect
from django.urls import reverse

from mailer_web.models_accounts import UserWorkspace


class WorkspaceMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        request.workspace_id = None

        # anon в панель нельзя
        if request.path.startswith("/panel/") and not request.user.is_authenticated:
            return redirect(reverse("login"))

        if request.user.is_authenticated:
            try:
                ws = UserWorkspace.objects.get(user=request.user)
                request.workspace_id = ws.workspace_id
                request.session["workspace_id"] = str(ws.workspace_id)
            except UserWorkspace.DoesNotExist:
                request.session.pop("workspace_id", None)

                dashboard_url = reverse("dashboard")  # "/panel/"

                if request.path.startswith("/panel/") and request.path != dashboard_url:
                    return redirect(dashboard_url)

        return self.get_response(request)
