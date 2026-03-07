# FILE: web/mailer_web/middleware.py  (обновлено — 2026-03-07)
# CHANGE: workspace берём напрямую из request.user.workspace_id.

from django.shortcuts import redirect
from django.urls import reverse


class WorkspaceMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        request.workspace_id = None

        # anon в панель нельзя
        if request.path.startswith("/panel/") and not request.user.is_authenticated:
            return redirect(reverse("login"))

        if request.user.is_authenticated:
            ws_id = getattr(request.user, "workspace_id", None)
            if ws_id:
                request.workspace_id = ws_id
                request.session["workspace_id"] = str(ws_id)
            else:
                request.session.pop("workspace_id", None)

                dashboard_url = reverse("dashboard")  # "/panel/"

                if request.path.startswith("/panel/") and request.path != dashboard_url:
                    return redirect(dashboard_url)

        return self.get_response(request)
