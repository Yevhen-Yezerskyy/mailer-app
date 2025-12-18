# FILE: web/panel/views.py  (обновлено — 2025-12-18)
# PURPOSE: dashboard панели рендерит старые шаблоны из зипа: panels/dashboard.html (+ access_denied)

from django.shortcuts import render


def dashboard(request):
    # auth уже закрывается middleware, но оставляем логику как в старом aap_console
    if not request.user.is_authenticated:
        return render(request, "public/login.html", status=401)

    if getattr(request, "workspace_id", None) is None:
        return render(request, "panels/access_denied.html")

    return render(
        request,
        "panels/dashboard.html",
        {
            "user_id": request.user.id,
            "username": request.user.username,
            "workspace_id": request.workspace_id,
        },
    )
