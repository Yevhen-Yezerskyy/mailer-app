# FILE: web/panel/aap_campaigns/views/campaigns.py
# DATE: 2026-01-14
# PURPOSE: Заглушка страницы "Кампании".
# CHANGE: (new) базовый view, просто рендер шаблона.

from __future__ import annotations

from django.shortcuts import render


def campaigns_view(request):
    return render(request, "panels/aap_campaigns/campaigns.html", {})
