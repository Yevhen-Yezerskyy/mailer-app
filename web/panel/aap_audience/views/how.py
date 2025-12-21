# FILE: web/panel/aap_audience/views/how.py
# DATE: 2025-12-21
# PURPOSE: пустая HOW-вью, без логики, старт с нуля

from django.shortcuts import render


def how_view(request):
    return render(request, "panels/aap_audience/how.html")
