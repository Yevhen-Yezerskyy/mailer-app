# FILE: web/aap_audience/views/status.py   (новое — 2025-12-08)

from django.shortcuts import render

def status_view(request):
    """
    Страница статуса процесса подбора параметров.
    Позже здесь появится реальный статус пайплайна HOW → PREVIEW → JSON.
    """
    context = {
        "title": "Статус подбора"
    }
    return render(request, "panels/aap_audience/status.html", context)
