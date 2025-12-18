# FILE: web/aap_audience/views/result.py   (новое — 2025-12-08)

from django.shortcuts import render

def result_view(request):
    """
    Страница результата. Позже здесь появится:
    - предпросмотр бранчей
    - предпросмотр географии
    - итоговые JSON (branches.json и cities.json)
    Сейчас — заготовка.
    """
    context = {
        "title": "Результат подбора"
    }
    return render(request, "panels/aap_audience/result.html", context)
