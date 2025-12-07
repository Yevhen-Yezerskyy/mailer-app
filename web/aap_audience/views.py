# FILE: web/aap_audience/views.py

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render


@login_required
def how_view(request: HttpRequest) -> HttpResponse:
    """
    Страница настройки логики поиска и фильтров.
    """
    return render(request, "panels/aap_audience/how.html")


@login_required
def status_view(request: HttpRequest) -> HttpResponse:
    """
    Статус процессов подбора аудитории (очереди, задачи, ошибки).
    """
    return render(request, "panels/aap_audience/status.html")


@login_required
def result_view(request: HttpRequest) -> HttpResponse:
    """
    Результаты: списки найденных адресатов, статистика, экспорт.
    """
    return render(request, "panels/aap_audience/result.html")
