# FILE: web/aap_audience/views.py

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

from .forms import AudienceHowForm
from common.gpt import GPTClient


@login_required
def how_view(request: HttpRequest) -> HttpResponse:
    """
    Страница «Как ищем?».

    Логика:
    - Есть три textarea:
        1) system
        2) user
        3) result
    - По сабмиту:
        - из первой textarea -> system
        - из второй textarea -> user
        - вызываем GPTClient (tier='nano', with_web=False)
        - ответ подставляем в третью textarea
        - первые две остаются как были
    - Никаких дебаг-блоков снизу.
    """
    if request.method == "POST":
        form = AudienceHowForm(request.POST)
        if form.is_valid():
            system_text = form.cleaned_data["system"]
            user_text = form.cleaned_data["user"]

            client = GPTClient()
            resp = client.ask(
                tier="nano",
                with_web=False,  # для nano веб-поиск запрещён
                service_tier="flex",
                workspace_id=getattr(request, "workspace_id", "audience"),
                user_id=request.user.id,
                system=system_text,
                user=user_text,
                endpoint="audience_how",
            )

            answer = resp.content or ""

            # Пересобираем форму с теми же system/user + полученным result
            form = AudienceHowForm(
                initial={
                    "system": system_text,
                    "user": user_text,
                    "result": answer,
                }
            )
    else:
        form = AudienceHowForm()

    return render(
        request,
        "panels/aap_audience/how.html",
        {
            "form": form,
        },
    )


@login_required
def status_view(request: HttpRequest) -> HttpResponse:
    """
    Статус процессов подбора аудитории (как было).
    """
    return render(request, "panels/aap_audience/status.html")


@login_required
def result_view(request: HttpRequest) -> HttpResponse:
    """
    Результаты подбора аудитории (как было).
    """
    return render(request, "panels/aap_audience/result.html")
