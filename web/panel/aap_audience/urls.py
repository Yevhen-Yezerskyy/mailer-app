# FILE: web/panel/aap_audience/urls.py  (обновлено — 2025-12-18)
# Смысл: urls audience перенесены под panel, логика и имена сохранены.

from django.urls import path
from django.shortcuts import redirect

from .views.how import how_view
from .views.clar import clar_view
from .views.modal_clar import modal_clar_view
from .views.status import status_view
from .views.status_task import status_task_view
from .views.result import result_view

app_name = "audience"

urlpatterns = [
    path("", lambda r: redirect("audience:how"), name="index"),

    path("how/", how_view, name="how"),
    path("clar/", clar_view, name="clar"),
    path("clar/modal/", modal_clar_view, name="clar_modal"),
    
    path("status/", status_view, name="status"),
    path("status/task-<int:task_id>/", status_task_view, name="status_task"),

    path("result/", result_view, name="result"),
]
