# FILE: web/aap_audience/urls.py  (новое) 2025-12-11

from django.urls import path
from django.shortcuts import redirect

from .views.how import how_view
from .views.status import status_view
from .views.result import result_view
from .views.clar import clar_view   # ← новый импорт

app_name = "audience"

urlpatterns = [
    path("", lambda r: redirect("audience:how"), name="index"),

    path("how/",    how_view,    name="how"),
    path("clar/",   clar_view,   name="clar"),    # ← новый пункт
    path("status/", status_view, name="status"),
    path("result/", result_view, name="result"),
]
