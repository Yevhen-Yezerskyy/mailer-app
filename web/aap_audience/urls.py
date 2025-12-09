# FILE: web/aap_audience/urls.py

from django.urls import path
from django.shortcuts import redirect

# БЫЛО:
# from . import views

# СТАЛО: импортируем именно наш новый how_view из файла views/how.py
from .views.how import how_view
from .views.status import status_view
from .views.result import result_view

app_name = "audience"

urlpatterns = [
    path("", lambda r: redirect("audience:how"), name="index"),

    path("how/",    how_view,    name="how"),
    path("status/", status_view, name="status"),
    path("result/", result_view, name="result"),
]
