# FILE: web/aap_audience/urls.py

from django.urls import path
from django.shortcuts import redirect

from . import views

app_name = "audience"

urlpatterns = [
    path("", lambda r: redirect("audience:how"), name="index"),

    path("how/",    views.how_view,    name="how"),
    path("status/", views.status_view, name="status"),
    path("result/", views.result_view, name="result"),
]
