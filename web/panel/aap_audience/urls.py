# FILE: web/panel/aap_audience/urls.py
# DATE: 2026-01-02

from django.urls import path
from django.shortcuts import redirect

from .views.how import how_view
from .views.clar import clar_view
from .views.modal_clar import modal_clar_view
from .views.status import status_view
from .views.status_task import status_task_view
from .views.modal_status_task import modal_status_task_view  # NEW
from .views.create_list import create_list_view
from .views.create_edit_buy import create_edit_buy_view
from .views.create_edit_sell import create_edit_sell_view

app_name = "audience"

urlpatterns = [
    path("", lambda r: redirect("audience:how"), name="index"),

    path("how/", how_view, name="how"),
    path("clar/", clar_view, name="clar"),
    path("clar/modal/", modal_clar_view, name="clar_modal"),

    path("status/", status_view, name="status"),
    path("status/task/", status_task_view, name="status_task"),
    path("status/task/modal/", modal_status_task_view, name="status_task_modal"),  # NEW
    path("create/", create_list_view, name="create_list"),
    path("create/edit-buy/", create_edit_buy_view, name="create_edit_buy"),
    path("create/edit-sell/", create_edit_sell_view, name="create_edit_sell"),
    path("create/edit-sell/<str:item_id>/", create_edit_sell_view, name="create_edit_sell_id"),
]
