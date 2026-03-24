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
from .views.modal_create_task import modal_create_task_view
from .views.modal_edit_branch_rate import modal_edit_branch_rate_view
from .views.modal_edit_title import modal_edit_title_view
from .views.modal_insert_company import modal_insert_company_view
from .views.create_list import create_list_view
from .views.create_edit_buy import (
    create_edit_buy_view,
    create_edit_buy_product_view,
    create_edit_buy_company_view,
    create_edit_buy_geo_view,
    create_edit_buy_branches_cities_view,
    create_edit_buy_contacts_view,
    create_edit_buy_mailing_list_view,
)
from .views.create_edit_sell import (
    create_edit_sell_view,
    create_edit_sell_product_view,
    create_edit_sell_company_view,
    create_edit_sell_geo_view,
    create_edit_sell_branches_cities_view,
    create_edit_sell_contacts_view,
    create_edit_sell_mailing_list_view,
)

app_name = "audience"

urlpatterns = [
    path("", lambda r: redirect("audience:how"), name="index"),

    path("how/", how_view, name="how"),
    path("clar/", clar_view, name="clar"),
    path("clar/modal/", modal_clar_view, name="clar_modal"),

    path("status/", status_view, name="status"),
    path("status/task/", status_task_view, name="status_task"),
    path("status/task/modal/", modal_status_task_view, name="status_task_modal"),  # NEW
    path("create/modal/", modal_create_task_view, name="create_modal"),
    path("create/branch-rate/modal/", modal_edit_branch_rate_view, name="create_branch_rate_modal"),
    path("create/title/modal/", modal_edit_title_view, name="create_title_modal"),
    path("create/company/insert/modal/", modal_insert_company_view, name="create_company_insert_modal"),
    path("create/", create_list_view, name="create_list"),
    path("create/edit-buy/", create_edit_buy_view, name="create_edit_buy"),
    path("create/edit-buy/product/", create_edit_buy_product_view, name="create_edit_buy_product"),
    path("create/edit-buy/company/", create_edit_buy_company_view, name="create_edit_buy_company"),
    path("create/edit-buy/geo/", create_edit_buy_geo_view, name="create_edit_buy_geo"),
    path(
        "create/edit-buy/branches-cities/",
        create_edit_buy_branches_cities_view,
        name="create_edit_buy_branches_cities",
    ),
    path("create/edit-buy/contacts/", create_edit_buy_contacts_view, name="create_edit_buy_contacts"),
    path(
        "create/edit-buy/mailing-list/",
        create_edit_buy_mailing_list_view,
        name="create_edit_buy_mailing_list",
    ),
    path("create/edit-buy/<str:item_id>/", create_edit_buy_view, name="create_edit_buy_id"),
    path("create/edit-buy/<str:item_id>/product/", create_edit_buy_product_view, name="create_edit_buy_product_id"),
    path("create/edit-buy/<str:item_id>/company/", create_edit_buy_company_view, name="create_edit_buy_company_id"),
    path("create/edit-buy/<str:item_id>/geo/", create_edit_buy_geo_view, name="create_edit_buy_geo_id"),
    path(
        "create/edit-buy/<str:item_id>/branches-cities/",
        create_edit_buy_branches_cities_view,
        name="create_edit_buy_branches_cities_id",
    ),
    path(
        "create/edit-buy/<str:item_id>/contacts/",
        create_edit_buy_contacts_view,
        name="create_edit_buy_contacts_id",
    ),
    path(
        "create/edit-buy/<str:item_id>/mailing-list/",
        create_edit_buy_mailing_list_view,
        name="create_edit_buy_mailing_list_id",
    ),
    path("create/edit-sell/", create_edit_sell_view, name="create_edit_sell"),
    path("create/edit-sell/product/", create_edit_sell_product_view, name="create_edit_sell_product"),
    path("create/edit-sell/company/", create_edit_sell_company_view, name="create_edit_sell_company"),
    path("create/edit-sell/geo/", create_edit_sell_geo_view, name="create_edit_sell_geo"),
    path(
        "create/edit-sell/branches-cities/",
        create_edit_sell_branches_cities_view,
        name="create_edit_sell_branches_cities",
    ),
    path("create/edit-sell/contacts/", create_edit_sell_contacts_view, name="create_edit_sell_contacts"),
    path(
        "create/edit-sell/mailing-list/",
        create_edit_sell_mailing_list_view,
        name="create_edit_sell_mailing_list",
    ),
    path("create/edit-sell/<str:item_id>/", create_edit_sell_view, name="create_edit_sell_id"),
    path("create/edit-sell/<str:item_id>/product/", create_edit_sell_product_view, name="create_edit_sell_product_id"),
    path("create/edit-sell/<str:item_id>/company/", create_edit_sell_company_view, name="create_edit_sell_company_id"),
    path("create/edit-sell/<str:item_id>/geo/", create_edit_sell_geo_view, name="create_edit_sell_geo_id"),
    path(
        "create/edit-sell/<str:item_id>/branches-cities/",
        create_edit_sell_branches_cities_view,
        name="create_edit_sell_branches_cities_id",
    ),
    path(
        "create/edit-sell/<str:item_id>/contacts/",
        create_edit_sell_contacts_view,
        name="create_edit_sell_contacts_id",
    ),
    path(
        "create/edit-sell/<str:item_id>/mailing-list/",
        create_edit_sell_mailing_list_view,
        name="create_edit_sell_mailing_list_id",
    ),
]
