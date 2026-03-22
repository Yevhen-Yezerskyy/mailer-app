# FILE: web/panel/aap_audience/views/create_edit_buy.py
# DATE: 2026-03-21
# PURPOSE: Buy-step wrappers over shared create/edit flow engine.

from django.shortcuts import redirect

from .create_edit_flow import create_edit_flow_view


def create_edit_buy_view(request, item_id: str = ""):
    if item_id:
        return redirect("audience:create_edit_buy_product_id", item_id)
    return redirect("audience:create_edit_buy_product")


def create_edit_buy_product_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="buy", step_key="product", item_id=item_id)


def create_edit_buy_company_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="buy", step_key="company", item_id=item_id)


def create_edit_buy_geo_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="buy", step_key="geo", item_id=item_id)


def create_edit_buy_branches_cities_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="buy", step_key="branches_cities", item_id=item_id)


def create_edit_buy_contacts_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="buy", step_key="contacts", item_id=item_id)


def create_edit_buy_mailing_list_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="buy", step_key="mailing_list", item_id=item_id)
