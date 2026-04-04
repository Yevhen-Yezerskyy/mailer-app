# FILE: web/panel/aap_audience/views/create_edit_sell.py
# DATE: 2026-03-21
# PURPOSE: Sell-step wrappers over shared create/edit flow engine.

from django.shortcuts import redirect

from .create_edit_flow import create_edit_flow_view


def create_edit_sell_view(request, item_id: str = ""):
    if item_id:
        return redirect("audience:create_edit_sell_product_id", item_id)
    return redirect("audience:create_edit_sell_product")


def create_edit_sell_product_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="sell", step_key="product", item_id=item_id)


def create_edit_sell_company_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="sell", step_key="company", item_id=item_id)


def create_edit_sell_geo_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="sell", step_key="geo", item_id=item_id)


def create_edit_sell_branches_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="sell", step_key="branches", item_id=item_id)


def create_edit_sell_cities_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="sell", step_key="cities", item_id=item_id)


def create_edit_sell_contacts_view(request, item_id: str = ""):
    return create_edit_flow_view(
        request,
        flow_type="sell",
        step_key="contacts",
        item_id=item_id,
        contacts_section="collect",
    )


def create_edit_sell_contacts_all_view(request, item_id: str = ""):
    return create_edit_flow_view(
        request,
        flow_type="sell",
        step_key="contacts",
        item_id=item_id,
        contacts_section="all",
    )


def create_edit_sell_contacts_branch_city_view(request, item_id: str = ""):
    return create_edit_flow_view(
        request,
        flow_type="sell",
        step_key="contacts",
        item_id=item_id,
        contacts_section="branch_city",
    )


def create_edit_sell_contacts_pairs_view(request, item_id: str = ""):
    return create_edit_flow_view(
        request,
        flow_type="sell",
        step_key="contacts",
        item_id=item_id,
        contacts_section="pairs",
    )


def create_edit_sell_mailing_list_view(request, item_id: str = ""):
    return create_edit_flow_view(request, flow_type="sell", step_key="mailing_list", item_id=item_id)
