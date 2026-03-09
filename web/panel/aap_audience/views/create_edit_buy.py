# FILE: web/panel/aap_audience/views/create_edit_buy.py
# DATE: 2026-03-08
# PURPOSE: Placeholder create/edit page for buy mode.

from django.shortcuts import render


def create_edit_buy_view(request):
    return render(
        request,
        "panels/aap_audience/create_edit_buy.html",
        {
            "type": "buy",
            "is_placeholder": True,
        },
    )
