# FILE: web/panel/aap_campaigns/urls.py
# DATE: 2026-01-14
# PURPOSE: URL-ы раздела "Campaigns" панели (campaigns / templates).
# CHANGE: (new) базовые урлы + редирект корня на campaigns/.

from django.urls import path
from django.views.generic import RedirectView

from .views import campaigns, templates

app_name = "campaigns"

urlpatterns = [
    path("", RedirectView.as_view(url="campaigns/", permanent=False)),
    path("campaigns/", campaigns.campaigns_view, name="campaigns"),
    path("templates/", templates.templates_view, name="templates"),
]
