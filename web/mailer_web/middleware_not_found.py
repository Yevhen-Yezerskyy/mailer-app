# FILE: web/mailer_web/middleware_not_found.py
# DATE: 2026-03-07
# PURPOSE: return custom 404 page even when DEBUG=True.

from __future__ import annotations

from django.http import Http404
from django.shortcuts import render
from django.urls.exceptions import Resolver404


class ForceCustom404Middleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        try:
            response = self.get_response(request)
        except (Http404, Resolver404):
            return render(request, "404.html", status=404)
        if getattr(response, "status_code", None) == 404:
            return render(request, "404.html", status=404)
        return response
