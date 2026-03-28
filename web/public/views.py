# FILE: web/public/views.py
# DATE: 2026-03-07
# PURPOSE: public index + custom 404 page.

from django.http import HttpResponse
from django.shortcuts import render


def public_index(request):
    return render(request, "public/index.html")


def error_404(request, exception):
    return render(request, "404.html", status=404)
