# FILE: web/public/views.py
# DATE: 2025-12-18
# PURPOSE: public index view — renders public/index.html

from django.shortcuts import render


def public_index(request):
    return render(request, "public/index.html")