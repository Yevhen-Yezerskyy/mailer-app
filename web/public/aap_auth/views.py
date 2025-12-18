# FILE: web/public/aap_auth/views.py  (обновлено — 2025-12-18)
# Смысл: только логин. Без регистрации, логаута, дашбордов. После логина — редирект в панель.

from django.shortcuts import render, redirect
from django.contrib.auth import login as auth_login
from django.contrib.auth.forms import AuthenticationForm


def login_view(request):
    if request.method == "POST":
        form = AuthenticationForm(request, data=request.POST)
        if form.is_valid():
            auth_login(request, form.get_user())
            return redirect("/panel/")
    else:
        form = AuthenticationForm(request)

    return render(request, "public/login.html", {"form": form})
