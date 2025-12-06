from django.shortcuts import render, redirect
from django.contrib.auth import login as auth_login
from django.contrib.auth.forms import UserCreationForm, AuthenticationForm
from django.contrib.auth.decorators import login_required

from .models import FrontUser


def landing(request):
    return render(request, "landing.html")


def register(request):
    if request.method == "POST":
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            FrontUser.objects.create(user=user)
            auth_login(request, user)
            return redirect("dashboard")
    else:
        form = UserCreationForm()

    return render(request, "register.html", {"form": form})


def login_view(request):
    if request.method == "POST":
        form = AuthenticationForm(request, data=request.POST)
        if form.is_valid():
            user = form.get_user()
            auth_login(request, user)
            return redirect("dashboard")
    else:
        form = AuthenticationForm(request)

    return render(request, "login.html", {"form": form})


def logout_confirm(request):
    return render(request, "logout_confirm.html")


def logout_done(request):
    return render(request, "logout_done.html")


@login_required
def dashboard(request):
    return render(request, "dashboard.html", {
        "user_id": request.user.id,
        "username": request.user.username,
        "workspace_id": request.workspace_id,
    })
