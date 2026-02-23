from uuid import uuid4

from django.contrib.auth.base_user import AbstractBaseUser, BaseUserManager
from django.contrib.auth.validators import UnicodeUsernameValidator
from django.db import models
from django.utils import timezone


class ClientUserManager(BaseUserManager):
    use_in_migrations = True

    def _normalize_login(self, username: str) -> str:
        return (username or "").strip()

    def get_by_natural_key(self, username):
        login = self._normalize_login(username)
        if "@" in login:
            try:
                return self.get(email__iexact=login)
            except self.model.DoesNotExist:
                pass
        return self.get(username__iexact=login)

    async def aget_by_natural_key(self, username):
        login = self._normalize_login(username)
        if "@" in login:
            try:
                return await self.aget(email__iexact=login)
            except self.model.DoesNotExist:
                pass
        return await self.aget(username__iexact=login)

    def create_user(self, username, email="", password=None, **extra_fields):
        if not username:
            raise ValueError("The username must be set")
        username = self.model.normalize_username(username)
        user = self.model(
            username=username,
            email=self.normalize_email(email),
            **extra_fields,
        )
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_superuser(self, username, email="", password=None, **extra_fields):
        return self.create_user(username=username, email=email, password=password, **extra_fields)


class ClientUser(AbstractBaseUser):
    username_validator = UnicodeUsernameValidator()

    username = models.CharField(
        max_length=150,
        unique=True,
        validators=[username_validator],
        error_messages={"unique": "A user with that username already exists."},
    )
    first_name = models.CharField(max_length=150, blank=True)
    last_name = models.CharField(max_length=150, blank=True)
    email = models.EmailField(blank=True)
    is_active = models.BooleanField(default=True)
    date_joined = models.DateTimeField(default=timezone.now)

    objects = ClientUserManager()

    USERNAME_FIELD = "username"
    REQUIRED_FIELDS = []

    class Meta:
        db_table = "accounts_clientuser"

    def get_full_name(self):
        full_name = f"{self.first_name} {self.last_name}".strip()
        return full_name

    def get_short_name(self):
        return self.first_name or self.username

    def __str__(self) -> str:
        return self.username


class UserWorkspace(models.Model):
    user = models.OneToOneField(
        "mailer_web.ClientUser",
        on_delete=models.CASCADE,
        related_name="workspace_link",
    )

    workspace_id = models.UUIDField(
        default=uuid4,
        db_index=True,
        editable=False,
    )

    class Meta:
        db_table = "accounts_userworkspace"

    def __str__(self) -> str:
        return f"{self.user.username} @ {self.workspace_id}"
