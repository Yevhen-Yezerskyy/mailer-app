from django.contrib.auth.base_user import AbstractBaseUser, BaseUserManager
from django.db import models
from django.utils import timezone
from uuid import uuid4


WORKSPACE_ACCESS_TYPES = {
    "full": "Full access",
    "test": "Test mode",
    "stat_only": "Stats only",
    "closed": "Closed",
    "super": "Super",
    "custom": "Custom",
}
WORKSPACE_ACCESS_TYPE_DEFAULT = "test"
WORKSPACE_BILLING_DAYS = (1, 5, 10, 15, 20, 25)
WORKSPACE_BILLING_DAY_DEFAULT = 1

CLIENT_USER_ROLES = {
    "main": "Main",
    "viewer": "Viewer",
}
CLIENT_USER_ROLE_DEFAULT = "main"

USER_ACTION_TYPES = {
    "email_confirm": "Email confirm",
    "password_reset": "Password reset",
}


class ClientUserManager(BaseUserManager):
    use_in_migrations = True

    def _normalize_login(self, email: str) -> str:
        return (email or "").strip().lower()

    def get_by_natural_key(self, email):
        login = self._normalize_login(email)
        return self.get(email__iexact=login)

    async def aget_by_natural_key(self, email):
        login = self._normalize_login(email)
        return await self.aget(email__iexact=login)

    def create_user(self, email, password=None, **extra_fields):
        email = self.normalize_email(email or "").strip().lower()
        if not email:
            raise ValueError("The email must be set")
        user = self.model(
            email=email,
            **extra_fields,
        )
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_superuser(self, email, password=None, **extra_fields):
        return self.create_user(email=email, password=password, **extra_fields)


class Workspace(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    company_name = models.CharField(max_length=255)
    company_address = models.TextField(blank=True, default="")
    company_phone = models.CharField(max_length=64, blank=True, default="")
    company_email = models.EmailField(blank=True, default="")
    access_type = models.CharField(
        max_length=32,
        choices=[(k, k) for k in WORKSPACE_ACCESS_TYPES.keys()],
        default=WORKSPACE_ACCESS_TYPE_DEFAULT,
        db_index=True,
    )
    billing_day = models.PositiveSmallIntegerField(
        choices=[(day, day) for day in WORKSPACE_BILLING_DAYS],
        default=WORKSPACE_BILLING_DAY_DEFAULT,
    )
    registration_date = models.DateTimeField(default=timezone.now)
    archived = models.BooleanField(default=False, db_index=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "accounts_workspaces"

    def __str__(self) -> str:
        company = (self.company_name or "").strip()
        return company or str(self.id)


class WorkspaceLimits(models.Model):
    workspace_id = models.UUIDField(null=True, blank=True, db_index=True)
    type = models.CharField(
        max_length=32,
        choices=[(k, k) for k in WORKSPACE_ACCESS_TYPES.keys()],
        null=True,
        blank=True,
        db_index=True,
    )
    sending_workspace_limit = models.IntegerField(null=True, blank=True)
    sending_task_limit = models.IntegerField(null=True, blank=True)
    active_tasks_limit = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = "accounts_workspace_limits"


class ClientUser(AbstractBaseUser):
    first_name = models.CharField(max_length=150, blank=True)
    last_name = models.CharField(max_length=150, blank=True)
    position = models.CharField(max_length=255, blank=True, default="")
    email = models.EmailField(unique=True)
    phone = models.CharField(max_length=64, blank=True, default="")
    email_confirmed = models.BooleanField(default=False, db_index=True)
    role = models.CharField(
        max_length=32,
        choices=[(k, k) for k in CLIENT_USER_ROLES.keys()],
        default=CLIENT_USER_ROLE_DEFAULT,
        db_index=True,
    )
    archived = models.BooleanField(default=False, db_index=True)
    workspace = models.ForeignKey(
        "mailer_web.Workspace",
        on_delete=models.PROTECT,
        related_name="users",
    )
    date_joined = models.DateTimeField(default=timezone.now)

    objects = ClientUserManager()

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = []

    class Meta:
        db_table = "accounts_clientuser"

    def get_full_name(self):
        full_name = f"{self.first_name} {self.last_name}".strip()
        return full_name

    def get_short_name(self):
        return self.first_name or self.email

    def __str__(self) -> str:
        return self.email


class UserActionToken(models.Model):
    user = models.ForeignKey(
        "mailer_web.ClientUser",
        on_delete=models.CASCADE,
        related_name="action_tokens",
    )
    action = models.CharField(
        max_length=32,
        choices=[(k, k) for k in USER_ACTION_TYPES.keys()],
        db_index=True,
    )
    token = models.CharField(max_length=128, unique=True)
    expires_at = models.DateTimeField(db_index=True)
    used_at = models.DateTimeField(null=True, blank=True, db_index=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    meta = models.JSONField(default=dict, blank=True)

    class Meta:
        db_table = "accounts_user_action_token"
        indexes = [
            models.Index(fields=["user", "action", "created_at"]),
            models.Index(fields=["action", "used_at", "expires_at"]),
        ]
