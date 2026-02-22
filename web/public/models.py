from django.contrib.auth.models import AbstractUser, Group, Permission
from django.db import models


class ClientUser(AbstractUser):
    groups = models.ManyToManyField(
        Group,
        blank=True,
        help_text="The groups this user belongs to. A user will get all permissions granted to each of their groups.",
        related_name="public_clientuser_set",
        related_query_name="public_clientuser",
        verbose_name="groups",
    )
    user_permissions = models.ManyToManyField(
        Permission,
        blank=True,
        help_text="Specific permissions for this user.",
        related_name="public_clientuser_set",
        related_query_name="public_clientuser",
        verbose_name="user permissions",
    )

    class Meta:
        db_table = "public_clientuser"
