from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("aap_audience", "0007_audiencetask_ready"),
    ]

    operations = [
        migrations.AddField(
            model_name="audiencetask",
            name="rating_branch_hash",
            field=models.BigIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="audiencetask",
            name="rating_city_hash",
            field=models.BigIntegerField(blank=True, null=True),
        ),
    ]
