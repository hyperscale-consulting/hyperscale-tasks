import datetime
import uuid
from typing import Any

from django.db import models
from django.utils import timezone


class OutboxMessage(models.Model):
    """A task waiting to be sent to SQS, written in the caller's transaction."""

    id: models.UUIDField[uuid.UUID, uuid.UUID] = models.UUIDField(
        primary_key=True, default=uuid.uuid4, editable=False
    )
    task_path: models.CharField[str, str] = models.CharField(max_length=255)
    args: models.JSONField[Any, Any] = models.JSONField()
    kwargs: models.JSONField[Any, Any] = models.JSONField()
    run_after: models.DateTimeField[
        datetime.datetime | None, datetime.datetime | None
    ] = models.DateTimeField(null=True)
    retry_for_seconds: models.PositiveIntegerField[int, int] = (
        models.PositiveIntegerField()
    )
    created_at: models.DateTimeField[datetime.datetime, datetime.datetime] = (
        models.DateTimeField(default=timezone.now)
    )
    sent_at: models.DateTimeField[
        datetime.datetime | None, datetime.datetime | None
    ] = models.DateTimeField(null=True)

    class Meta:
        db_table = "hyperscale_tasks_outbox"
        indexes = [
            models.Index(
                fields=["created_at"],
                name="hyperscale_tasks_unsent",
                condition=models.Q(sent_at__isnull=True),
            )
        ]
