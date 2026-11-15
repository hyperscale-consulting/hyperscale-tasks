import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta

from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError
from django.db import transaction
from django.db.models import F
from django.db.models import Q
from django.db.models.functions import Coalesce
from django.utils import timezone

from hyperscale.tasks import sqs
from hyperscale.tasks.models import OutboxMessage

logger = logging.getLogger(__name__)

BATCH_SIZE = 100
BUDGET_SECONDS = 60
MIN_AGE = timedelta(seconds=30)
RELEASE_WINDOW = timedelta(seconds=sqs.MAX_DELAY_SECONDS)
SENT_RETENTION = timedelta(days=7)


@dataclass(frozen=True)
class SweepResult:
    sent: int
    deleted: int
    oldest_unsent_age_seconds: int


def sweep(
    queue_url: str,
    now: datetime | None = None,
    *,
    budget_seconds: float = BUDGET_SECONDS,
) -> SweepResult:
    """Send unsent rows, delete old sent rows, and report how stale the outbox is.

    Sending stops once `budget_seconds` have passed (the Lambda handler passes
    less when its invocation has less time left); the next sweep carries on.
    """
    now = now or timezone.now()
    deadline = time.monotonic() + budget_seconds
    sent = 0
    failed: set[uuid.UUID] = set()  # tried this run; the next sweep retries them

    while time.monotonic() < deadline:
        with transaction.atomic():
            rows = list(
                OutboxMessage.objects.select_for_update(skip_locked=True)
                .filter(sent_at__isnull=True, created_at__lte=now - MIN_AGE)
                .filter(
                    Q(run_after__isnull=True) | Q(run_after__lte=now + RELEASE_WINDOW)
                )
                .exclude(id__in=failed)
                .order_by("created_at")[:BATCH_SIZE]
            )
            for row in rows:
                try:
                    sqs.send_row(row, queue_url, now)
                except (BotoCoreError, ClientError) as exc:
                    failed.add(row.id)
                    # Class name only: the message can carry personal data.
                    logger.warning(
                        "tasks.send_deferred",
                        extra={"outbox_id": str(row.id), "error": type(exc).__name__},
                    )
                else:
                    sent += 1
        if not rows:
            break

    deleted, _ = OutboxMessage.objects.filter(sent_at__lt=now - SENT_RETENTION).delete()

    result = SweepResult(sent, deleted, _oldest_unsent_age_seconds(now))
    logger.info(
        "tasks.outbox_swept",
        extra={
            "sent": result.sent,
            "deleted": result.deleted,
            "oldest_unsent_age_seconds": result.oldest_unsent_age_seconds,
        },
    )
    return result


def _oldest_unsent_age_seconds(now: datetime) -> int:
    """Age of the longest-waiting unsent row that is due; delayed rows don't count."""
    oldest = (
        OutboxMessage.objects.filter(sent_at__isnull=True)
        .filter(Q(run_after__isnull=True) | Q(run_after__lte=now))
        .annotate(due_since=Coalesce(F("run_after"), F("created_at")))
        .order_by("due_since")
        .values_list("due_since", flat=True)
        .first()
    )
    return 0 if oldest is None else max(0, int((now - oldest).total_seconds()))
