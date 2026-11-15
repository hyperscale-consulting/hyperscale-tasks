import logging
from collections.abc import Sequence
from datetime import timedelta
from functools import partial
from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.db import transaction
from django.tasks import DEFAULT_TASK_QUEUE_NAME
from django.tasks import Task
from django.tasks import TaskResult
from django.tasks import TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.backends.immediate import ImmediateBackend as _ImmediateBackend
from django.tasks.exceptions import InvalidTask
from django.utils import timezone

from hyperscale.tasks import sqs
from hyperscale.tasks.models import OutboxMessage
from hyperscale.tasks.retrying import DEFAULT_RETRY_FOR
from hyperscale.tasks.retrying import RetryingTask

logger = logging.getLogger(__name__)
MAX_DELAY = timedelta(seconds=sqs.MAX_DELAY_SECONDS)


class ImmediateBackend(_ImmediateBackend):
    """Django's immediate backend, building `RetryingTask`s."""

    # run_after is accepted, though this backend still runs tasks at once.
    supports_defer = True
    # Refuse what SQSOutboxBackend refuses, so tests catch it before production.
    supports_priority = False
    supports_async_task = False
    task_class = RetryingTask


class SQSOutboxBackend(BaseTaskBackend):
    """Writes tasks to an outbox table and sends them to SQS after commit."""

    task_class = RetryingTask
    supports_defer = True
    supports_get_result = False
    supports_priority = False
    supports_async_task = False

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        try:
            self.queue_url: str = self.options["QUEUE_URL"]
        except KeyError:
            raise ImproperlyConfigured(
                "SQSOutboxBackend needs OPTIONS['QUEUE_URL']"
            ) from None

    def validate_task(self, task: Task[..., Any]) -> None:
        super().validate_task(task)
        if task.queue_name != DEFAULT_TASK_QUEUE_NAME:
            raise InvalidTask("SQSOutboxBackend only supports the default queue.")

    def enqueue(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        task: Task[..., Any],
        args: Sequence[Any],
        kwargs: dict[str, Any],
    ) -> TaskResult[..., Any]:
        self.validate_task(task)
        retry_for: timedelta = getattr(task, "retry_for", DEFAULT_RETRY_FOR)
        row = OutboxMessage(
            task_path=task.module_path,
            args=list(args),
            kwargs=kwargs,
            run_after=task.run_after,
            retry_for_seconds=int(retry_for.total_seconds()),
        )
        # Before anything is written: an unserialisable argument raises
        # TypeError here, and an oversize body would fail every send.
        size = len(sqs.body_for(row).encode())
        if size > sqs.MAX_BODY_BYTES:
            raise ValueError(
                f"The task's message body is {size} bytes; SQS takes at most "
                f"{sqs.MAX_BODY_BYTES}. Pass ids, not data."
            )
        row.save(force_insert=True)
        transaction.on_commit(partial(self._send_if_due, row))
        return TaskResult(
            task=task,
            id=str(row.id),
            status=TaskResultStatus.READY,
            enqueued_at=None,
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=list(args),
            kwargs=kwargs,
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )

    def _send_if_due(self, row: OutboxMessage) -> None:
        """Send after commit. Never raises: the sweep picks up what fails here.

        An exception escaping an on_commit hook would fail the committed
        request and skip the hooks registered after this one.
        """
        now = timezone.now()
        if row.run_after is not None and row.run_after > now + MAX_DELAY:
            return
        try:
            sqs.send_row(row, self.queue_url, now)
        except Exception as exc:
            # Class name only: the message can carry personal data.
            logger.warning(
                "tasks.send_deferred",
                extra={"outbox_id": str(row.id), "error": type(exc).__name__},
            )
