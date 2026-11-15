from typing import Any

from django.apps import AppConfig
from django.conf import settings
from django.core import checks
from django.core.exceptions import ImproperlyConfigured

SQS_OUTBOX_BACKEND = "hyperscale.tasks.backend.SQSOutboxBackend"


class HyperscaleTasksConfig(AppConfig):
    name = "hyperscale.tasks"
    label = "hyperscale_tasks"

    def ready(self) -> None:
        from hyperscale.tasks.checks import check_lambda_connections

        checks.register(check_lambda_connections)
        _refuse_missing_queue_url()


def _refuse_missing_queue_url() -> None:
    """Outside DEBUG, an outbox backend with no queue would never deliver."""
    default: dict[str, Any] = settings.TASKS.get("default", {})
    if settings.DEBUG or default.get("BACKEND") != SQS_OUTBOX_BACKEND:
        return
    if not default.get("OPTIONS", {}).get("QUEUE_URL"):
        raise ImproperlyConfigured(
            "TASKS['default'] uses SQSOutboxBackend but OPTIONS['QUEUE_URL'] is "
            "empty: set JOBS_QUEUE_URL, or run with DEBUG and ImmediateBackend."
        )
