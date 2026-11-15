import pytest
from django.tasks import task
from django.tasks.exceptions import InvalidTask

from tests.testapp.tasks import notify

# The tests' TASKS use hyperscale.tasks.backend.ImmediateBackend. It must
# refuse what SQSOutboxBackend refuses, or a task passes the app's tests and
# fails in production.


async def notify_async() -> None:
    """Made a task only inside the test: Django needs a module-level function."""


def test_async_task_is_rejected() -> None:
    with pytest.raises(InvalidTask, match="async"):
        task(notify_async)


def test_non_default_priority_is_rejected() -> None:
    with pytest.raises(InvalidTask, match="priority"):
        notify.using(priority=10)
