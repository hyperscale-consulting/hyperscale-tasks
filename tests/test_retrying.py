from datetime import timedelta

import pytest
from django.utils import timezone

from hyperscale.tasks import COMMAND_RETRY_FOR
from hyperscale.tasks import DEFAULT_RETRY_FOR
from hyperscale.tasks import MAX_RETRY_FOR
from hyperscale.tasks import RetryingTask


def _noop() -> None:
    return None


def test_constants():
    assert DEFAULT_RETRY_FOR == timedelta(days=1)
    assert MAX_RETRY_FOR == timedelta(days=4)
    assert COMMAND_RETRY_FOR == timedelta(hours=1)


def test_retry_for_defaults_to_one_day():
    t = RetryingTask(func=_noop)
    assert t.retry_for == timedelta(days=1)


def test_retry_for_accepts_the_bounds():
    assert RetryingTask(func=_noop, retry_for=timedelta(minutes=1)).retry_for == (
        timedelta(minutes=1)
    )
    assert RetryingTask(func=_noop, retry_for=MAX_RETRY_FOR).retry_for == MAX_RETRY_FOR


def test_retry_for_rejects_under_a_minute_and_over_four_days():
    with pytest.raises(ValueError):
        RetryingTask(func=_noop, retry_for=timedelta(seconds=59))
    with pytest.raises(ValueError):
        RetryingTask(func=_noop, retry_for=timedelta(days=4, seconds=1))


def test_django_task_decorator_passes_retry_for_through():
    from tests.testapp.tasks import slow_partner

    assert isinstance(slow_partner, RetryingTask)
    assert slow_partner.retry_for == timedelta(hours=6)


def test_using_keeps_retry_for():
    from tests.testapp.tasks import slow_partner

    later = slow_partner.using(run_after=timezone.now())  # pyright: ignore[reportFunctionMemberAccess]
    assert isinstance(later, RetryingTask)
    assert later.retry_for == timedelta(hours=6)


def test_app_config():
    from django.apps import apps

    config = apps.get_app_config("hyperscale_tasks")
    assert config.name == "hyperscale.tasks"
