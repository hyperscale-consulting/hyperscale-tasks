from typing import Any

import pytest
from django.apps import apps
from django.conf import settings as django_settings
from django.core.checks.registry import registry
from django.core.exceptions import ImproperlyConfigured
from pytest_django.fixtures import Settings

from hyperscale.tasks.checks import check_lambda_connections

SQS_BACKEND = "hyperscale.tasks.backend.SQSOutboxBackend"
SQLITE = {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}


def ready() -> None:
    apps.get_app_config("hyperscale_tasks").ready()


@pytest.mark.parametrize(
    "options",
    [pytest.param({"QUEUE_URL": ""}, id="empty"), pytest.param({}, id="missing")],
)
def test_ready_refuses_missing_queue_url_outside_debug(
    settings: Settings, options: dict[str, str]
) -> None:
    settings.DEBUG = False
    settings.TASKS = {"default": {"BACKEND": SQS_BACKEND, "OPTIONS": options}}

    with pytest.raises(ImproperlyConfigured, match="JOBS_QUEUE_URL"):
        ready()


@pytest.mark.parametrize(
    "debug,tasks",
    [
        pytest.param(
            False,
            {"BACKEND": SQS_BACKEND, "OPTIONS": {"QUEUE_URL": "https://sqs.example/q"}},
            id="queue_url_set",
        ),
        pytest.param(
            True, {"BACKEND": SQS_BACKEND, "OPTIONS": {"QUEUE_URL": ""}}, id="debug"
        ),
        pytest.param(
            False,
            {"BACKEND": "hyperscale.tasks.backend.ImmediateBackend"},
            id="immediate_backend",
        ),
    ],
)
def test_ready_accepts(settings: Settings, debug: bool, tasks: dict[str, Any]) -> None:
    settings.DEBUG = debug
    settings.TASKS = {"default": tasks}

    ready()


def use_databases(monkeypatch: pytest.MonkeyPatch, **database: Any) -> None:
    """Swap the DATABASES the check reads, leaving the live connections alone.

    (Overriding DATABASES through the settings fixture warns, and would reach
    the test database connection.)
    """
    monkeypatch.setattr(
        django_settings, "DATABASES", {"default": SQLITE, "other": SQLITE | database}
    )


def e001(errors: list[Any]) -> list[Any]:
    return [error for error in errors if error.id == "hyperscale_tasks.E001"]


def test_check_flags_pool_in_lambda(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HYPERSCALE_RUNTIME", "lambda")
    use_databases(monkeypatch, OPTIONS={"pool": True})

    [error] = e001(check_lambda_connections(None))

    assert error.is_serious()
    assert "'other'" in error.msg
    assert "CONN_MAX_AGE" in error.hint and "pool" in error.hint


def test_check_is_registered_by_the_app() -> None:
    assert check_lambda_connections in registry.get_checks()


@pytest.mark.parametrize("conn_max_age", [60, None])
def test_check_flags_persistent_connections_in_lambda(
    monkeypatch: pytest.MonkeyPatch, conn_max_age: int | None
) -> None:
    monkeypatch.setenv("HYPERSCALE_RUNTIME", "lambda")
    use_databases(monkeypatch, CONN_MAX_AGE=conn_max_age)

    assert len(e001(check_lambda_connections(None))) == 1


@pytest.mark.parametrize("runtime", ["ecs", None])
def test_check_quiet_on_ecs(
    monkeypatch: pytest.MonkeyPatch, runtime: str | None
) -> None:
    if runtime is None:
        monkeypatch.delenv("HYPERSCALE_RUNTIME", raising=False)
    else:
        monkeypatch.setenv("HYPERSCALE_RUNTIME", runtime)
    use_databases(monkeypatch, OPTIONS={"pool": True}, CONN_MAX_AGE=60)

    assert check_lambda_connections(None) == []


@pytest.mark.parametrize(
    "database",
    [
        pytest.param({}, id="defaults"),
        pytest.param({"CONN_MAX_AGE": 0, "OPTIONS": {"pool": False}}, id="explicit"),
    ],
)
def test_check_quiet_in_lambda_without_pool_or_persistent_connections(
    monkeypatch: pytest.MonkeyPatch, database: dict[str, Any]
) -> None:
    monkeypatch.setenv("HYPERSCALE_RUNTIME", "lambda")
    use_databases(monkeypatch, **database)

    assert check_lambda_connections(None) == []
