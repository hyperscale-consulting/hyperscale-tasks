import os
import subprocess
import sys
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError
from django.utils import timezone

from hyperscale.tasks import lambda_handler
from hyperscale.tasks.lambda_handler import handler
from hyperscale.tasks.models import OutboxMessage
from tests.fakes import FakeSQS
from tests.records import record
from tests.testapp import tasks

QUEUE = "https://sqs.example/q"
DLQ = "https://sqs.example/dlq"
ADDRESS = "secret@example.com"


@pytest.fixture(autouse=True)
def cold_start(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """A fresh process for each test, with the cold-start steps recorded.

    pytest-django has already set Django up, so the real `django.setup()` is
    replaced by a recorder (running it again would reconfigure logging).
    """
    calls: list[str] = []

    def load_secrets(environ: object) -> None:
        calls.append("secrets")

    monkeypatch.setattr(lambda_handler, "_ready", False)
    monkeypatch.setattr(lambda_handler, "_setup_error", None)
    monkeypatch.setattr(lambda_handler, "load_secrets", load_secrets)
    monkeypatch.setattr("django.setup", lambda: calls.append("setup"))
    monkeypatch.setenv("JOBS_QUEUE_URL", QUEUE)
    monkeypatch.setenv("JOBS_DLQ_URL", DLQ)
    tasks.calls.clear()
    yield calls
    tasks.calls.clear()


def task_body(name: str, *args: Any) -> dict[str, Any]:
    return {
        "v": 1,
        "kind": "task",
        "task": f"tests.testapp.tasks.{name}",
        "args": list(args),
        "kwargs": {},
    }


def sqs_event(*records: dict[str, Any]) -> dict[str, Any]:
    return {"Records": list(records)}


def test_sqs_event_reports_only_failed_records(fake_sqs: FakeSQS) -> None:
    now = timezone.now()
    event = sqs_event(
        record(task_body("ok", 1), 1, now, message_id="m1", receipt_handle="rh-m1"),
        record(task_body("boom"), 1, now, message_id="m2", receipt_handle="rh-m2"),
    )

    result = handler(event, None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m2"}]}
    assert tasks.calls == [("ok", (1, ""))]
    assert fake_sqs.deleted == [{"QueueUrl": QUEUE, "ReceiptHandle": "rh-m1"}]
    assert [c["ReceiptHandle"] for c in fake_sqs.visibility_changes] == ["rh-m2"]


def test_sqs_event_with_no_failures_reports_none(fake_sqs: FakeSQS) -> None:
    assert handler(sqs_event(record(task_body("ok", 2))), None) == {
        "batchItemFailures": []
    }


def test_record_that_raises_crashes_the_invocation(fake_sqs: FakeSQS) -> None:
    fake_sqs.fail_sends = True  # the DLQ send for the bad message fails

    with pytest.raises(ClientError):
        handler(sqs_event(record("not json {")), None)

    assert fake_sqs.deleted == []


@pytest.mark.django_db(transaction=True)
def test_sweep_event_runs_sweep(fake_sqs: FakeSQS) -> None:
    row = OutboxMessage.objects.create(
        task_path="tests.testapp.tasks.notify",
        args=[],
        kwargs={},
        retry_for_seconds=3600,
        created_at=timezone.now() - timedelta(minutes=1),
    )

    assert handler({"v": 1, "kind": "sweep"}, None) == {"swept": True}

    assert [call["QueueUrl"] for call in fake_sqs.sent] == [QUEUE]
    row.refresh_from_db()
    assert row.sent_at is not None


@pytest.mark.parametrize("fails", [False, True], ids=["success", "sweep_raises"])
def test_sweep_closes_connections(monkeypatch: pytest.MonkeyPatch, fails: bool) -> None:
    closed: list[None] = []
    swept: list[str] = []

    def fake_sweep(queue_url: str) -> None:
        swept.append(queue_url)
        if fails:
            raise RuntimeError("database went away")

    monkeypatch.setattr("hyperscale.tasks.sweep.sweep", fake_sweep)
    monkeypatch.setattr(
        lambda_handler.connections, "close_all", lambda: closed.append(None)
    )

    if fails:
        with pytest.raises(RuntimeError):
            handler({"v": 1, "kind": "sweep"}, None)
    else:
        handler({"v": 1, "kind": "sweep"}, None)

    assert swept == [QUEUE]
    assert closed == [None]


@pytest.mark.parametrize(
    "event",
    [
        pytest.param({}, id="empty"),
        pytest.param({"v": 1, "kind": "task", "email": ADDRESS}, id="other_kind"),
        pytest.param({"v": 2, "kind": "sweep"}, id="other_version"),
        pytest.param({"v": True, "kind": "sweep"}, id="bool_version"),
        pytest.param({"Records": ADDRESS}, id="records_not_a_list"),
        pytest.param(
            {"Records": [{"eventSource": "aws:sns", "Sns": {"Message": ADDRESS}}]},
            id="not_sqs",
        ),
        pytest.param([ADDRESS], id="not_a_dict"),
    ],
)
def test_unknown_event_shape_raises(event: Any, fake_sqs: FakeSQS) -> None:
    with pytest.raises(ValueError) as raised:
        handler(event, None)

    assert ADDRESS not in str(raised.value)
    assert fake_sqs.deleted == []


def test_secrets_failure_propagates_and_is_retried(
    monkeypatch: pytest.MonkeyPatch, cold_start: list[str], fake_sqs: FakeSQS
) -> None:
    failing = [True]

    def load_secrets(environ: object) -> None:
        cold_start.append("secrets")
        if failing:
            raise RuntimeError("secrets failed")

    monkeypatch.setattr(lambda_handler, "load_secrets", load_secrets)
    event = sqs_event(record(task_body("ok", 1)))

    with pytest.raises(RuntimeError, match="secrets failed"):
        handler(event, None)

    # Secrets come first: settings may read them. Nothing ran and nothing was
    # settled, so the message comes back.
    assert cold_start == ["secrets"]
    assert tasks.calls == []
    assert fake_sqs.deleted == []
    # Secrets run before the app registry is touched, so the next invoke can
    # try the whole cold start again.
    failing.clear()
    cold_start.clear()
    assert handler(event, None) == {"batchItemFailures": []}
    assert cold_start == ["secrets", "setup"]


def test_setup_failure_propagates_and_is_resurfaced(
    monkeypatch: pytest.MonkeyPatch, cold_start: list[str], fake_sqs: FakeSQS
) -> None:
    original = RuntimeError("setup failed")

    def setup() -> None:
        cold_start.append("setup")
        raise original

    monkeypatch.setattr("django.setup", setup)
    event = sqs_event(record(task_body("ok", 1)))

    with pytest.raises(RuntimeError) as first:
        handler(event, None)
    assert first.value is original
    with pytest.raises(RuntimeError, match="failed earlier") as second:
        handler(event, None)

    # A failed populate() cannot be retried, so later invokes are not tried
    # again; each raises a fresh error caused by the original.
    assert second.value is not original
    assert second.value.__cause__ is original
    assert cold_start == ["secrets", "setup"]
    assert tasks.calls == []
    assert fake_sqs.deleted == []


def test_real_setup_failure_names_the_cause_on_every_invoke() -> None:
    """With Django's real populate(), which cannot be re-entered after a failure."""
    env = {k: v for k, v in os.environ.items() if k != "DJANGO_SETTINGS_MODULE"}
    code = """
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from hyperscale.tasks.lambda_handler import handler

settings.configure(
    DEBUG=False,
    INSTALLED_APPS=["hyperscale.tasks"],
    TASKS={"default": {
        "BACKEND": "hyperscale.tasks.backend.SQSOutboxBackend",
        "OPTIONS": {"QUEUE_URL": ""},
    }},
)
errors = []
for _ in range(2):
    try:
        handler({"Records": []}, None)
    except Exception as exc:
        errors.append(exc)
first, second = errors
assert isinstance(first, ImproperlyConfigured), repr(first)
assert isinstance(second, RuntimeError), repr(second)
assert second.__cause__ is first, repr(second.__cause__)
"""

    done = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr


def test_setup_runs_once_across_invocations(
    cold_start: list[str], fake_sqs: FakeSQS
) -> None:
    handler(sqs_event(record(task_body("ok", 1))), None)
    handler(sqs_event(record(task_body("ok", 2))), None)

    assert cold_start == ["secrets", "setup"]
    assert tasks.calls == [("ok", (1, "")), ("ok", (2, ""))]


def test_secrets_are_loaded_into_the_process_environment(
    monkeypatch: pytest.MonkeyPatch, fake_sqs: FakeSQS
) -> None:
    seen: list[object] = []
    monkeypatch.setattr(lambda_handler, "load_secrets", seen.append)

    handler(sqs_event(record(task_body("ok", 1))), None)

    assert seen == [os.environ]


def test_import_needs_no_settings() -> None:
    """Importing the handler module must not touch settings or models."""
    env = {k: v for k, v in os.environ.items() if k != "DJANGO_SETTINGS_MODULE"}
    code = (
        "import sys, django.apps, hyperscale.tasks.lambda_handler;"
        "assert not django.apps.apps.ready;"
        "assert 'hyperscale.tasks.models' not in sys.modules"
    )

    subprocess.run([sys.executable, "-c", code], env=env, check=True)
