import json
import uuid
from datetime import timedelta
from typing import Any

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError
from django.db import transaction
from django.tasks.exceptions import InvalidTask
from django.utils import timezone

from hyperscale.tasks import sqs
from hyperscale.tasks.models import OutboxMessage
from tests.fakes import FakeSQS
from tests.testapp.tasks import notify
from tests.testapp.tasks import ok

ADDRESS = "secret@example.com"

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture(autouse=True)
def sqs_backend(settings: Any) -> None:
    # Assigning TASKS fires setting_changed, which resets Django's cached backends
    # both now and again when pytest-django restores the setting.
    settings.TASKS = {
        "default": {
            "BACKEND": "hyperscale.tasks.backend.SQSOutboxBackend",
            "OPTIONS": {"QUEUE_URL": "https://sqs.example/q"},
        }
    }


def test_enqueue_writes_row_and_sends_after_commit(fake_sqs: FakeSQS) -> None:
    with transaction.atomic():
        result = notify.enqueue(42)
        assert fake_sqs.sent == []  # nothing before commit
    row = OutboxMessage.objects.get()
    assert str(row.id) == result.id
    assert fake_sqs.sent[0]["QueueUrl"] == "https://sqs.example/q"
    assert json.loads(fake_sqs.sent[0]["MessageBody"]) == {
        "v": 1,
        "kind": "task",
        "task": "tests.testapp.tasks.notify",
        "args": [42],
        "kwargs": {},
        "id": result.id,
        "retry_for_s": 86400,
    }
    assert row.sent_at is not None


def test_rollback_sends_nothing_and_leaves_no_row(fake_sqs: FakeSQS) -> None:
    with pytest.raises(RuntimeError), transaction.atomic():
        notify.enqueue(42)
        raise RuntimeError
    assert fake_sqs.sent == []
    assert not OutboxMessage.objects.exists()


def test_send_failure_keeps_row_unsent_and_logs(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture
) -> None:
    fake_sqs.fail_sends = True
    with transaction.atomic():
        notify.enqueue(42)  # must not raise
    assert OutboxMessage.objects.get().sent_at is None
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert [r.message for r in warnings] == ["tasks.send_deferred"]
    assert vars(warnings[0])["outbox_id"] == str(OutboxMessage.objects.get().id)
    assert vars(warnings[0])["error"] == "ClientError"
    assert "boom" not in caplog.text


def test_run_after_within_15_minutes_uses_delay_seconds(fake_sqs: FakeSQS) -> None:
    notify.using(run_after=timezone.now() + timedelta(minutes=10, seconds=1)).enqueue(1)
    assert 598 <= fake_sqs.sent[0]["DelaySeconds"] <= 600
    assert OutboxMessage.objects.get().sent_at is not None


def test_run_after_beyond_15_minutes_is_not_sent(fake_sqs: FakeSQS) -> None:
    notify.using(run_after=timezone.now() + timedelta(hours=2)).enqueue(1)
    assert fake_sqs.sent == []
    assert OutboxMessage.objects.get().sent_at is None


def test_non_default_queue_name_is_rejected() -> None:
    with pytest.raises(InvalidTask):
        notify.using(queue_name="other")


def test_args_must_be_json_serialisable(fake_sqs: FakeSQS) -> None:
    with pytest.raises(TypeError):
        notify.enqueue(object())  # pyright: ignore[reportArgumentType]
    assert not OutboxMessage.objects.exists()
    assert fake_sqs.sent == []


def test_enqueue_in_autocommit_sends_immediately(fake_sqs: FakeSQS) -> None:
    notify.enqueue(7)
    assert len(fake_sqs.sent) == 1
    assert OutboxMessage.objects.get().sent_at is not None


def test_queue_url_is_required(settings: Any) -> None:
    settings.TASKS = {
        "default": {"BACKEND": "hyperscale.tasks.backend.SQSOutboxBackend"}
    }
    with pytest.raises(ImproperlyConfigured, match="QUEUE_URL"):
        notify.enqueue(1)


def broken_send(*args: object, **kwargs: object) -> None:
    raise DatabaseError(ADDRESS)


def test_any_send_error_is_deferred_and_later_hooks_still_run(
    fake_sqs: FakeSQS,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(sqs, "send_row", broken_send)
    later: list[str] = []

    with transaction.atomic():
        notify.enqueue(42)  # its hook raises after commit
        transaction.on_commit(lambda: later.append("ran"))

    assert later == ["ran"]
    row = OutboxMessage.objects.get()
    assert row.sent_at is None
    [deferred] = [r for r in caplog.records if r.getMessage() == "tasks.send_deferred"]
    assert vars(deferred)["outbox_id"] == str(row.id)
    assert vars(deferred)["error"] == "DatabaseError"
    assert ADDRESS not in caplog.text


def test_any_send_error_in_autocommit_does_not_raise(
    fake_sqs: FakeSQS, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sqs, "send_row", broken_send)

    result = notify.enqueue(42)

    assert OutboxMessage.objects.get().id == uuid.UUID(result.id)


def test_send_after_commit_does_not_read_the_row_back(
    fake_sqs: FakeSQS, django_assert_num_queries: Any
) -> None:
    with django_assert_num_queries(2):  # the INSERT, then the UPDATE of sent_at
        notify.enqueue(7)

    assert len(fake_sqs.sent) == 1


def body_bytes_with_label(label: str) -> int:
    """The encoded body size of `ok.enqueue(1, label=label)`."""
    row = OutboxMessage(
        task_path="tests.testapp.tasks.ok",
        args=[1],
        kwargs={"label": label},
        retry_for_seconds=86400,
    )
    return len(sqs.body_for(row).encode())


def test_body_at_the_size_limit_is_enqueued(fake_sqs: FakeSQS) -> None:
    label = "x" * (sqs.MAX_BODY_BYTES - body_bytes_with_label(""))

    ok.enqueue(1, label=label)

    assert len(fake_sqs.sent[0]["MessageBody"].encode()) == sqs.MAX_BODY_BYTES
    assert OutboxMessage.objects.get().sent_at is not None


def test_body_over_the_size_limit_is_refused_before_writing(
    fake_sqs: FakeSQS,
) -> None:
    label = "x" * (sqs.MAX_BODY_BYTES - body_bytes_with_label("") + 1)

    with pytest.raises(ValueError, match=str(sqs.MAX_BODY_BYTES)) as raised:
        ok.enqueue(1, label=label)

    assert label not in str(raised.value)
    assert not OutboxMessage.objects.exists()
    assert fake_sqs.sent == []
