import json
from datetime import timedelta
from typing import Any

import pytest
from django.db import transaction
from django.tasks.exceptions import InvalidTask
from django.utils import timezone

from hyperscale.tasks.models import OutboxMessage
from tests.fakes import FakeSQS
from tests.testapp.tasks import notify

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
    with pytest.raises(Exception, match="QUEUE_URL"):
        notify.enqueue(1)
