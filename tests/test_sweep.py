import logging
from datetime import timedelta

import pytest
from django.utils import timezone

from hyperscale.tasks.models import OutboxMessage
from hyperscale.tasks.sweep import sweep
from hyperscale.tasks.sweep import SweepResult
from tests.fakes import FakeSQS

pytestmark = pytest.mark.django_db(transaction=True)

QUEUE = "https://sqs.example/q"


def make_row(**fields: object) -> OutboxMessage:
    return OutboxMessage.objects.create(
        task_path="tests.testapp.tasks.notify",
        args=[],
        kwargs={},
        retry_for_seconds=3600,
        **fields,
    )


def test_sends_unsent_rows_older_than_30_seconds_only(fake_sqs: FakeSQS) -> None:
    now = timezone.now()
    old = make_row(created_at=now - timedelta(seconds=60))
    fresh = make_row(created_at=now - timedelta(seconds=5))

    result = sweep(QUEUE, now)

    assert result.sent == 1
    assert len(fake_sqs.sent) == 1
    old.refresh_from_db()
    fresh.refresh_from_db()
    assert old.sent_at == now
    assert fresh.sent_at is None


def test_releases_delayed_rows_within_15_minutes(fake_sqs: FakeSQS) -> None:
    now = timezone.now()
    soon = make_row(
        created_at=now - timedelta(minutes=5), run_after=now + timedelta(minutes=10)
    )
    later = make_row(
        created_at=now - timedelta(minutes=5), run_after=now + timedelta(minutes=20)
    )

    result = sweep(QUEUE, now)

    assert result.sent == 1
    assert [call["DelaySeconds"] for call in fake_sqs.sent] == [600]
    soon.refresh_from_db()
    later.refresh_from_db()
    assert soon.sent_at is not None
    assert later.sent_at is None


def test_deletes_sent_rows_older_than_7_days(fake_sqs: FakeSQS) -> None:
    now = timezone.now()
    created = now - timedelta(days=9)
    old = make_row(created_at=created, sent_at=now - timedelta(days=8))
    recent = make_row(created_at=created, sent_at=now - timedelta(days=6))

    result = sweep(QUEUE, now)

    assert result.deleted == 1
    assert not OutboxMessage.objects.filter(id=old.id).exists()
    assert OutboxMessage.objects.filter(id=recent.id).exists()


def test_oldest_unsent_age_counts_only_due_rows(fake_sqs: FakeSQS) -> None:
    now = timezone.now()
    fake_sqs.fail_sends = True  # keep the due row unsent
    make_row(created_at=now - timedelta(seconds=1200))
    make_row(created_at=now - timedelta(days=1), run_after=now + timedelta(hours=2))

    assert sweep(QUEUE, now).oldest_unsent_age_seconds == 1200

    OutboxMessage.objects.filter(run_after__isnull=True).delete()
    assert sweep(QUEUE, now).oldest_unsent_age_seconds == 0


def test_send_failure_leaves_row_for_next_sweep(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture
) -> None:
    now = timezone.now()
    row = make_row(created_at=now - timedelta(seconds=60))
    fake_sqs.fail_sends = True
    attempts: list[object] = []
    original = fake_sqs.send_message

    def counting(**kwargs: object) -> dict[str, object]:
        attempts.append(kwargs)
        return original(**kwargs)

    fake_sqs.send_message = counting  # type: ignore[method-assign]

    with caplog.at_level(logging.WARNING):
        result = sweep(QUEUE, now)

    assert result.sent == 0
    assert len(attempts) == 1  # not retried within the same sweep
    row.refresh_from_db()
    assert row.sent_at is None
    [record] = [r for r in caplog.records if r.getMessage() == "tasks.send_deferred"]
    assert record.__dict__["outbox_id"] == str(row.id)
    assert record.__dict__["error"] == "ClientError"
    assert "boom" not in caplog.text


def test_logs_outbox_swept(fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture) -> None:
    now = timezone.now()
    make_row(created_at=now - timedelta(seconds=60))

    with caplog.at_level(logging.INFO):
        result = sweep(QUEUE, now)

    assert result == SweepResult(sent=1, deleted=0, oldest_unsent_age_seconds=0)
    [record] = [r for r in caplog.records if r.getMessage() == "tasks.outbox_swept"]
    assert record.levelno == logging.INFO
    assert record.__dict__["sent"] == 1
    assert record.__dict__["deleted"] == 0
    assert record.__dict__["oldest_unsent_age_seconds"] == 0
