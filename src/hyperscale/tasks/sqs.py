import json
from datetime import datetime
from functools import cache
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config

from hyperscale.tasks.models import OutboxMessage

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient

# SQS refuses a DelaySeconds above 15 minutes.
MAX_DELAY_SECONDS = 900
# The largest message body sent: 256 KB, the SQS message size limit.
MAX_BODY_BYTES = 262144

# Fail fast: a send runs in the request after commit and in the sweep while it
# holds row locks, and the sweep sends whatever fails here.
CLIENT_CONFIG = Config(
    connect_timeout=2,
    read_timeout=5,
    retries={"mode": "standard", "max_attempts": 2},
)


@cache
def client() -> SQSClient:
    """The one place a boto3 SQS client is made; tests replace this."""
    # Its own session, so app code that reconfigures boto3's shared default
    # session cannot change this client's timeouts or retries.
    session = boto3.session.Session()
    # The stubs type only the services installed (sqs); the other overloads
    # are unknown.
    return session.client("sqs", config=CLIENT_CONFIG)  # pyright: ignore[reportUnknownMemberType]


def body_for(row: OutboxMessage) -> str:
    """The SQS message body for an outbox row."""
    return json.dumps(
        {
            "v": 1,
            "kind": "task",
            "task": row.task_path,
            "args": row.args,
            "kwargs": row.kwargs,
            "id": str(row.id),
            "retry_for_s": row.retry_for_seconds,
        }
    )


def delay_seconds(row: OutboxMessage, now: datetime) -> int:
    if row.run_after is None:
        return 0
    return max(0, min(MAX_DELAY_SECONDS, int((row.run_after - now).total_seconds())))


def send_row(row: OutboxMessage, queue_url: str, now: datetime) -> None:
    """Send the row, then mark it sent. Raises on SQS errors; callers catch."""
    client().send_message(
        QueueUrl=queue_url,
        MessageBody=body_for(row),
        DelaySeconds=delay_seconds(row, now),
    )
    OutboxMessage.objects.filter(id=row.id, sent_at__isnull=True).update(sent_at=now)
