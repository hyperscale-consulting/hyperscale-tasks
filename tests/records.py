import json
from datetime import datetime
from datetime import UTC
from typing import Any

NOW = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)


def record(
    body: object,
    receive_count: int = 1,
    first_receive: datetime = NOW,
    *,
    message_id: str = "msg-1",
    receipt_handle: str = "rh-1",
) -> dict[str, Any]:
    """One entry of a Lambda SQS event's `Records`."""
    return {
        "messageId": message_id,
        "receiptHandle": receipt_handle,
        "body": body if isinstance(body, str) else json.dumps(body),
        "attributes": {
            "ApproximateReceiveCount": str(receive_count),
            "ApproximateFirstReceiveTimestamp": str(
                int(first_receive.timestamp() * 1000)
            ),
        },
        "eventSource": "aws:sqs",
    }
