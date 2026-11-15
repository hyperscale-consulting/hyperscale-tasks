"""Smoke-test helper, run in the image against the SQS stub.

sqs_stub.py queues              create the queues; print their URLs
sqs_stub.py event QUEUE_URL     send and receive a message for
                                tests.smoke.tasks.ok; print the Lambda event
sqs_stub.py count QUEUE_URL     print the queue's message counts
"""

import json
import sys
from typing import Any
from typing import cast
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient

BODY: dict[str, Any] = {
    "v": 1,
    "kind": "task",
    "task": "tests.smoke.tasks.ok",
    "args": [],
    "kwargs": {},
}


def main(argv: list[str]) -> None:
    sqs: SQSClient = boto3.client("sqs")  # pyright: ignore[reportUnknownMemberType]
    match argv:
        case ["queues"]:
            queue = sqs.create_queue(QueueName="hyperscale-smoke-jobs")
            dlq = sqs.create_queue(QueueName="hyperscale-smoke-jobs-dlq")
            print(queue["QueueUrl"], dlq["QueueUrl"])
        case ["event", queue_url]:
            sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(BODY))
            received = sqs.receive_message(
                QueueUrl=queue_url, MessageSystemAttributeNames=["All"]
            )
            [message] = cast(list[dict[str, Any]], received.get("Messages", []))
            record: dict[str, Any] = {
                "messageId": message["MessageId"],
                "receiptHandle": message["ReceiptHandle"],
                "body": message["Body"],
                "attributes": message["Attributes"],
                "messageAttributes": {},
                "md5OfBody": message["MD5OfBody"],
                "eventSource": "aws:sqs",
                "awsRegion": sqs.meta.region_name,
            }
            print(json.dumps({"Records": [record]}))
        case ["count", queue_url]:
            attributes = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                ],
            ).get("Attributes", {})
            print(json.dumps(attributes, sort_keys=True))
        case _:
            sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
