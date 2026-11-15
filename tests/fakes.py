from typing import Any

from botocore.exceptions import ClientError


class FakeSQS:
    """Records the SQS calls the package makes; can fail sends on demand."""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.sent_batches: list[dict[str, Any]] = []
        self.visibility_changes: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []
        self.fail_sends = False

    def _fail(self, operation: str) -> ClientError:
        return ClientError(
            {"Error": {"Code": "ServiceUnavailable", "Message": "boom"}}, operation
        )

    def send_message(self, **kwargs: Any) -> dict[str, Any]:
        if self.fail_sends:
            raise self._fail("SendMessage")
        self.sent.append(kwargs)
        return {"MessageId": f"m{len(self.sent)}"}

    def send_message_batch(self, **kwargs: Any) -> dict[str, Any]:
        if self.fail_sends:
            raise self._fail("SendMessageBatch")
        self.sent_batches.append(kwargs)
        return {"Successful": [], "Failed": []}

    def change_message_visibility(self, **kwargs: Any) -> dict[str, Any]:
        self.visibility_changes.append(kwargs)
        return {}

    def delete_message(self, **kwargs: Any) -> dict[str, Any]:
        self.deleted.append(kwargs)
        return {}
