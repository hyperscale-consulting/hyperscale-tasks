"""The AWS Lambda entry point: SQS records and the outbox sweep.

Importing this module touches neither settings nor models. The cold start
(secrets, then ``django.setup()``) runs inside the first invoke, so a failure
there is an invocation error that Lambda counts, and the messages come back.
"""

import os
from typing import Any
from typing import cast

import django
from django.db import connections

from hyperscale.tasks.secrets import load_secrets

# Set only once the whole cold start has succeeded. A secrets failure is
# retried on the next invoke; a django.setup() failure cannot be (Django's
# populate() is not re-entrant after it raises), so it is kept and named as the
# cause on every later invoke in this environment.
_ready = False
_setup_error: BaseException | None = None


def handler(event: dict[str, Any], context: object) -> dict[str, Any]:
    _setup()
    payload: object = event
    if _is_sweep(payload):
        return _sweep()
    records = _sqs_records(payload)
    if records is None:
        raise ValueError(f"Unsupported Lambda event: {_describe(payload)}")
    return _process(records)


def _setup() -> None:
    global _ready, _setup_error
    if _ready:
        return
    if _setup_error is not None:
        # A fresh exception each time: re-raising the original would grow
        # its traceback on every invoke.
        raise RuntimeError(
            "django.setup() failed earlier in this environment"
        ) from _setup_error
    load_secrets(os.environ)  # first: settings may read the secrets
    try:
        django.setup()
    except BaseException as exc:
        _setup_error = exc
        raise
    _ready = True


def _is_sweep(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    fields = cast(dict[str, Any], payload)
    return (
        type(fields.get("v")) is int
        and fields["v"] == 1
        and fields.get("kind") == "sweep"
    )


def _sqs_records(payload: object) -> list[dict[str, Any]] | None:
    """The event's records when it is an SQS event, else None."""
    if not isinstance(payload, dict):
        return None
    records = cast(dict[str, Any], payload).get("Records")
    if not isinstance(records, list):
        return None
    entries = cast(list[object], records)
    if not all(
        isinstance(entry, dict)
        and cast(dict[str, Any], entry).get("eventSource") == "aws:sqs"
        for entry in entries
    ):
        return None
    return cast(list[dict[str, Any]], entries)


def _describe(payload: object) -> str:
    """The event's shape, never its content (it may carry personal data)."""
    if isinstance(payload, dict):
        keys = sorted(str(key) for key in cast(dict[object, object], payload))
        return f"top-level keys {keys}"
    return f"a JSON {type(payload).__name__}"


def _sweep() -> dict[str, Any]:
    from hyperscale.tasks.sweep import sweep

    try:
        sweep(os.environ["JOBS_QUEUE_URL"])
    finally:
        connections.close_all()
    return {"swept": True}


def _process(records: list[dict[str, Any]]) -> dict[str, Any]:
    # process_record closes connections itself; an exception it raises (an
    # SQS error) crashes the invocation, so every record comes back.
    from hyperscale.tasks.dispatch import process_record

    queue_url = os.environ["JOBS_QUEUE_URL"]
    dlq_url = os.environ["JOBS_DLQ_URL"]
    failures = [
        {"itemIdentifier": record["messageId"]}
        for record in records
        if not process_record(record, queue_url, dlq_url)
    ]
    return {"batchItemFailures": failures}
