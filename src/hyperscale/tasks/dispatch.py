"""Run one SQS record: parse the body, resolve it, run it, settle the outcome."""

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import UTC
from functools import partial
from types import TracebackType
from typing import Any
from typing import cast

from django.conf import settings
from django.core.management import call_command
from django.core.management import get_commands
from django.db import connections
from django.tasks import Task
from django.tasks import TaskContext
from django.tasks import TaskResult
from django.tasks import TaskResultStatus
from django.utils import timezone
from django.utils.module_loading import import_string

from hyperscale.tasks import sqs
from hyperscale.tasks.backoff import backoff_seconds
from hyperscale.tasks.retrying import COMMAND_RETRY_FOR
from hyperscale.tasks.retrying import DEFAULT_RETRY_FOR
from hyperscale.tasks.retrying import MAX_RETRY_FOR

logger = logging.getLogger(__name__)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# SQS caps visibility at 12 h from the ReceiveMessage, and extending it does
# not reset that clock; a longer VisibilityTimeout is an error. Leave room for
# the longest Lambda run (900 s) plus a margin for the receive and invoke.
MAX_VISIBILITY_SECONDS = 43200 - 900 - 60
# TaskContext.attempt counts worker ids; SQS doesn't say which workers ran it.
WORKER_ID = "sqs"

type ExcInfo = tuple[type[BaseException], BaseException, TracebackType | None]


class _BadMessage(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class _Job:
    name: str  # the `task` log field
    run: Callable[[], object]
    retry_for: timedelta


@dataclass(frozen=True)
class _Unhandled:
    source: str
    detail_type: str


def process_record(
    record: dict[str, Any], queue_url: str, dlq_url: str, now: datetime | None = None
) -> bool:
    """Run one record of a Lambda SQS event.

    True when the record is finished (it succeeded, gave up or was a bad
    message, and is deleted or moved to the DLQ); False when it is backing off
    and belongs in `batchItemFailures`. SQS errors propagate: the invocation
    fails and SQS redelivers, so a message is never deleted unless it reached
    the DLQ.
    """
    now = now or timezone.now()
    try:
        attempt = int(record["attributes"]["ApproximateReceiveCount"])
        job = _resolve(record, attempt, now)
        if isinstance(job, str):
            logger.error(
                "tasks.bad_message",
                extra={"message_id": record["messageId"], "reason": job},
            )
            _move_to_dlq(record, queue_url, dlq_url, "bad_message")
            return True
        if isinstance(job, _Unhandled):
            logger.error(
                "tasks.unhandled_event",
                extra={"source": job.source, "detail_type": job.detail_type},
            )
            _delete(record, queue_url)
            return True
        return _run(job, record, attempt, queue_url, dlq_url, now)
    finally:
        connections.close_all()


def _resolve(
    record: dict[str, Any], attempt: int, now: datetime
) -> _Job | _Unhandled | str:
    """What the body asks to run, or the reason it is a bad message."""
    try:
        try:
            parsed = json.loads(record["body"])
        except ValueError:
            raise _BadMessage("not_json") from None
        if not isinstance(parsed, dict):
            raise _BadMessage("not_object")
        body = cast(dict[str, Any], parsed)
        if "v" not in body:
            return _event(body, record["messageId"], attempt, now)
        if type(body["v"]) is not int or body["v"] != 1:
            raise _BadMessage("bad_version")
        match body.get("kind"):
            case "task":
                return _task(body, record["messageId"], attempt, now)
            case "command":
                return _command(body)
            case _:
                raise _BadMessage("unknown_kind")
    except _BadMessage as exc:
        return exc.reason


def _task(body: dict[str, Any], message_id: str, attempt: int, now: datetime) -> _Job:
    path, args, kwargs = body.get("task"), body.get("args"), body.get("kwargs")
    retry_for_s = body.get("retry_for_s")
    if (
        not isinstance(path, str)
        or not isinstance(args, list)
        or not isinstance(kwargs, dict)
        or (retry_for_s is not None and type(retry_for_s) is not int)
    ):
        raise _BadMessage("bad_task")
    task = _import_task(path, "bad_task")
    result_id = str(body.get("id") or message_id)
    run = partial(
        _call,
        task,
        cast(list[Any], args),
        cast(dict[str, Any], kwargs),
        result_id,
        attempt,
        now,
    )
    if retry_for_s is None:
        return _Job(path, run, _retry_for(task))
    return _Job(path, run, timedelta(seconds=retry_for_s))


def _command(body: dict[str, Any]) -> _Job:
    name, args = body.get("command"), body.get("args")
    if not isinstance(name, str) or not isinstance(args, list):
        raise _BadMessage("bad_command")
    words = cast(list[object], args)
    if not all(isinstance(word, str) for word in words):
        raise _BadMessage("bad_command")
    if name not in get_commands():
        raise _BadMessage("unknown_command")
    return _Job(name, partial(call_command, name, *words), COMMAND_RETRY_FOR)


def _event(
    body: dict[str, Any], message_id: str, attempt: int, now: datetime
) -> _Job | _Unhandled:
    source, detail_type = body.get("source"), body.get("detail-type")
    if not isinstance(source, str) or not isinstance(detail_type, str):
        raise _BadMessage("unknown_kind")
    handlers: dict[str, str] = getattr(settings, "HYPERSCALE_TASKS_EVENT_HANDLERS", {})
    path = handlers.get(f"{source}/{detail_type}")
    if path is None:
        return _Unhandled(source, detail_type)
    task = _import_task(path, "bad_handler")
    run = partial(_call, task, [], {"event": body}, message_id, attempt, now)
    return _Job(path, run, _retry_for(task))


def _import_task(path: str, reason: str) -> Task[..., Any]:
    try:
        task = import_string(path)
    # Anything a broken module raises on import: retrying would not fix it.
    except Exception:
        raise _BadMessage(reason) from None
    if not isinstance(task, Task):
        raise _BadMessage(reason)
    return cast("Task[..., Any]", task)


def _retry_for(task: Task[..., Any]) -> timedelta:
    return getattr(task, "retry_for", DEFAULT_RETRY_FOR)


def _call(
    task: Task[..., Any],
    args: list[Any],
    kwargs: dict[str, Any],
    result_id: str,
    attempt: int,
    now: datetime,
) -> object:
    """Call the task as Django's ImmediateBackend does, with a context if it asks."""
    if not task.takes_context:
        return task.call(*args, **kwargs)
    result = TaskResult(
        task=task,
        id=result_id,
        status=TaskResultStatus.RUNNING,
        enqueued_at=None,
        started_at=now,
        last_attempted_at=now,
        finished_at=None,
        args=args,
        kwargs=kwargs,
        backend=task.backend,
        errors=[],
        worker_ids=[WORKER_ID] * attempt,
    )
    return task.call(TaskContext(task_result=result), *args, **kwargs)


def _run(
    job: _Job,
    record: dict[str, Any],
    attempt: int,
    queue_url: str,
    dlq_url: str,
    now: datetime,
) -> bool:
    fields = {"task": job.name, "attempt": attempt, "message_id": record["messageId"]}
    logger.info("tasks.started", extra=fields)
    started = time.monotonic()
    try:
        job.run()
    except Exception as exc:
        error = exc
    else:
        duration_ms = round((time.monotonic() - started) * 1000)
        logger.info("tasks.succeeded", extra=fields | {"duration_ms": duration_ms})
        _delete(record, queue_url)
        return True

    # Settled outside the except block, so an SQS error raised here does not
    # carry the task's exception (and its message) as its context.
    fields["error"] = type(error).__name__
    exc_info = exc_info_without_messages(error)
    delay = min(backoff_seconds(attempt), MAX_VISIBILITY_SECONDS)
    first_receive = int(record["attributes"]["ApproximateFirstReceiveTimestamp"])
    window_end = (
        EPOCH
        + timedelta(milliseconds=first_receive)
        + min(job.retry_for, MAX_RETRY_FOR)
    )
    if now + timedelta(seconds=delay) > window_end:
        logger.error("tasks.gave_up", exc_info=exc_info, extra=fields)
        _move_to_dlq(record, queue_url, dlq_url, "gave_up")
        return True
    # Logged first, as above, so the failure is on record even if SQS raises.
    logger.warning(
        "tasks.retrying", exc_info=exc_info, extra=fields | {"delay_seconds": delay}
    )
    sqs.client().change_message_visibility(
        QueueUrl=queue_url,
        ReceiptHandle=record["receiptHandle"],
        VisibilityTimeout=delay,
    )
    return False


def _move_to_dlq(
    record: dict[str, Any], queue_url: str, dlq_url: str, reason: str
) -> None:
    """Send the body, unchanged, to the DLQ, then delete it (not if the send raises)."""
    sqs.client().send_message(
        QueueUrl=dlq_url,
        MessageBody=record["body"],
        MessageAttributes={
            "hyperscale_reason": {"DataType": "String", "StringValue": reason}
        },
    )
    _delete(record, queue_url)


def _delete(record: dict[str, Any], queue_url: str) -> None:
    sqs.client().delete_message(
        QueueUrl=queue_url, ReceiptHandle=record["receiptHandle"]
    )


def exc_info_without_messages(exc: BaseException) -> ExcInfo:
    """`exc_info` for logging `exc` with its frames and class names, but no messages.

    Exception messages can carry personal data. Each exception in the chain is
    replaced by a message-free instance of its class carrying the original
    traceback, so any formatter that renders `record.exc_info` prints the
    frames, the chain and the class names only.
    """
    copies: dict[int, BaseException] = {}

    def strip(original: BaseException) -> BaseException:
        if id(original) in copies:
            return copies[id(original)]
        cls = type(original)
        try:
            bare = cls.__new__(cls)
        except Exception:
            # e.g. ExceptionGroup, whose __new__ needs arguments: a stand-in
            # that prints under the same name.
            names = {"__module__": cls.__module__, "__qualname__": cls.__qualname__}
            bare = type(cls.__name__, (Exception,), names)()
        copies[id(original)] = bare
        bare.__traceback__ = original.__traceback__
        if original.__cause__ is not None:
            bare.__cause__ = strip(original.__cause__)
        if original.__context__ is not None:
            bare.__context__ = strip(original.__context__)
        bare.__suppress_context__ = original.__suppress_context__
        return bare

    stripped = strip(exc)
    return type(stripped), stripped, exc.__traceback__
