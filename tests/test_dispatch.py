import logging
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError
from pytest_django.fixtures import Settings

from hyperscale.tasks import dispatch
from hyperscale.tasks.dispatch import process_record
from tests.fakes import FakeSQS
from tests.records import NOW
from tests.records import record
from tests.testapp import tasks
from tests.testapp.management.commands import testcmd

QUEUE = "https://sqs.example/q"
DLQ = "https://sqs.example/dlq"
ADDRESS = "secret@example.com"
DELETED = {"QueueUrl": QUEUE, "ReceiptHandle": "rh-1"}
ENVELOPE = {
    "version": "0",
    "id": "e-1",
    "source": "hyperscale.platform.test",
    "detail-type": "Platform Test",
    "detail": {"tenant_id": "t1"},
}


@pytest.fixture(autouse=True)
def clean_calls() -> Iterator[None]:
    tasks.calls.clear()
    testcmd.runs.clear()
    yield
    tasks.calls.clear()
    testcmd.runs.clear()


@pytest.fixture(autouse=True)
def info_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="hyperscale.tasks.dispatch")


def task_body(name: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
    return {
        "v": 1,
        "kind": "task",
        "task": f"tests.testapp.tasks.{name}",
        "args": list(args),
        "kwargs": kwargs,
    }


def to_dlq(body: str, reason: str) -> dict[str, Any]:
    return {
        "QueueUrl": DLQ,
        "MessageBody": body,
        "MessageAttributes": {
            "hyperscale_reason": {"DataType": "String", "StringValue": reason}
        },
    }


def events(caplog: pytest.LogCaptureFixture) -> list[tuple[str, str]]:
    return [
        (r.getMessage(), r.levelname)
        for r in caplog.records
        if r.name == "hyperscale.tasks.dispatch"
    ]


def event(caplog: pytest.LogCaptureFixture, name: str) -> dict[str, Any]:
    """The one record logged as `name`, as its attribute dict."""
    [found] = [r for r in caplog.records if r.getMessage() == name]
    return vars(found)


def run(rec: dict[str, Any]) -> bool:
    return process_record(rec, QUEUE, DLQ, now=NOW)


def test_task_success_deletes_and_logs(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture
) -> None:
    assert run(record(task_body("ok", 7, label="x") | {"id": "abc"})) is True

    assert tasks.calls == [("ok", (7, "x"))]
    assert fake_sqs.deleted == [DELETED]
    assert fake_sqs.sent == []
    assert fake_sqs.visibility_changes == []
    assert events(caplog) == [("tasks.started", "INFO"), ("tasks.succeeded", "INFO")]
    started = event(caplog, "tasks.started")
    assert started["task"] == "tests.testapp.tasks.ok"
    assert started["attempt"] == 1
    assert started["message_id"] == "msg-1"
    succeeded = event(caplog, "tasks.succeeded")
    assert succeeded["task"] == "tests.testapp.tasks.ok"
    assert succeeded["attempt"] == 1
    assert succeeded["message_id"] == "msg-1"
    assert isinstance(succeeded["duration_ms"], int)
    assert succeeded["duration_ms"] >= 0


def test_task_receives_attempt_through_context(fake_sqs: FakeSQS) -> None:
    assert run(record(task_body("ctx") | {"id": "abc"}, receive_count=3)) is True
    # A scheduled task has no outbox id; the SQS message id stands in.
    assert run(record(task_body("ctx"), receive_count=1)) is True

    assert tasks.calls == [("ctx", (3, "abc")), ("ctx", (1, "msg-1"))]


def test_failure_inside_window_backs_off(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture
) -> None:
    rec = record(
        task_body("boom"), receive_count=2, first_receive=NOW - timedelta(minutes=1)
    )

    assert run(rec) is False

    assert fake_sqs.visibility_changes == [
        {"QueueUrl": QUEUE, "ReceiptHandle": "rh-1", "VisibilityTimeout": 120}
    ]
    assert fake_sqs.deleted == []
    assert fake_sqs.sent == []
    assert events(caplog) == [("tasks.started", "INFO"), ("tasks.retrying", "WARNING")]
    retrying = event(caplog, "tasks.retrying")
    assert retrying["task"] == "tests.testapp.tasks.boom"
    assert retrying["attempt"] == 2
    assert retrying["message_id"] == "msg-1"
    assert retrying["delay_seconds"] == 120
    assert retrying["error"] == "ValueError"
    assert retrying["exc_info"] is not None


def test_failure_past_window_gives_up_to_dlq(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture
) -> None:
    rec = record(
        task_body("boom") | {"retry_for_s": 86400},
        receive_count=8,
        first_receive=NOW - timedelta(hours=23, minutes=59),
    )

    assert run(rec) is True

    assert fake_sqs.sent == [to_dlq(rec["body"], "gave_up")]
    assert fake_sqs.deleted == [DELETED]
    assert fake_sqs.visibility_changes == []
    assert events(caplog) == [("tasks.started", "INFO"), ("tasks.gave_up", "ERROR")]
    gave_up = event(caplog, "tasks.gave_up")
    assert gave_up["task"] == "tests.testapp.tasks.boom"
    assert gave_up["attempt"] == 8
    assert gave_up["message_id"] == "msg-1"
    assert gave_up["error"] == "ValueError"
    assert gave_up["exc_info"] is not None


def test_long_backoff_is_capped_below_the_sqs_receive_limit(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture
) -> None:
    # backoff_seconds(7) is 12 h, which SQS rejects once any time has passed
    # since the receive.
    assert run(record(task_body("boom"), receive_count=7)) is False

    assert [c["VisibilityTimeout"] for c in fake_sqs.visibility_changes] == [42240]
    assert event(caplog, "tasks.retrying")["delay_seconds"] == 42240


@pytest.mark.parametrize(
    "retry_for_s,gives_up",
    # Attempt 7 waits the capped 42240 s, not 43200 s: a window of exactly
    # 42240 s still retries.
    [(42240, False), (42239, True)],
)
def test_give_up_uses_the_capped_delay(
    fake_sqs: FakeSQS, retry_for_s: int, gives_up: bool
) -> None:
    rec = record(task_body("boom") | {"retry_for_s": retry_for_s}, receive_count=7)

    assert run(rec) is gives_up
    assert len(fake_sqs.sent) == int(gives_up)


@pytest.mark.parametrize(
    "seconds_ago,gives_up",
    # retry_for 600 s, attempt 1 backs off 30 s: the window ends at
    # first_receive + 600, so 570 s ago lands exactly on it.
    [(570, False), (571, True)],
)
def test_gives_up_only_when_backoff_overruns_the_window(
    fake_sqs: FakeSQS, seconds_ago: int, gives_up: bool
) -> None:
    rec = record(
        task_body("boom") | {"retry_for_s": 600},
        first_receive=NOW - timedelta(seconds=seconds_ago),
    )

    assert run(rec) is gives_up
    assert len(fake_sqs.sent) == int(gives_up)


def test_message_retry_for_overrides_task_default(fake_sqs: FakeSQS) -> None:
    eleven_min_ago = NOW - timedelta(minutes=11)
    # boom's own window is a day, so attempt 4 (32 min backoff) would retry...
    assert run(record(task_body("boom"), 4, eleven_min_ago)) is False
    # ...but the message's 10 minutes wins.
    with_override = record(task_body("boom") | {"retry_for_s": 600}, 4, eleven_min_ago)
    assert run(with_override) is True

    assert fake_sqs.sent == [to_dlq(with_override["body"], "gave_up")]


def test_task_retry_for_is_used_without_message_value(fake_sqs: FakeSQS) -> None:
    # fragile allows 6 h; attempt 6 backs off 8.5 h. A 1-day default would retry.
    rec = record(task_body("fragile"), 6, NOW - timedelta(hours=5))

    assert run(rec) is True
    assert fake_sqs.sent == [to_dlq(rec["body"], "gave_up")]


def test_message_retry_for_is_clamped_to_max(fake_sqs: FakeSQS) -> None:
    # Ten days asked for, four allowed: four days in, a 2 min backoff overruns.
    rec = record(
        task_body("boom") | {"retry_for_s": 10 * 86400}, 2, NOW - timedelta(days=4)
    )

    assert run(rec) is True
    assert fake_sqs.sent == [to_dlq(rec["body"], "gave_up")]


def test_command_runs_call_command_with_args(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture
) -> None:
    rec = record({"v": 1, "kind": "command", "command": "testcmd", "args": ["--x"]})

    assert run(rec) is True

    assert testcmd.runs == [{"x": True, "fail": False}]
    assert fake_sqs.deleted == [DELETED]
    assert events(caplog) == [("tasks.started", "INFO"), ("tasks.succeeded", "INFO")]
    assert event(caplog, "tasks.started")["task"] == "testcmd"
    assert event(caplog, "tasks.succeeded")["task"] == "testcmd"


def test_command_uses_one_hour_window(fake_sqs: FakeSQS) -> None:
    body = {"v": 1, "kind": "command", "command": "testcmd", "args": ["--fail"]}
    half_hour_ago = NOW - timedelta(minutes=30)

    # Attempt 3 backs off 8 min: inside the hour.
    assert run(record(body, 3, half_hour_ago)) is False
    # Attempt 4 backs off 32 min: past the hour (a task's day would retry).
    gives_up = record(body, 4, half_hour_ago)
    assert run(gives_up) is True

    assert [c["VisibilityTimeout"] for c in fake_sqs.visibility_changes] == [480]
    assert fake_sqs.sent == [to_dlq(gives_up["body"], "gave_up")]


@pytest.mark.parametrize(
    "path",
    [
        "tests.testapp.tasks.missing",  # no such attribute
        "no.such.module.fn",  # no such module
        "tests.testapp.tasks.calls",  # not a Task
        "notapath",
    ],
)
def test_unknown_task_path_is_bad_message(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture, path: str
) -> None:
    rec = record(task_body("ok", 1) | {"task": path})

    assert run(rec) is True

    assert events(caplog) == [("tasks.bad_message", "ERROR")]
    bad = event(caplog, "tasks.bad_message")
    assert bad["message_id"] == "msg-1"
    assert bad["reason"] == "bad_task"
    assert fake_sqs.sent == [to_dlq(rec["body"], "bad_message")]
    assert fake_sqs.deleted == [DELETED]
    assert tasks.calls == []


def test_non_json_body_is_bad_message(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture
) -> None:
    assert run(record("not json {")) is True

    assert event(caplog, "tasks.bad_message")["reason"] == "not_json"
    assert fake_sqs.sent == [to_dlq("not json {", "bad_message")]
    assert fake_sqs.deleted == [DELETED]


BAD_BODIES: list[tuple[object, str]] = [
    ("[1, 2]", "not_object"),
    ('"text"', "not_object"),
    (task_body("ok", 1) | {"v": 2}, "bad_version"),
    (task_body("ok", 1) | {"v": "1"}, "bad_version"),
    ({"v": 1, "kind": "sweep"}, "unknown_kind"),
    ({"hello": "world"}, "unknown_kind"),
    ({"source": "hyperscale.platform.test"}, "unknown_kind"),
    ({"v": 1, "kind": "task", "task": "tests.testapp.tasks.ok"}, "bad_task"),
    (task_body("ok") | {"args": {"a": 1}}, "bad_task"),
    (task_body("ok") | {"kwargs": []}, "bad_task"),
    (task_body("ok") | {"task": 7}, "bad_task"),
    (task_body("ok", 1) | {"retry_for_s": "600"}, "bad_task"),
    (
        {"v": 1, "kind": "command", "command": "nosuch", "args": []},
        "unknown_command",
    ),
    ({"v": 1, "kind": "command", "command": "testcmd", "args": [1]}, "bad_command"),
    ({"v": 1, "kind": "command", "command": "testcmd"}, "bad_command"),
]


@pytest.mark.parametrize("body,reason", BAD_BODIES)
def test_malformed_bodies_are_bad_messages(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture, body: object, reason: str
) -> None:
    rec = record(body)

    assert run(rec) is True

    assert events(caplog) == [("tasks.bad_message", "ERROR")]
    assert event(caplog, "tasks.bad_message")["reason"] == reason
    assert fake_sqs.sent == [to_dlq(rec["body"], "bad_message")]
    assert fake_sqs.deleted == [DELETED]


def test_event_dispatches_by_source_and_detail_type(
    fake_sqs: FakeSQS, settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    handler = "tests.testapp.tasks.on_platform_test"
    settings.HYPERSCALE_TASKS_EVENT_HANDLERS = {
        "hyperscale.platform.test/Platform Test": handler
    }

    assert run(record(ENVELOPE)) is True

    assert tasks.calls == [("on_platform_test", ENVELOPE)]
    assert fake_sqs.deleted == [DELETED]
    assert events(caplog) == [("tasks.started", "INFO"), ("tasks.succeeded", "INFO")]
    assert event(caplog, "tasks.started")["task"] == handler


def test_unloadable_event_handler_is_bad_message(
    fake_sqs: FakeSQS, settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    settings.HYPERSCALE_TASKS_EVENT_HANDLERS = {
        "hyperscale.platform.test/Platform Test": "tests.testapp.tasks.missing"
    }
    rec = record(ENVELOPE)

    assert run(rec) is True

    assert event(caplog, "tasks.bad_message")["reason"] == "bad_handler"
    assert fake_sqs.sent == [to_dlq(rec["body"], "bad_message")]
    assert fake_sqs.deleted == [DELETED]


def test_unhandled_event_logs_and_deletes(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture
) -> None:
    assert run(record(ENVELOPE)) is True

    assert events(caplog) == [("tasks.unhandled_event", "ERROR")]
    unhandled = event(caplog, "tasks.unhandled_event")
    assert unhandled["source"] == "hyperscale.platform.test"
    assert unhandled["detail_type"] == "Platform Test"
    assert fake_sqs.deleted == [DELETED]
    assert fake_sqs.sent == []


def test_error_field_is_class_name_only(
    fake_sqs: FakeSQS, caplog: pytest.LogCaptureFixture
) -> None:
    body = task_body("boom", ADDRESS)
    assert run(record(body)) is False  # retrying
    assert run(record(body, 8, NOW - timedelta(hours=23))) is True  # gave up

    logged = [r for r in caplog.records if r.name == "hyperscale.tasks.dispatch"]
    failures = [
        r for r in logged if r.getMessage() in ("tasks.retrying", "tasks.gave_up")
    ]
    assert len(failures) == 2
    for r in failures:
        assert vars(r)["error"] == "ValueError"
        assert r.exc_info is not None
        # What the app's JSON formatter does with a record's exc_info.
        rendered = logging.Formatter().formatException(r.exc_info)
        assert ADDRESS not in rendered
        # The traceback is still there: frames, the chain, and the class names.
        assert rendered.startswith("Traceback (most recent call last):")
        assert ", in boom\n" in rendered
        assert "raise ValueError(message) from exc" in rendered
        assert "direct cause of the following exception" in rendered
        assert rendered.endswith("ValueError")
        assert "\nKeyError\n" in rendered
    assert ADDRESS not in caplog.text
    assert "raise ValueError(message) from exc" in caplog.text
    for r in caplog.records:
        logging.Formatter().format(r)  # fills exc_text
        assert ADDRESS not in r.getMessage()
        for value in vars(r).values():
            assert ADDRESS not in str(value)


def test_give_up_never_deletes_when_dlq_send_fails(fake_sqs: FakeSQS) -> None:
    fake_sqs.fail_sends = True

    with pytest.raises(ClientError):
        run(record(task_body("boom"), 8, NOW - timedelta(hours=23)))
    with pytest.raises(ClientError):
        run(record("not json {"))

    assert fake_sqs.deleted == []


def test_base_exceptions_propagate(fake_sqs: FakeSQS) -> None:
    with pytest.raises(SystemExit):
        run(record(task_body("stop")))

    assert fake_sqs.deleted == []
    assert fake_sqs.sent == []
    assert fake_sqs.visibility_changes == []


@pytest.mark.parametrize(
    "rec,fail_sends",
    [
        pytest.param(record(task_body("ok", 1)), False, id="success"),
        pytest.param(record(task_body("boom")), False, id="retry"),
        pytest.param(
            record(task_body("boom"), 8, NOW - timedelta(hours=23)), False, id="gave_up"
        ),
        pytest.param(record("not json {"), False, id="bad_message"),
        pytest.param(record(ENVELOPE), False, id="unhandled_event"),
        pytest.param(record("not json {"), True, id="dlq_send_fails"),
        pytest.param(record(task_body("stop")), False, id="base_exception"),
    ],
)
def test_connections_closed_on_every_path(
    fake_sqs: FakeSQS,
    monkeypatch: pytest.MonkeyPatch,
    rec: dict[str, Any],
    fail_sends: bool,
) -> None:
    closed: list[None] = []
    monkeypatch.setattr(dispatch.connections, "close_all", lambda: closed.append(None))
    fake_sqs.fail_sends = fail_sends

    try:
        run(rec)
    except ClientError, SystemExit:
        pass

    assert closed == [None]


def test_exception_groups_are_stripped_too() -> None:
    def fail() -> None:
        raise ExceptionGroup(ADDRESS, [ValueError(ADDRESS)])

    with pytest.raises(ExceptionGroup) as caught:
        fail()
    rendered = logging.Formatter().formatException(
        dispatch.exc_info_without_messages(caught.value)
    )

    assert ADDRESS not in rendered
    assert ", in fail\n" in rendered
    assert rendered.endswith("ExceptionGroup")
