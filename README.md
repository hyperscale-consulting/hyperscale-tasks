# hyperscale-tasks

A `django.tasks` backend for Django 6.1 on a transactional outbox and SQS, with
the AWS Lambda handler that runs the tasks. The same handler runs scheduled
management commands and AWS (EventBridge) events. Requires Python 3.14.

How it works: `task.enqueue()` writes a row to an outbox table in the caller's
database transaction, and sends it to SQS after the transaction commits. If the
send fails, or the task is due later, a sweep sends it. A Lambda function
consumes the queue, runs the task and settles the message: delete on success,
back off and retry on failure, dead-letter queue (DLQ) when it gives up.

## Installation

```sh
uv add hyperscale-tasks
```

Add the app and run `migrate`, which creates the outbox table
(`hyperscale_tasks_outbox`):

```python
INSTALLED_APPS = [
    # ...
    "hyperscale.tasks",
]
```

Settings:

```python
JOBS_QUEUE_URL = os.environ.get("JOBS_QUEUE_URL", "")
TASKS = {
    "default": (
        {"BACKEND": "hyperscale.tasks.backend.ImmediateBackend"}
        if DEBUG and not JOBS_QUEUE_URL
        else {
            "BACKEND": "hyperscale.tasks.backend.SQSOutboxBackend",
            "OPTIONS": {"QUEUE_URL": JOBS_QUEUE_URL},
        }
    )
}
# When HYPERSCALE_RUNTIME == "lambda": no "pool" in DATABASES["default"]["OPTIONS"], CONN_MAX_AGE = 0.
HYPERSCALE_TASKS_EVENT_HANDLERS = {  # optional
    "hyperscale.platform.test/Platform Test": "myapp.tasks.on_platform_test",
}
```

Startup checks:

- Outside `DEBUG`, Django refuses to start (`ImproperlyConfigured`, raised from
  the app's `ready()`) if `TASKS["default"]` is `SQSOutboxBackend` with an
  empty `QUEUE_URL`. An outbox with no queue would never deliver.
- When `HYPERSCALE_RUNTIME` is `lambda`, the system check
  `hyperscale_tasks.E001` fails if a database has `"pool"` in `OPTIONS` or a
  non-zero `CONN_MAX_AGE`. Lambda freezes the environment between invocations,
  so a held connection goes stale. Lambda never runs system checks, so the
  handler also enforces E001 at cold start: after `django.setup()` it raises
  `ImproperlyConfigured` naming the error, and every later invoke in that
  environment fails too, until Lambda replaces it.

## Writing tasks

```python
from datetime import timedelta
from django.tasks import task


@task(retry_for=timedelta(hours=6))  # optional; default 1 day
def notify_partner(booking_id: int) -> None: ...


notify_partner.enqueue(booking.id)
notify_partner.using(run_after=when).enqueue(booking.id)
```

Rules for task authors:

- **Make tasks idempotent.** Delivery is at least once, and a send can be
  duplicated.
- **Pass ids, not objects or personal data.** Arguments must be
  JSON-serialisable and small: an SQS message is at most 256 KB. An
  unserialisable argument (`TypeError`) or a message body over 262144 bytes
  (`ValueError`) fails at `enqueue`, before anything is written.
- **Keep a task's import path and argument shape valid for one release after
  you change them.** Messages enqueued by the old web release are consumed by
  the new worker.
- Enqueue inside the transaction that creates the data the task needs. The send
  happens only on commit, so the task never runs against rows that were rolled
  back.

### Retries and `retry_for`

`retry_for` is a field of `hyperscale.tasks.RetryingTask`, the task class of
both backends. It is how long a task keeps being retried after it first
fails, counted from the first receive. It must be between 1 minute and 4 days
(`MAX_RETRY_FOR`) or the task raises `ValueError` when defined. The default is
1 day. Scheduled commands always use 1 hour, so a failing nightly command
alerts that night.

A raised exception is a handled failure. The handler hides the message for
`min(30 * 4 ** (attempt - 1), 43200)` seconds (30 s, 2 min, 8 min, ... up to
12 h) with `ChangeMessageVisibility`, and reports the record in
`batchItemFailures`. The visibility timeout actually applied is capped at
42240 s: SQS's 12-hour maximum counts from the receive, so the cap is 12 h less
the 15-minute Lambda maximum and 60 s. The give-up decision uses the capped
value.

When the next attempt would fall after `retry_for` has elapsed, the handler
gives up: it logs `tasks.gave_up` at ERROR, sends the original body to the
DLQ with the message attribute `hyperscale_reason=gave_up`, and deletes it
from the main queue.

A crash (timeout, out of memory) leaves the message to reappear after its
visibility timeout and reach the DLQ through the queue's `maxReceiveCount`.

### Scheduling and the outbox

- `run_after` within 15 minutes is sent at once with SQS `DelaySeconds`.
- A later `run_after` waits in the outbox. The sweep (run every 5 minutes)
  sends it once it is within 15 minutes.
- The sweep also sends rows whose post-commit send failed (rows at least
  30 seconds old), and deletes sent rows after 7 days. It logs
  `tasks.outbox_swept` with `sent`, `deleted` and `oldest_unsent_age_seconds`.

### Unsupported

- Results: `get_result` is not supported, and `enqueue` returns a `TaskResult`
  in `READY` status that never updates.
- Priority.
- Non-default queues: a task with another `queue_name` is rejected with
  `InvalidTask`.
- Async tasks.
- Django's `task_enqueued`, `task_started` and `task_finished` signals: the
  Lambda handler does not send them.
- Retrying a task module that fails to import. The handler treats any import
  failure, even a transient one, as a bad message: it goes to the DLQ as
  `bad_message`, and needs a redrive once the cause is fixed.

## Event handlers

`HYPERSCALE_TASKS_EVENT_HANDLERS` maps `"<source>/<detail-type>"` to the import
path of a task. When the queue receives an EventBridge event with that source
and detail type, the handler task is called with one keyword argument,
`event`: the whole EventBridge envelope, as a dict.

```python
@task
def on_platform_test(event: dict) -> None: ...
```

The handler follows the same rules and retries as any other task (the task's
own `retry_for`, or the default). An event with no handler is logged as
`tasks.unhandled_event` at ERROR and dropped.

## The Lambda handler

`hyperscale.tasks.lambda_handler.handler` is the Lambda entry point for the
tenant's image. It handles two kinds of invocation:

- **SQS events.** Each record is one of: a task (`kind: "task"`), a
  management command (`kind: "command"`, run with `call_command`, 1-hour retry
  window) or an EventBridge event (no `v` field). The handler returns
  `{"batchItemFailures": [...]}` for records that are backing off.
- **Sweep** (`{"v": 1, "kind": "sweep"}`, a direct invoke). Runs the outbox
  sweep and returns `{"swept": true}`.

On a cold start it loads secrets, then calls `django.setup()`.

A bad message is moved to the DLQ with `hyperscale_reason=bad_message`, logged
as `tasks.bad_message` with a `reason`, and never retried. Bad means: the body
is not JSON, not an object, or has an unsupported version or unknown kind; the
task path does not import or is not a `Task`; the command is unknown; or an
event handler cannot be loaded. If SQS itself errors while settling a record,
the invocation fails and SQS redelivers, so a message is never deleted unless
it reached the DLQ.

### Environment variables

| Name | Where | Value |
|---|---|---|
| `JOBS_QUEUE_URL` | web, migrations, schedules, Lambda | URL of the main queue |
| `JOBS_DLQ_URL` | Lambda | URL of the DLQ |
| `HYPERSCALE_RUNTIME` | web, migrations, schedules: `ecs`; Lambda: `lambda` | |
| `HYPERSCALE_SECRETS_MAP` | Lambda | JSON grouped by secret, so each ARN appears once: `{"<secret arn>": {"<ENV NAME>": "<json key>" \| null}}`. A `null` field means the whole `SecretString`. A variable may appear under only one ARN. |
| `DJANGO_SETTINGS_MODULE` | Lambda | the app's settings module |

### Logging

Events, all as `logging` records with `extra` fields: `tasks.started`,
`tasks.succeeded` (with `duration_ms`), `tasks.retrying` (WARNING, with
`delay_seconds`), `tasks.gave_up` (ERROR), `tasks.bad_message` (ERROR),
`tasks.unhandled_event` (ERROR), `tasks.send_deferred` (WARNING) and
`tasks.outbox_swept`. Task events carry `task`, `attempt` and `message_id`.
The events are logged to the `hyperscale.tasks` logger and its children.

The app's `LOGGING` must route the `hyperscale.tasks` logger, at INFO, to its
JSON handler on stdout. The `jobs-outbox` alarm reads the INFO
`tasks.outbox_swept` line, and `jobs-errors` reads the ERROR lines; neither
fires if the lines are dropped. For example, with a handler named `json`:

```python
LOGGING = {
    # ...
    "loggers": {
        "hyperscale.tasks": {"handlers": ["json"], "level": "INFO"},
    },
}
```

Logs never include exception messages or task arguments: errors log the class
name, and tracebacks are logged with their frames and class names but with
messages stripped. The frames are real, though. An error-reporting integration
that captures local variables (for example Sentry with locals enabled) could
still see arguments, so configure it not to.

## Local development

`ImmediateBackend` runs a task as soon as it is enqueued, in the same process,
like Django's own immediate backend, and accepts `retry_for`. It accepts
`run_after` but ignores the delay and runs the task at once. Like
`SQSOutboxBackend`, it rejects async tasks and a non-default `priority` with
`InvalidTask`, so tests catch them. Use it for development and tests only.

```python
TASKS = {"default": {"BACKEND": "hyperscale.tasks.backend.ImmediateBackend"}}
```

## Smoke test

`tests/smoke/run.sh` runs the Lambda handler in a container, as a tenant's
Lambda image runs it, and sends it one SQS record. It is a manual check, not a
CI step, and needs Docker and curl:

```sh
tests/smoke/run.sh
```

The script:

1. builds `tests/smoke/Dockerfile` (`python:3.14-slim`, the package installed
   into `/app/.venv`, the Lambda Runtime Interface Emulator, and
   `tests/smoke/settings.py`: SQLite, `DEBUG = True`, the immediate backend);
2. starts a `motoserver/moto` container as the SQS endpoint
   (`AWS_ENDPOINT_URL_SQS`, dummy credentials, `eu-west-2`), creates the queues,
   and sends and receives a message for `tests.smoke.tasks.ok`, to get a real
   receipt handle;
3. runs the image under the emulator:
   `docker run -p 127.0.0.1:9000:8080 --entrypoint /usr/local/bin/aws-lambda-rie <image> /app/.venv/bin/python -m awslambdaric hyperscale.tasks.lambda_handler.handler`;
4. posts the record to
   `http://localhost:9000/2015-03-31/functions/function/invocations`.

It passes when the response is `{"batchItemFailures": []}` and the queue is
empty afterwards. It removes its containers, network and image when it exits.
