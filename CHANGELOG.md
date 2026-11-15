# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-15

### Added

- `hyperscale.tasks.backend.SQSOutboxBackend`, a `django.tasks` backend that
  writes each task to an outbox table in the caller's transaction and sends it
  to SQS after commit. `run_after` within 15 minutes uses SQS `DelaySeconds`;
  later tasks wait for the sweep. Results, priority, non-default queues and
  async tasks are not supported.
- `hyperscale.tasks.backend.ImmediateBackend` for development and tests: runs
  tasks at once and ignores `run_after`.
- `hyperscale.tasks.RetryingTask` with `retry_for` (1 minute to 4 days,
  default 1 day).
- The `hyperscale.tasks` Django app with the outbox model and migration.
- `hyperscale.tasks.lambda_handler.handler`: runs tasks, management commands
  and AWS events (via `HYPERSCALE_TASKS_EVENT_HANDLERS`) from SQS, retries
  with backoff up to 12 hours (visibility capped at 42240 s), and moves
  messages to the DLQ with `hyperscale_reason` `gave_up` or `bad_message`.
- The outbox sweep, invoked directly on the handler: sends unsent and due rows
  and deletes sent rows after 7 days.
- Loading of Lambda secrets from Secrets Manager (`HYPERSCALE_SECRETS_MAP`).
- System check `hyperscale_tasks.E001` (no connection pool or `CONN_MAX_AGE`
  on Lambda) and a startup refusal when `SQSOutboxBackend` has no queue URL
  outside `DEBUG`.
- Structured `tasks.*` log events that never include exception messages or
  task arguments.
- A manual Docker smoke test, `tests/smoke/run.sh`.
