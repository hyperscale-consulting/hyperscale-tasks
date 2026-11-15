from datetime import timedelta
from typing import Any

from django.tasks import task
from django.tasks import TaskContext

# What the dispatch test tasks were called with; tests clear it.
calls: list[tuple[str, Any]] = []


# django-stubs does not know the backend task class's extra fields.
@task(retry_for=timedelta(hours=6))  # pyright: ignore[reportCallIssue, reportUntypedFunctionDecorator]
def slow_partner() -> None:
    """A task that declares its own retry window."""


@task
def notify(user_id: int) -> None:  # pyright: ignore[reportUnusedFunction]
    """A plain task taking one argument."""


@task
def ok(value: int, *, label: str = "") -> None:
    calls.append(("ok", (value, label)))


@task
def boom(message: str = "boom") -> None:
    """Fails with `message` in both the exception and its cause."""
    try:
        raise KeyError(message)
    except KeyError as exc:
        raise ValueError(message) from exc


@task(retry_for=timedelta(hours=6))  # pyright: ignore[reportCallIssue, reportUntypedFunctionDecorator]
def fragile() -> None:
    """Fails, with its own 6 hour retry window."""
    raise RuntimeError("fragile")


@task
def stop() -> None:
    raise SystemExit(1)


@task(takes_context=True)
def ctx(context: TaskContext[..., None]) -> None:
    calls.append(("ctx", (context.attempt, context.task_result.id)))


@task
def on_platform_test(event: dict[str, Any]) -> None:
    calls.append(("on_platform_test", event))
