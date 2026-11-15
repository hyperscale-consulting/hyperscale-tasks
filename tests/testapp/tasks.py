from datetime import timedelta

from django.tasks import task


# django-stubs does not know the backend task class's extra fields.
@task(retry_for=timedelta(hours=6))  # pyright: ignore[reportCallIssue, reportUntypedFunctionDecorator]
def slow_partner() -> None:
    """A task that declares its own retry window."""
