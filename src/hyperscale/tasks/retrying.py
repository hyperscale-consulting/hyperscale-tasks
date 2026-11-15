from dataclasses import dataclass
from datetime import timedelta

from django.tasks import Task

DEFAULT_RETRY_FOR = timedelta(days=1)
MAX_RETRY_FOR = timedelta(days=4)
COMMAND_RETRY_FOR = timedelta(hours=1)
MIN_RETRY_FOR = timedelta(minutes=1)


# Django's Task is generic in the stubs only; it is not subscriptable at runtime.
@dataclass(frozen=True, slots=True, kw_only=True)
class RetryingTask(Task):  # pyright: ignore[reportMissingTypeArgument]
    """A task that is retried for up to `retry_for` after it first fails."""

    retry_for: timedelta = DEFAULT_RETRY_FOR

    def __post_init__(self) -> None:
        if not MIN_RETRY_FOR <= self.retry_for <= MAX_RETRY_FOR:
            raise ValueError(
                f"retry_for must be between {MIN_RETRY_FOR} and {MAX_RETRY_FOR}"
            )
        super().__post_init__()  # pyright: ignore[reportUnknownMemberType]
