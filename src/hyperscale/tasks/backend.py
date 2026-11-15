from django.tasks.backends.immediate import ImmediateBackend as _ImmediateBackend

from hyperscale.tasks.retrying import RetryingTask


class ImmediateBackend(_ImmediateBackend):
    """Django's immediate backend, building `RetryingTask`s."""

    # run_after is accepted, though this backend still runs tasks at once.
    supports_defer = True
    task_class = RetryingTask
