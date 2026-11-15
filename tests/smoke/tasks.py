from django.tasks import task


@task
def ok() -> None:
    """Does nothing: the smoke test checks the round trip through the handler."""
