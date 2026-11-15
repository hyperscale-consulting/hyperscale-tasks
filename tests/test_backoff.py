import pytest

from hyperscale.tasks.backoff import backoff_seconds


@pytest.mark.parametrize(
    "attempt,seconds",
    [
        (1, 30),
        (2, 120),
        (3, 480),
        (4, 1920),
        (5, 7680),
        (6, 30720),
        (7, 43200),
        (20, 43200),
    ],
)
def test_backoff_schedule(attempt: int, seconds: int) -> None:
    assert backoff_seconds(attempt) == seconds
