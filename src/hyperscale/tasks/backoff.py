MAX_BACKOFF_SECONDS = 43200  # 12 hours


def backoff_seconds(attempt: int) -> int:
    """How long to hide a message after its `attempt`-th receive failed."""
    return min(30 * 4 ** (attempt - 1), MAX_BACKOFF_SECONDS)
