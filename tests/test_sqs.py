from collections.abc import Iterator

import boto3
import pytest
from botocore.config import Config

from hyperscale.tasks import sqs


class FakeSession:
    """Stands in for `boto3.session.Session`; records the clients asked for."""

    made: list[FakeSession] = []

    def __init__(self) -> None:
        self.clients: list[tuple[str, Config]] = []
        FakeSession.made.append(self)

    def client(self, service_name: str, *, config: Config) -> object:
        self.clients.append((service_name, config))
        return object()


@pytest.fixture
def sessions(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[FakeSession]]:
    """The real `sqs.client()`, uncached, over a fake session (nothing reaches AWS)."""

    def default_session_client(*args: object, **kwargs: object) -> object:
        raise AssertionError("made from boto3's shared default session")

    FakeSession.made = []
    monkeypatch.setattr(boto3.session, "Session", FakeSession)
    monkeypatch.setattr(boto3, "client", default_session_client)
    sqs.client.cache_clear()
    yield FakeSession.made
    sqs.client.cache_clear()


def test_client_has_short_timeouts_and_few_retries(
    sessions: list[FakeSession],
) -> None:
    sqs.client()

    [session] = sessions
    [(service, config)] = session.clients
    assert service == "sqs"
    # vars(): botocore-stubs declares Config's arguments but not its attributes.
    options = vars(config)
    assert options["connect_timeout"] == 2
    assert options["read_timeout"] == 5
    assert options["retries"] == {"mode": "standard", "max_attempts": 2}


def test_client_is_made_once_from_its_own_session(
    sessions: list[FakeSession],
) -> None:
    assert sqs.client() is sqs.client()

    [session] = sessions
    assert len(session.clients) == 1
