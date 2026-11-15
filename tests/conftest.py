import pytest

from tests.fakes import FakeSQS


@pytest.fixture
def fake_sqs(monkeypatch: pytest.MonkeyPatch) -> FakeSQS:
    fake = FakeSQS()
    monkeypatch.setattr("hyperscale.tasks.sqs.client", lambda: fake)
    return fake
