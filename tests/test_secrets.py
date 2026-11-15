import json
from typing import Any

import pytest

from hyperscale.tasks.secrets import load_secrets

ARN_A = "arn:aws:secretsmanager:eu-west-2:111111111111:secret:db-AbCdEf"
ARN_B = "arn:aws:secretsmanager:eu-west-2:111111111111:secret:key-GhIjKl"


class FakeSecretsManager:
    def __init__(self, secrets: dict[str, str]) -> None:
        self.secrets = secrets
        self.calls: list[str] = []

    def get_secret_value(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs["SecretId"])
        return {"SecretString": self.secrets[kwargs["SecretId"]]}


def _map(entries: dict[str, dict[str, str | None]]) -> str:
    return json.dumps(entries)


def test_sets_fields_from_json_secret() -> None:
    fake = FakeSecretsManager({ARN_A: json.dumps({"password": "pw", "user": "u"})})
    environ = {"HYPERSCALE_SECRETS_MAP": _map({ARN_A: {"DB_PASSWORD": "password"}})}
    assert load_secrets(environ, lambda: fake) == ["DB_PASSWORD"]
    assert environ["DB_PASSWORD"] == "pw"


def test_null_field_uses_whole_secret_string() -> None:
    fake = FakeSecretsManager({ARN_B: "raw-key-value"})
    environ = {"HYPERSCALE_SECRETS_MAP": _map({ARN_B: {"API_KEY": None}})}
    load_secrets(environ, lambda: fake)
    assert environ["API_KEY"] == "raw-key-value"


def test_fetches_each_arn_once() -> None:
    fake = FakeSecretsManager({ARN_A: json.dumps({"password": "pw", "user": "u"})})
    environ = {
        "HYPERSCALE_SECRETS_MAP": _map(
            {ARN_A: {"DB_PASSWORD": "password", "DB_USER": "user"}}
        )
    }
    assert load_secrets(environ, lambda: fake) == ["DB_PASSWORD", "DB_USER"]
    assert fake.calls == [ARN_A]
    assert environ["DB_USER"] == "u"


def test_missing_field_raises_with_name_not_value() -> None:
    fake = FakeSecretsManager({ARN_A: json.dumps({"user": "sensitive-user"})})
    environ = {"HYPERSCALE_SECRETS_MAP": _map({ARN_A: {"DB_PASSWORD": "password"}})}
    with pytest.raises(KeyError) as excinfo:
        load_secrets(environ, lambda: fake)
    message = str(excinfo.value)
    assert "DB_PASSWORD" in message
    assert "password" in message
    assert "sensitive-user" not in message
    assert "DB_PASSWORD" not in environ


def test_non_object_secret_with_field_raises_without_content() -> None:
    fake = FakeSecretsManager({ARN_A: "plain-secret-text"})
    environ = {"HYPERSCALE_SECRETS_MAP": _map({ARN_A: {"DB_PASSWORD": "password"}})}
    with pytest.raises(ValueError, match="DB_PASSWORD") as excinfo:
        load_secrets(environ, lambda: fake)
    assert "plain-secret-text" not in str(excinfo.value)


def test_absent_map_is_a_no_op() -> None:
    fake = FakeSecretsManager({})
    environ: dict[str, str] = {}
    assert load_secrets(environ, lambda: fake) == []
    assert environ == {}
    assert fake.calls == []


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        "[]",
        '{"arn:a": "X"}',
        '{"arn:a": ["X"]}',
        '{"arn:a": {"X": 1}}',
        '{"arn:a": {"X": ["f"]}}',
    ],
)
def test_malformed_map_raises_value_error(raw: str) -> None:
    with pytest.raises(ValueError, match="HYPERSCALE_SECRETS_MAP"):
        load_secrets({"HYPERSCALE_SECRETS_MAP": raw}, lambda: FakeSecretsManager({}))


def test_two_arns_set_their_own_variables() -> None:
    fake = FakeSecretsManager(
        {ARN_A: json.dumps({"password": "pw"}), ARN_B: "raw-key-value"}
    )
    environ = {
        "HYPERSCALE_SECRETS_MAP": _map(
            {ARN_A: {"DB_PASSWORD": "password"}, ARN_B: {"API_KEY": None}}
        )
    }
    assert load_secrets(environ, lambda: fake) == ["DB_PASSWORD", "API_KEY"]
    assert fake.calls == [ARN_A, ARN_B]
    assert environ["DB_PASSWORD"] == "pw"
    assert environ["API_KEY"] == "raw-key-value"


def test_variable_under_two_arns_raises_before_fetching() -> None:
    fake = FakeSecretsManager({})
    raw = _map({ARN_A: {"DB_PASSWORD": "password"}, ARN_B: {"DB_PASSWORD": None}})
    with pytest.raises(ValueError, match="DB_PASSWORD") as excinfo:
        load_secrets({"HYPERSCALE_SECRETS_MAP": raw}, lambda: fake)
    assert "HYPERSCALE_SECRETS_MAP" in str(excinfo.value)
    assert fake.calls == []
