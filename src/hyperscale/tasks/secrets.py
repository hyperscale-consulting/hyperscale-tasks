"""Fetch secret environment variables from Secrets Manager.

Runs before ``django.setup()``, so this module imports no Django.
"""

import json
from collections.abc import Callable
from collections.abc import MutableMapping
from typing import Any
from typing import cast
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_secretsmanager import SecretsManagerClient

MAP_VAR = "HYPERSCALE_SECRETS_MAP"


def _client() -> SecretsManagerClient:
    """The one place a boto3 Secrets Manager client is made."""
    return boto3.client("secretsmanager")  # pyright: ignore[reportUnknownMemberType]


def _parse_map(raw: str) -> dict[str, tuple[str, str | None]]:
    """Parse ``{"<secret arn>": {"<ENV NAME>": "<json key>" | null}}``.

    Returns ``{name: (arn, field)}`` in map order. A name may appear under
    only one ARN.
    """
    try:
        data: Any = json.loads(raw)
    except ValueError:
        raise ValueError(f"{MAP_VAR} is not valid JSON") from None
    if not isinstance(data, dict):
        raise ValueError(f"{MAP_VAR} must be a JSON object")
    parsed: dict[str, tuple[str, str | None]] = {}
    for arn, entry in cast("dict[str, Any]", data).items():
        if not isinstance(entry, dict):
            raise ValueError(f"{MAP_VAR} entry for {arn} must be an object")
        for name, field in cast("dict[str, Any]", entry).items():
            if not (field is None or isinstance(field, str)):
                raise ValueError(
                    f"{MAP_VAR} field for {name} under {arn} must be a string or null"
                )
            if name in parsed:
                raise ValueError(f"{MAP_VAR} lists {name} under more than one ARN")
            parsed[name] = (arn, field)
    return parsed


def load_secrets(
    environ: MutableMapping[str, str],
    client_factory: Callable[[], Any] = _client,
) -> list[str]:
    """Write each variable in HYPERSCALE_SECRETS_MAP into ``environ``.

    Each distinct ARN is fetched once. Returns the names set, in map order.
    Error messages name variables and fields, never secret values.
    """
    raw = environ.get(MAP_VAR)
    if not raw:
        return []
    mapping = _parse_map(raw)
    client = client_factory()
    fetched: dict[str, str] = {}
    resolved: dict[str, str] = {}
    for name, (arn, field) in mapping.items():
        if arn not in fetched:
            fetched[arn] = client.get_secret_value(SecretId=arn)["SecretString"]
        secret = fetched[arn]
        if field is None:
            resolved[name] = secret
            continue
        try:
            document: Any = json.loads(secret)
        except ValueError:
            document = None
        if not isinstance(document, dict):
            raise ValueError(f"Secret for {name} is not a JSON object")
        if field not in document:
            raise KeyError(f"{name}: field {field!r} not found in secret {arn}")
        resolved[name] = str(document[field])  # pyright: ignore[reportUnknownArgumentType]
    environ.update(resolved)
    return list(resolved)
