import os
from typing import Any

from django.conf import settings
from django.core.checks import CheckMessage
from django.core.checks import Error


def check_lambda_connections(app_configs: object, **kwargs: Any) -> list[CheckMessage]:
    """hyperscale_tasks.E001: no pooled or persistent connections on Lambda.

    Lambda freezes the environment between invocations; a connection held
    open across the freeze goes stale and holds a database slot.
    """
    if os.environ.get("HYPERSCALE_RUNTIME") != "lambda":
        return []
    errors: list[CheckMessage] = []
    for alias, database in settings.DATABASES.items():
        if (
            database.get("OPTIONS", {}).get("pool")
            or database.get("CONN_MAX_AGE", 0) != 0
        ):
            errors.append(
                Error(
                    f"Database {alias!r} keeps connections open between Lambda "
                    "invocations.",
                    hint=(
                        f"When HYPERSCALE_RUNTIME is 'lambda', remove 'pool' from "
                        f"DATABASES[{alias!r}]['OPTIONS'] and set CONN_MAX_AGE = 0."
                    ),
                    id="hyperscale_tasks.E001",
                )
            )
    return errors
