import os
from typing import Any
from urllib.parse import unquote
from urllib.parse import urlsplit

os.environ.setdefault("ENV", "test")

SECRET_KEY = "test"
INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "hyperscale.tasks",
    "tests.testapp",
]


def _database(url: str) -> dict[str, Any]:
    """`postgres://user:password@host:port/name` as Django database settings."""
    parts = urlsplit(url)
    if parts.scheme not in ("postgres", "postgresql"):
        raise ValueError(f"unsupported DATABASE_URL scheme {parts.scheme!r}")
    return {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": unquote(parts.path.lstrip("/")),
        "USER": unquote(parts.username or ""),
        "PASSWORD": unquote(parts.password or ""),
        "HOST": parts.hostname or "",
        "PORT": str(parts.port or ""),
    }


_database_url = os.environ.get("DATABASE_URL")
DATABASES = {
    "default": _database(_database_url)
    if _database_url
    else {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}
}
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
TASKS = {
    "default": {"BACKEND": "hyperscale.tasks.backend.ImmediateBackend"},
}
