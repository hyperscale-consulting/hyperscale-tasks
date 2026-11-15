"""Settings for the container smoke test: SQLite, no queue, DEBUG."""

SECRET_KEY = "smoke"
DEBUG = True
INSTALLED_APPS = ["hyperscale.tasks"]
DATABASES = {
    "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
}
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
TASKS = {
    "default": {"BACKEND": "hyperscale.tasks.backend.ImmediateBackend"},
}
