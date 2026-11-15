from argparse import ArgumentParser
from typing import Any

from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

# The options each run was called with; tests clear it.
runs: list[dict[str, Any]] = []


class Command(BaseCommand):
    help = "Records its options; fails on --fail."

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("--x", action="store_true")
        parser.add_argument("--fail", action="store_true")

    def handle(self, *args: Any, **options: Any) -> None:
        runs.append({"x": options["x"], "fail": options["fail"]})
        if options["fail"]:
            raise CommandError("testcmd failed")
