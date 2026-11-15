# Contributing

## Development setup

Requires [uv](https://docs.astral.sh/uv/) and Python 3.14+.

```bash
git clone https://github.com/hyperscale-consulting/hyperscale-tasks.git
cd hyperscale-tasks
uv sync
uvx pre-commit install
```

## Running checks

```bash
uv run pytest                        # tests
uv run ruff check .                  # lint
uv run ruff format --check .         # formatting
uv run pyright                       # types
uv run pre-commit run --all-files    # all pre-commit hooks
```

CI runs the same checks on every PR.

## Pull requests

- Add tests for behavior changes.
- Update `CHANGELOG.md` under `[Unreleased]`.
- Keep the public API documented in `README.md`.
