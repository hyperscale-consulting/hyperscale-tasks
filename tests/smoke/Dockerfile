# A throwaway image for the container smoke test (tests/smoke/run.sh); not
# built in CI. Build from the repository root:
#   docker build -f tests/smoke/Dockerfile -t hyperscale-tasks-smoke .
FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.18 /uv /usr/local/bin/uv

# The Lambda Runtime Interface Emulator, for the build platform's architecture.
ARG TARGETARCH
ARG RIE_VERSION=v1.38
RUN set -eu; \
    case "$TARGETARCH" in \
      arm64) asset=aws-lambda-rie-arm64; \
             sha=65ade4687c22c308ff7acac6974d088d8aafaadea59e61985a083bad86f921d2 ;; \
      amd64) asset=aws-lambda-rie-x86_64; \
             sha=5ccd1df155e92aa3eb055e60fba492afdebf424626a7983b13a39aa5c2b71616 ;; \
      *) echo "unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    python -c 'import sys, urllib.request; urllib.request.urlretrieve(sys.argv[1], sys.argv[2])' \
      "https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/download/$RIE_VERSION/$asset" \
      /usr/local/bin/aws-lambda-rie; \
    echo "$sha  /usr/local/bin/aws-lambda-rie" | sha256sum -c -; \
    chmod 755 /usr/local/bin/aws-lambda-rie

# The package, installed into /app/.venv as a tenant's Lambda image has it.
ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
WORKDIR /src
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

WORKDIR /app
COPY tests/__init__.py tests/__init__.py
COPY tests/smoke tests/smoke
ENV DJANGO_SETTINGS_MODULE=tests.smoke.settings
ENTRYPOINT ["/app/.venv/bin/python", "-m", "awslambdaric"]
CMD ["hyperscale.tasks.lambda_handler.handler"]
