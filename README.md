# hyperscale-tasks

A Django tasks backend on a transactional outbox and SQS, with an AWS Lambda handler.

Documentation to follow.

## Smoke test

`tests/smoke/run.sh` runs the Lambda handler in a container, as a tenant's
Lambda image runs it, and sends it one SQS record. It is a manual check, not a
CI step, and needs Docker and curl:

```sh
tests/smoke/run.sh
```

The script:

1. builds `tests/smoke/Dockerfile` (`python:3.14-slim`, the package installed
   into `/app/.venv`, the Lambda Runtime Interface Emulator, and
   `tests/smoke/settings.py`: SQLite, `DEBUG = True`, the immediate backend);
2. starts a `motoserver/moto` container as the SQS endpoint
   (`AWS_ENDPOINT_URL_SQS`, dummy credentials, `eu-west-2`), creates the queues,
   and sends and receives a message for `tests.smoke.tasks.ok`, to get a real
   receipt handle;
3. runs the image under the emulator:
   `docker run -p 127.0.0.1:9000:8080 --entrypoint /usr/local/bin/aws-lambda-rie <image> /app/.venv/bin/python -m awslambdaric hyperscale.tasks.lambda_handler.handler`;
4. posts the record to
   `http://localhost:9000/2015-03-31/functions/function/invocations`.

It passes when the response is `{"batchItemFailures": []}` and the queue is
empty afterwards. It removes its containers, network and image when it exits.
