#!/bin/sh
# Smoke-test the Lambda handler in a container, through the Lambda Runtime
# Interface Emulator, with a moto server standing in for SQS. Not run in CI.
# Needs Docker and curl. Usage: tests/smoke/run.sh
set -eu
cd "$(dirname "$0")/../.."

name=hyperscale-tasks-smoke
moto_image=motoserver/moto:5.2.5

cleanup() {
    docker rm -f "$name-lambda" "$name-sqs" >/dev/null 2>&1 || true
    docker network rm "$name" >/dev/null 2>&1 || true
    docker rmi "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

# Dummy credentials: nothing leaves this machine.
export AWS_ENDPOINT_URL_SQS="http://$name-sqs:5000"
export AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing
export AWS_DEFAULT_REGION=eu-west-2
aws_env="-e AWS_ENDPOINT_URL_SQS -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION"

docker build -q -f tests/smoke/Dockerfile -t "$name" . >/dev/null
docker network create "$name" >/dev/null
docker run -d --name "$name-sqs" --network "$name" "$moto_image" >/dev/null

stub() {
    # shellcheck disable=SC2086
    docker run --rm --network "$name" $aws_env \
        --entrypoint /app/.venv/bin/python "$name" tests/smoke/sqs_stub.py "$@"
}

tries=0
until queues=$(stub queues 2>/dev/null); do
    tries=$((tries + 1))
    [ "$tries" -lt 30 ] || { echo "SQS stub did not start" >&2; exit 1; }
    sleep 1
done
queue_url=${queues% *}
dlq_url=${queues#* }
event=$(stub event "$queue_url")

# shellcheck disable=SC2086
docker run -d --name "$name-lambda" --network "$name" -p 127.0.0.1:9000:8080 \
    $aws_env -e JOBS_QUEUE_URL="$queue_url" -e JOBS_DLQ_URL="$dlq_url" \
    -e HYPERSCALE_RUNTIME=lambda \
    --entrypoint /usr/local/bin/aws-lambda-rie "$name" \
    /app/.venv/bin/python -m awslambdaric hyperscale.tasks.lambda_handler.handler \
    >/dev/null

response=$(curl -sS --retry 10 --retry-all-errors --retry-delay 1 -d "$event" \
    http://localhost:9000/2015-03-31/functions/function/invocations)
echo "response: $response"
counts=$(stub count "$queue_url")
echo "queue after: $counts"

expected='{"batchItemFailures": []}'
drained='{"ApproximateNumberOfMessages": "0", "ApproximateNumberOfMessagesNotVisible": "0"}'
if [ "$response" != "$expected" ] || [ "$counts" != "$drained" ]; then
    echo "FAILED; Lambda container log:" >&2
    docker logs "$name-lambda" >&2
    exit 1
fi
echo "OK"
