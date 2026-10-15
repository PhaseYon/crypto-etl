#!/usr/bin/env bash
# Run the lake test suite inside the Spark image.
#   lake/scripts/test.sh unit          # fast, no services needed (default)
#   lake/scripts/test.sh integration   # starts Kafka + object store, then runs integration tests
#   lake/scripts/test.sh all
# Extra arguments are passed to pytest, e.g. `lake/scripts/test.sh unit -k gold`.
set -euo pipefail

cd "$(dirname "$0")/../.."   # repo root, where docker-compose.yml lives
mode="${1:-unit}"
shift || true

compose() { docker compose --profile test "$@"; }

compose build lake-tests

start_services() {
  compose up -d --wait kafka minio
  compose run --rm minio-init
  compose run --rm kafka-init
}

case "$mode" in
  unit)
    compose run --rm --no-deps lake-tests python3 -m pytest tests/unit "$@"
    ;;
  integration)
    start_services
    compose run --rm --no-deps lake-tests python3 -m pytest tests/integration -m integration "$@"
    ;;
  all)
    start_services
    compose run --rm --no-deps lake-tests python3 -m pytest tests -m "integration or not integration" "$@"
    ;;
  *)
    echo "usage: $0 [unit|integration|all] [pytest args...]" >&2
    exit 2
    ;;
esac
