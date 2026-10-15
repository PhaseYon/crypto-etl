#!/usr/bin/env sh
# End-to-end smoke test: starts the dashboard (API in mock mode + nginx UI) in its own compose
# project, runs Playwright in the pinned Playwright image on that network, then tears it all down.
#   ui/scripts/e2e.sh                 # run the tests
#   E2E_KEEP=1 ui/scripts/e2e.sh      # leave the stack running afterwards
set -eu
UI_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ROOT_DIR="$(cd "$UI_DIR/.." && pwd)"
PLAYWRIGHT_IMAGE="mcr.microsoft.com/playwright:v1.64.1-noble"

export COMPOSE_PROJECT_NAME="${E2E_PROJECT:-cetl-ui-e2e}"
export KAFKA_HOST_PORT="${E2E_KAFKA_HOST_PORT:-59093}"
export DASHBOARD_HOST_PORT="${E2E_DASHBOARD_HOST_PORT:-13001}"
export DASHBOARD_API_HOST_PORT="${E2E_API_HOST_PORT:-18001}"
export DASHBOARD_DATA_SOURCE=mock

compose() { docker compose -f "$ROOT_DIR/docker-compose.yml" "$@"; }

cleanup() {
  if [ "${E2E_KEEP:-0}" != "1" ]; then compose down -v --remove-orphans >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

compose up -d --build --wait dashboard

docker run --rm \
  --network "${COMPOSE_PROJECT_NAME}_default" \
  -u "$(id -u):$(id -g)" -e HOME=/tmp -e CI="${CI:-}" \
  -e E2E_BASE_URL=http://dashboard:8080 \
  -v "$UI_DIR:/app" -w /app \
  "$PLAYWRIGHT_IMAGE" npx playwright test "$@"
