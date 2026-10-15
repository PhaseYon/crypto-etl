#!/usr/bin/env sh
# Runs a command (default: npm) in the pinned Node image, since Node isn't installed on the host.
#   ui/scripts/npm.sh npm ci
#   ui/scripts/npm.sh npm test
#   NODE_DOCKER_ARGS="-p 15173:5173" ui/scripts/npm.sh npm run dev
set -eu
UI_DIR="$(cd "$(dirname "$0")/.." && pwd)"
NODE_IMAGE="node:24.22.0-bookworm-slim"
TTY_FLAG=""
if [ -t 0 ] && [ -t 1 ]; then TTY_FLAG="-it"; fi
# shellcheck disable=SC2086
exec docker run --rm $TTY_FLAG \
  -u "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -e npm_config_cache=/app/.npm-cache \
  -e npm_config_update_notifier=false \
  -v "$UI_DIR:/app" -w /app \
  --add-host=host.docker.internal:host-gateway \
  ${NODE_DOCKER_ARGS:-} \
  "$NODE_IMAGE" "$@"
