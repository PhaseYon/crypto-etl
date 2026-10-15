#!/usr/bin/env bash
# Daily lake maintenance: stop the streaming writers, OPTIMIZE + VACUUM, start them again.
# The writers must be stopped because on S3 the default Delta log store is only safe with
# one process committing to a table at a time. Stopping is graceful: each job resumes from
# its checkpoint, and Kafka buffers new trades meanwhile.
#   lake/scripts/maintenance.sh            # cron: 0 3 * * * /path/to/lake/scripts/maintenance.sh
set -euo pipefail

cd "$(dirname "$0")/../.."
writers=(lake-bronze lake-silver lake-gold)

running=$(docker compose ps --status running --services | grep -Fx -f <(printf '%s\n' "${writers[@]}") | paste -sd' ' - || true)
restart_writers() {
  if [ -n "$running" ]; then
    echo "restarting: $running"
    # shellcheck disable=SC2086
    docker compose start $running
  fi
}
trap restart_writers EXIT  # always bring the writers back, even if maintenance fails

if [ -n "$running" ]; then
  echo "stopping writers: $running"
  # shellcheck disable=SC2086
  docker compose stop $running
fi
# Arguments given to `compose run` replace the service command, so spell out the module;
# extra flags (e.g. --skip-vacuum, --retention-hours 336) come last and win
docker compose --profile batch run --rm lake-maintenance \
  lake.jobs.maintenance --retention-hours "${VACUUM_RETENTION_HOURS:-168}" "$@"
