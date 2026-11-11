#!/usr/bin/env bash
# End-to-end check: waits until events, Flink window metrics and dead letters
# all show up in ClickHouse, then prints a short summary.
set -euo pipefail

cd "$(dirname "$0")/.."
if [[ -f .env ]]; then
  set -a; source .env; set +a
fi

CH_URL="${CLICKHOUSE_HTTP_URL:-http://localhost:8123}"
CH_USER="${CLICKHOUSE_USER:-analytics}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-analytics_pwd}"
TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-240}"

query() {
  curl -sS --fail -u "${CH_USER}:${CH_PASSWORD}" "${CH_URL}/?database=analytics" --data-binary "$1"
}

wait_for_rows() {
  local table="$1" deadline=$((SECONDS + TIMEOUT_SECONDS)) count=0
  printf "Waiting for rows in %-28s" "${table} ..."
  while (( SECONDS < deadline )); do
    count=$(query "SELECT count() FROM ${table}" 2>/dev/null || echo 0)
    if [[ "${count}" =~ ^[0-9]+$ ]] && (( count > 0 )); then
      echo " OK (${count} rows)"
      return 0
    fi
    sleep 5
  done
  echo " FAILED (no rows after ${TIMEOUT_SECONDS}s)"
  return 1
}

echo "ClickHouse: ${CH_URL}"
query "SELECT 1" > /dev/null || { echo "ClickHouse is not reachable"; exit 1; }

wait_for_rows analytics.events
wait_for_rows analytics.page_metrics_1m
wait_for_rows analytics.events_hourly
wait_for_rows analytics.dead_letters

echo ""
echo "Events by type (last 15 minutes):"
query "SELECT event_type, count() AS events, uniq(user_id) AS users
       FROM analytics.events
       WHERE event_time >= now() - INTERVAL 15 MINUTE
       GROUP BY event_type ORDER BY events DESC
       FORMAT PrettyCompactMonoBlock"

echo "Dead letters by error type:"
query "SELECT error_type, count() AS events FROM analytics.dead_letters
       GROUP BY error_type ORDER BY events DESC
       FORMAT PrettyCompactMonoBlock"

echo "Smoke test passed."
