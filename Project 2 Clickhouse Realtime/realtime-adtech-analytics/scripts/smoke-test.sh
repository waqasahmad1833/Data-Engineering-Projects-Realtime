#!/usr/bin/env bash
# End-to-end check of the running stack: data reaches every table on every shard.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then set -a; source .env; set +a; fi

CH_URL="${CH_URL:-http://localhost:8123}"
CH_PASSWORD="${CH_PASSWORD:-adtech_pwd}"
TIMEOUT="${SMOKE_TIMEOUT_SECONDS:-300}"

q() { curl -sS --fail -u "adtech:${CH_PASSWORD}" "${CH_URL}/" --data-binary "$1"; }

wait_for() {
  local table="$1" deadline=$((SECONDS + TIMEOUT)) n=0
  printf "%-32s" "${table}"
  while (( SECONDS < deadline )); do
    n=$(q "SELECT count() FROM ${table}" 2>/dev/null || echo 0)
    if [[ "${n}" =~ ^[0-9]+$ ]] && (( n > 0 )); then echo "OK  (${n} rows)"; return 0; fi
    sleep 5
  done
  echo "FAILED (no rows after ${TIMEOUT}s)"; return 1
}

q "SELECT 1" > /dev/null || { echo "ClickHouse not reachable at ${CH_URL}"; exit 1; }
echo "Waiting for data (Distributed tables):"
for t in adtech.ad_events adtech.campaign_stats_1m adtech.campaign_spend_10s adtech.attributed_clicks adtech.dead_letters; do
  wait_for "$t"
done

echo ""
echo "Rows of ad_events_local per node (both replicas of a shard must match closely):"
q "SELECT hostName() AS host, count() AS events
   FROM clusterAllReplicas('adtech', adtech.ad_events_local)
   GROUP BY host ORDER BY host FORMAT PrettyCompactMonoBlock"

# _shard_num is the Distributed engine's virtual column (1-based shard of each row).
# The local stack has 2 shards; Flink must have placed every row on shard (user_id % 2) + 1.
echo "Sharding check (must be 0):"
q "SELECT countIf(user_id % 2 + 1 != _shard_num) AS misplaced_rows
   FROM adtech.ad_events FORMAT PrettyCompactMonoBlock"

echo "Storage policy and disks:"
q "SELECT policy_name, volume_name, disks FROM system.storage_policies
   WHERE policy_name = 'hot_cold' FORMAT PrettyCompactMonoBlock"

echo "Smoke test passed."
