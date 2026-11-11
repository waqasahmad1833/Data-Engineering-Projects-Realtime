#!/usr/bin/env bash
# Applies infra/clickhouse/schema/*.sql in order against one node.
# All DDL uses ON CLUSTER, so ClickHouse fans it out to every shard/replica.
set -euo pipefail

CH_HOST="${CH_HOST:-ch-s1-r1}"
SCHEMA_DIR="${SCHEMA_DIR:-/schema}"
CLIENT=(clickhouse-client --host "${CH_HOST}" --user adtech --password "${CH_PASSWORD}")

echo "Waiting for ${CH_HOST} ..."
until "${CLIENT[@]}" --query "SELECT 1" > /dev/null 2>&1; do sleep 2; done

echo "Waiting for all cluster nodes to be reachable ..."
for _ in $(seq 1 60); do
  reachable=$("${CLIENT[@]}" --query \
    "SELECT count() FROM clusterAllReplicas('adtech', system.one)" 2>/dev/null || echo 0)
  expected=$("${CLIENT[@]}" --query \
    "SELECT count() FROM system.clusters WHERE cluster = 'adtech'")
  [[ "${reachable}" == "${expected}" ]] && break
  sleep 2
done

for file in "${SCHEMA_DIR}"/*.sql; do
  echo "Applying $(basename "${file}")"
  "${CLIENT[@]}" --multiquery --distributed_ddl_task_timeout 300 < "${file}"
done

echo "Schema applied. Tables per node:"
"${CLIENT[@]}" --query "
  SELECT hostName() AS host, count() AS tables
  FROM clusterAllReplicas('adtech', system.tables)
  WHERE database = 'adtech'
  GROUP BY host ORDER BY host
  FORMAT PrettyCompactMonoBlock"
