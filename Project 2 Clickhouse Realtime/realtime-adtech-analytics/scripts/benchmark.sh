#!/usr/bin/env bash
# Runs clickhouse-benchmark with the dashboard-style queries in benchmarks/queries.sql.
# Usage: scripts/benchmark.sh [concurrency] [iterations]
set -euo pipefail
cd "$(dirname "$0")/.."
CONCURRENCY="${1:-4}"
ITERATIONS="${2:-200}"
docker compose exec -T ch-s1-r1 bash -c \
  "clickhouse-benchmark --user adtech --password \"\$CH_PASSWORD\" --concurrency ${CONCURRENCY} \
   --iterations ${ITERATIONS} --randomize 1 --delay 0" < benchmarks/queries.sql
