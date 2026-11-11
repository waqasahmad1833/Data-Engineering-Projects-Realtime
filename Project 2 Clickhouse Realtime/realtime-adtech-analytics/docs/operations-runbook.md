# Operations runbook

## Daily health check
- Grafana → **Ad-Tech - Pipeline Health**: consumer lag flat, no insert retries, checkpoint
  duration well below the 30 s interval, max parts per partition < 100.
- `sql/analytics_queries.sql` → `ops_replication`, `ops_parts_health`, `ops_storage_tiers`.

## Flink

### Change parallelism or upgrade the job
Upgrades go through a savepoint (`upgradeMode: savepoint` in `k8s/flink.yaml`): change the image
tag or `parallelism` and apply; the operator takes a savepoint, stops, and restarts from it.
All operators have stable `uid`s, so state maps across versions. Do not change a `uid`.

Locally: `docker compose stop flink-jobmanager flink-taskmanager`, then start with
`--fromSavepoint` or let it resume from committed Kafka offsets (dedup state is then empty;
expect a few duplicates around the restart point).

### Lag is growing
1. Back-pressure panel: which operator is busy?
2. If the ClickHouse sinks: check insert latency/retries; raise `SINK_WRITERS_PER_SHARD`.
3. If dedup/join: add TaskManagers and raise parallelism (savepoint upgrade).
4. If the source: add Kafka partitions first (partition count caps source parallelism).

## ClickHouse

### Schema changes
All DDL is `ON CLUSTER adtech`. Additive changes are online:

```sql
ALTER TABLE adtech.ad_events_local ON CLUSTER adtech ADD COLUMN IF NOT EXISTS deal_id String DEFAULT '';
ALTER TABLE adtech.ad_events ON CLUSTER adtech ADD COLUMN IF NOT EXISTS deal_id String DEFAULT '';
```

Deploy the ClickHouse change **before** the Flink job starts sending the new field. Depending on
`input_format_skip_unknown_fields`, an unknown field is either rejected (inserts fail) or
silently dropped (data lost), and neither is what you want.

### Too many parts
Symptoms: `ClickHouseTooManyParts` alert, inserts slowing. Causes and fixes:
- batches too small → increase `SINK_FLUSH_INTERVAL_MS` / `SINK_BATCH_ROWS`;
- too many writers per shard → lower `SINK_WRITERS_PER_SHARD`;
- merges starved (disk/CPU) → check `system.merges`, disk free space on the hot volume.

### Replica lagging or read-only
`ops_replication` query. `is_readonly = 1` usually means Keeper connectivity; fix Keeper first.
A replica that was down catches up automatically by fetching parts from its peer.

### Adding replicas (read scaling)
Increase `replicasCount` in `k8s/clickhouse.yaml`. New replicas create tables via the operator
and fetch all parts from existing replicas. No application change except adding the new
replica URL to `CLICKHOUSE_SHARDS` (optional; it is only used for failover/spreading).

### Adding shards
The sharding key is `user_id % shards`, so changing the shard count changes where new data
goes. Options:
1. **New data only** (simplest): add shards, append them to `CLICKHOUSE_SHARDS` in the same
   order as `remote_servers`, restart Flink from a savepoint. Old data stays where it is; reads
   through Distributed tables are unaffected. Shard-local per-user analysis is exact only for
   data written after the change (or within a single day partition boundary you choose).
2. **Full rebalance**: create a new cluster, backfill with `INSERT INTO new.ad_events SELECT ...`
   per day partition, switch readers and writers, drop the old cluster.

## Replays and backfills
- Replay from Kafka: stop the job, reset the consumer group offsets for `ad.events` to a time,
  start without a savepoint. Rows already in ClickHouse are written again (dedup state is gone):
  first drop the affected day partitions on all shards
  (`ALTER TABLE adtech.ad_events_local ON CLUSTER adtech DROP PARTITION 20261110`).
- Replay dead letters after a fix: read `adtech.dead_letters` raw payloads and re-publish them to
  `ad.events` (they keep their original `event_id`, so duplicates are dropped by Flink).

## Storage tiers
- What lives where: `ops_storage_tiers` query (`disk_name` = `default` hot, `s3_cold_cache` cold).
- Force a move (e.g. after changing TTL):
  `ALTER TABLE adtech.ad_events_local ON CLUSTER adtech MATERIALIZE TTL IN PARTITION 20261101`.
- Cold reads are served through a 200 GiB local cache per node; first reads of old data are slower.
