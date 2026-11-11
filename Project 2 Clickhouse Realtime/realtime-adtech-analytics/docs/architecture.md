# Architecture

## Data flow

```
                 ┌───────────── ad.dlq ◄──── invalid ─────┐
                 │                                         │
 Exchanges ──► Kafka ad.events (48 partitions, zstd) ──► Flink ──► parse/validate
                                                           │
                                                event-time watermarks (15 s)
                                                           │
                                             dedup by event_id (RocksDB, TTL 1 h)
                                                           │
             ┌──────────────────────────┬──────────────────┴──────────────┐
             ▼                          ▼                                 ▼
   ad_events_local              impressions ⋈ clicks               impressions →
   (shard = user_id % N)        interval join [-10 s, +30 min]     10 s spend windows
             │                          │                           │        │
             │                attributed_clicks_local     campaign_spend   Kafka
             │                (shard = user_id % N)       (shard = campaign) campaign.spend
             ▼                                                               (pacing)
   MV → campaign_stats_1m_local (per shard)
   TTL: 7 d → S3 cold tier, bids deleted at 30 d, rest at 180 d
```

## Key design decisions

### 1. Co-location by `user_id`
The raw table is sharded by `user_id`. Every event of an auction (bid, impression, click,
conversion) belongs to one user, so a request's whole funnel lives on one shard. Per-user and
per-request analysis (frequency capping, funnels, attribution checks) runs shard-locally with no
cross-shard shuffles; `optimize_distributed_group_by_sharding_key` lets ClickHouse skip the
final merge on the initiator when grouping by the sharding key.

### 2. Flink writes to local tables, routed like the Distributed engine
`ShardRouter` computes `shard = user_id % shards` on the unsigned value, exactly as a
Distributed table with equal weights does. The sink writes straight to `*_local` tables:

- no second hop through a Distributed table, no Distributed on-disk queue;
- each sink subtask serves **one** shard (custom partitioner in front of the sink), so batches are
  large and inserts per shard stay below one per second;
- `_shard_num` lets the smoke test prove every row sits on the expected shard.

### 3. Duplicates handled before storage, not at query time
Exchanges deliver at-least-once. Instead of `ReplacingMergeTree` + `FINAL` on a 30 TB table:

| Duplicate source | Handled by |
|---|---|
| Exchange re-sends an event | Flink keyed dedup on `event_id` (state TTL 1 h) |
| HTTP insert timed out but committed; retried on the other replica | `insert_deduplication_token` per batch, stored in Keeper by the replicated table |
| Same, for materialized views | `deduplicate_blocks_in_dependent_materialized_views = 1` |
| Flink restart replays from the last checkpoint | Rows after the checkpoint can be re-inserted; small, bounded by checkpoint interval (30 s). Derived tables (attribution, spend) use ReplacingMergeTree. |

### 4. Delivery guarantees

| Path | Guarantee |
|---|---|
| Kafka → Flink | exactly-once state (offsets in checkpoints) |
| Flink → ClickHouse | at-least-once (all buffers flushed in `snapshotState`), effectively-once within the dedup mechanisms above |
| Flink → Kafka (`ad.dlq`, `campaign.spend`) | at-least-once; `campaign.spend` is keyed by campaign+window and compacted, consumers take the latest version |

### 5. Late data
Watermarks trail the highest event time by 15 s (configurable). Spend windows accept impressions
up to 60 s late; each late impression re-fires the window with `version + 1`, kept in Flink
window state, and `ReplacingMergeTree(version)` / the compacted topic keep the newest result.
Wall-clock timestamps are not used as versions because two firings can share a millisecond.
The interval join drops events that are already behind the watermark; raw events are always
stored, so attribution can be recomputed in SQL for very late data.

### 6. Storage layout of `ad_events_local`

| Choice | Why |
|---|---|
| `PARTITION BY toYYYYMMDD(event_time)` | ~35 GB partitions per shard; cheap TTL drops and moves |
| `ORDER BY (advertiser_id, campaign_id, event_type, toStartOfHour(event_time), user_id)` | Dashboards filter by advertiser/campaign/type/time; `user_id` last improves compression and per-user lookups |
| `Enum8` event type, `UUID` columns, `LowCardinality` dimensions | 1 byte instead of ~10; 16 bytes instead of 36 |
| `DoubleDelta` on timestamps, `T64` on prices | codecs designed for near-monotonic timestamps and small-range integers (benchmark alternatives on your data) |
| bloom filters on `request_id`, `user_id` | point lookups for debugging/support without a full scan |
| projection `p_publisher_hourly` | publisher reports read a tiny pre-aggregate inside each part |
| TTL `TO VOLUME 'cold'` after 7 days, `DELETE WHERE event_type = 'bid'` after 30 days | bids are 74% of rows but rarely queried after a month |

### 7. Rollups
`campaign_stats_1m_local` is an `AggregatingMergeTree` fed by a materialized view on each shard
(sums as `SimpleAggregateFunction`, reach as `uniqCombined64` state). Dashboards read it through
a Distributed table: thousands of times fewer rows than the raw table.

### 8. Dimensions
`campaigns` is replicated to every node (Keeper path without `{shard}`) and exposed as a
`HASHED` dictionary, so dashboards use `dictGet()` instead of distributed JOINs.

## Failure modes

| Failure | Behaviour |
|---|---|
| One ClickHouse replica down | Sink fails over to the other replica of the shard with the same dedup token; reads continue from the remaining replica |
| A whole shard down | Inserts for that shard retry with backoff, then the job fails and restarts from the last checkpoint; Kafka buffers 3 days |
| Keeper quorum lost | Replicated tables become read-only; inserts fail and Flink backs off/restarts until Keeper returns |
| Schema drift upstream | Invalid events go to `ad.dlq` → `dead_letters`; `DeadLetterRateHigh` alert fires |
| Too many parts | `ClickHouseTooManyParts` alert at 200 (insert delay starts at 300); raise batch size / interval |
| S3 outage | Hot tier keeps working; moves to cold and cold reads fail until S3 recovers |
