"""Generates the Grafana dashboards in infra/grafana/dashboards/ (run after editing panels)."""
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "infra" / "grafana" / "dashboards"
CH = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}
PROM = {"type": "prometheus", "uid": "prometheus"}
TABLE, TIMESERIES = 1, 0
STAT_OPTS = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
             "colorMode": "value", "graphMode": "none", "textMode": "auto"}
TS_OPTS = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
           "tooltip": {"mode": "multi", "sort": "desc"}}


class Dashboard:
    def __init__(self, uid, title, tags, refresh="10s", time_from="now-1h"):
        self.d = {"uid": uid, "title": title, "tags": tags, "refresh": refresh, "schemaVersion": 39,
                  "time": {"from": time_from, "to": "now"}, "timezone": "utc", "editable": True,
                  "graphTooltip": 1, "annotations": {"list": []}, "templating": {"list": []},
                  "panels": [], "version": 1}

    def panel(self, ptype, title, grid, targets, ds, unit="short", options=None, description=""):
        self.d["panels"].append({
            "id": len(self.d["panels"]) + 1, "type": ptype, "title": title, "description": description,
            "datasource": ds, "gridPos": dict(zip("hwxy", grid)), "targets": targets,
            "fieldConfig": {"defaults": {"unit": unit}, "overrides": []}, "options": options or {}})

    def save(self, name):
        (OUT / name).write_text(json.dumps(self.d, indent=2) + "\n")


def ch(sql, fmt=TABLE):
    return [{"refId": "A", "datasource": CH, "editorType": "sql", "format": fmt,
             "queryType": "timeseries" if fmt == TIMESERIES else "table", "rawSql": sql}]


def prom(*exprs):
    return [{"refId": chr(65 + i), "datasource": PROM, "expr": e, "legendFormat": legend}
            for i, (e, legend) in enumerate(exprs)]


# Shared SQL (also validated by scripts/validate_sql.py) ------------------------
SQL = {
    "spend": "SELECT round(sum(spend_micros) / 1e6, 2) AS spend_usd FROM adtech.campaign_stats_1m WHERE $__timeFilter(minute)",
    "impressions": "SELECT sum(impressions) AS imps FROM adtech.campaign_stats_1m WHERE $__timeFilter(minute)",
    "ctr": "SELECT round(sum(clicks) / nullIf(sum(impressions), 0) * 100, 3) AS ctr_pct FROM adtech.campaign_stats_1m WHERE $__timeFilter(minute)",
    "roas": "SELECT round(sum(conversion_value_micros) / nullIf(sum(spend_micros), 0), 2) AS roas FROM adtech.campaign_stats_1m WHERE $__timeFilter(minute)",
    "events_per_minute": (
        "SELECT minute AS time, sum(bids) AS bid_events, sum(impressions) AS impression_events,\n"
        "       sum(clicks) AS click_events, sum(conversions) AS conversion_events\n"
        "FROM adtech.campaign_stats_1m\nWHERE $__timeFilter(minute)\nGROUP BY time\nORDER BY time"),
    "spend_by_device": (
        "SELECT minute AS time, device_type, round(sum(spend_micros) / 1e6, 2) AS spend_usd\n"
        "FROM adtech.campaign_stats_1m\nWHERE $__timeFilter(minute)\nGROUP BY time, device_type\nORDER BY time"),
    "realtime_spend": (
        "SELECT window_start AS time, round(sum(spend) / 1e6, 4) AS spend_usd_per_10s\nFROM (\n"
        "  SELECT campaign_id, window_start, argMax(spend_micros, version) AS spend\n"
        "  FROM adtech.campaign_spend_10s\n  WHERE $__timeFilter(window_start)\n"
        "  GROUP BY campaign_id, window_start\n)\nGROUP BY time\nORDER BY time"),
    "top_campaigns": (
        "SELECT dictGet('adtech.campaigns_dict', 'campaign_name', toUInt64(campaign_id)) AS campaign,\n"
        "       sum(impressions) AS imps, sum(clicks) AS clks, sum(conversions) AS convs,\n"
        "       round(sum(spend_micros) / 1e6, 2) AS spend_usd,\n"
        "       round(clks / nullIf(imps, 0) * 100, 3) AS ctr_pct,\n"
        "       round(sum(conversion_value_micros) / nullIf(sum(spend_micros), 0), 2) AS roas,\n"
        "       uniqCombined64Merge(unique_users) AS reach\n"
        "FROM adtech.campaign_stats_1m\nWHERE $__timeFilter(minute)\nGROUP BY campaign_id\n"
        "ORDER BY spend_usd DESC\nLIMIT 15"),
    "budget_pacing": (
        "SELECT dictGet('adtech.campaigns_dict', 'campaign_name', toUInt64(campaign_id)) AS campaign,\n"
        "       round(sum(spend) / dictGet('adtech.campaigns_dict', 'daily_budget_micros', toUInt64(campaign_id)) * 100, 3) AS budget_used_pct\n"
        "FROM (\n  SELECT campaign_id, window_start, argMax(spend_micros, version) AS spend\n"
        "  FROM adtech.campaign_spend_10s\n  WHERE window_start >= toStartOfDay(now())\n"
        "  GROUP BY campaign_id, window_start\n)\nGROUP BY campaign_id\nORDER BY budget_used_pct DESC\nLIMIT 10"),
    "time_to_click": (
        "SELECT device_type, count() AS clicks_attributed,\n"
        "       round(quantile(0.5)(time_to_click_ms) / 1000, 1) AS p50_s,\n"
        "       round(quantile(0.9)(time_to_click_ms) / 1000, 1) AS p90_s,\n"
        "       round(quantile(0.99)(time_to_click_ms) / 1000, 1) AS p99_s\n"
        "FROM adtech.attributed_clicks\nWHERE $__timeFilter(click_time)\nGROUP BY device_type\nORDER BY clicks_attributed DESC"),
    "dead_letters": (
        "SELECT error_type, count() AS events, max(failed_at) AS last_seen\nFROM adtech.dead_letters\n"
        "WHERE $__timeFilter(failed_at)\nGROUP BY error_type\nORDER BY events DESC"),
}


def build_business():
    d = Dashboard("adtech-performance", "Ad-Tech - Real-time Performance", ["clickhouse", "adtech"])
    d.panel("stat", "Spend", (4, 6, 0, 0), ch(SQL["spend"]), CH, "currencyUSD", STAT_OPTS)
    d.panel("stat", "Impressions", (4, 6, 6, 0), ch(SQL["impressions"]), CH, "short", STAT_OPTS)
    d.panel("stat", "CTR %", (4, 6, 12, 0), ch(SQL["ctr"]), CH, "percent", STAT_OPTS)
    d.panel("stat", "ROAS", (4, 6, 18, 0), ch(SQL["roas"]), CH, "none", STAT_OPTS)
    d.panel("timeseries", "Events per minute", (8, 12, 0, 4), ch(SQL["events_per_minute"], TIMESERIES), CH,
            "short", TS_OPTS, "From the campaign_stats_1m materialized-view rollup.")
    d.panel("timeseries", "Spend per minute by device", (8, 12, 12, 4), ch(SQL["spend_by_device"], TIMESERIES), CH,
            "currencyUSD", TS_OPTS)
    d.panel("timeseries", "Real-time spend (Flink 10 s windows)", (8, 12, 0, 12), ch(SQL["realtime_spend"], TIMESERIES),
            CH, "currencyUSD", TS_OPTS, "Latest version of each window (late impressions re-fire windows).")
    d.panel("bargauge", "Budget used today (top 10)", (8, 12, 12, 12), ch(SQL["budget_pacing"]), CH, "percent",
            {"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
             "reduceOptions": {"calcs": [], "fields": "/budget_used_pct/", "values": True}})
    d.panel("table", "Top campaigns", (10, 16, 0, 20), ch(SQL["top_campaigns"]), CH, "short", {"showHeader": True})
    d.panel("table", "Time to click (attributed)", (5, 8, 16, 20), ch(SQL["time_to_click"]), CH, "short", {"showHeader": True})
    d.panel("table", "Dead letters", (5, 8, 16, 25), ch(SQL["dead_letters"]), CH, "short", {"showHeader": True})
    d.save("adtech-performance.json")


def build_pipeline():
    d = Dashboard("adtech-pipeline", "Ad-Tech - Pipeline Health", ["prometheus", "flink", "kafka", "clickhouse"],
                  refresh="15s", time_from="now-30m")
    d.panel("timeseries", "Kafka ingest rate (events/s)", (8, 8, 0, 0),
            prom(('sum(rate(kafka_topic_partition_current_offset{topic="ad.events"}[1m]))', "ad.events")), PROM, "short", TS_OPTS)
    d.panel("timeseries", "Flink consumer lag (messages)", (8, 8, 8, 0),
            prom(('sum(kafka_consumergroup_lag{consumergroup="flink-adtech", topic="ad.events"})', "lag")), PROM, "short", TS_OPTS,
            "Offsets are committed on checkpoints, so lag moves in steps of one checkpoint interval.")
    d.panel("timeseries", "Data quality (events/s)", (8, 8, 16, 0), prom(
        ("sum(rate(flink_taskmanager_job_task_operator_adtech_validEvents[1m]))", "valid"),
        ("sum(rate(flink_taskmanager_job_task_operator_adtech_invalidEvents[1m]))", "invalid -> DLQ"),
        ("sum(rate(flink_taskmanager_job_task_operator_adtech_duplicatesDropped[1m]))", "duplicates dropped")),
        PROM, "short", TS_OPTS)
    d.panel("timeseries", "Rows written to ClickHouse by table (rows/s)", (8, 12, 0, 8), prom(
        ("sum by (clickhouse) (rate(flink_taskmanager_job_task_operator_clickhouse_rowsWritten[1m]))", "{{clickhouse}}")),
        PROM, "short", TS_OPTS)
    d.panel("timeseries", "Insert batches and retries per second", (8, 12, 12, 8), prom(
        ("sum by (clickhouse) (rate(flink_taskmanager_job_task_operator_clickhouse_batchesWritten[1m]))", "batches {{clickhouse}}"),
        ("sum by (clickhouse) (rate(flink_taskmanager_job_task_operator_clickhouse_insertRetries[1m]))", "retries {{clickhouse}}")),
        PROM, "short", TS_OPTS, "Target: about one insert per second per shard or fewer.")
    d.panel("timeseries", "Checkpoint duration / size", (8, 8, 0, 16), prom(
        ("max(flink_jobmanager_job_lastCheckpointDuration)", "duration ms"),
        ("max(flink_jobmanager_job_lastCheckpointSize)", "size bytes")), PROM, "short", TS_OPTS)
    d.panel("timeseries", "Back-pressure (ms busy per second, max subtask)", (8, 8, 8, 16), prom(
        ("max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)", "back-pressured")), PROM, "ms", TS_OPTS)
    d.panel("timeseries", "ClickHouse inserted rows/s per node", (8, 8, 16, 16), prom(
        ("sum by (instance) (rate(ClickHouseProfileEvents_InsertedRows[1m]))", "{{instance}}")), PROM, "short", TS_OPTS)
    d.panel("timeseries", "Max parts per partition (alert < 300)", (8, 8, 0, 24), prom(
        ("max by (instance) (ClickHouseAsyncMetrics_MaxPartCountForPartition)", "{{instance}}")), PROM, "short", TS_OPTS)
    d.panel("timeseries", "Replication delay (s)", (8, 8, 8, 24), prom(
        ("max by (instance) (ClickHouseAsyncMetrics_ReplicasMaxAbsoluteDelay)", "{{instance}}")), PROM, "s", TS_OPTS)
    d.panel("timeseries", "ClickHouse memory", (8, 8, 16, 24), prom(
        ("max by (instance) (ClickHouseMetrics_MemoryTracking)", "{{instance}}")), PROM, "bytes", TS_OPTS)
    d.save("adtech-pipeline.json")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    build_business()
    build_pipeline()
    print("dashboards written to", OUT)
