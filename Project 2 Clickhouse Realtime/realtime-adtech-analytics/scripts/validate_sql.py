"""Validates all ClickHouse SQL in the repo with an embedded ClickHouse engine (chdb).

* Schema: cluster DDL is converted to single-node equivalents (ON CLUSTER removed,
  Replicated* -> plain engines, S3 tier / Distributed / Kafka engine skipped) and executed.
* Cookbook + Grafana panels: every query is executed against the schema
  (Distributed table names mapped to their *_local tables, cluster functions to local tables).

Run: pip install chdb && python scripts/validate_sql.py
"""
from __future__ import annotations

import importlib.util
import re
import sys
import tempfile
from pathlib import Path

from chdb import session

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = ROOT / "infra" / "clickhouse" / "schema"
COOKBOOK = ROOT / "sql" / "analytics_queries.sql"
# Server-only system tables (enabled in infra/clickhouse/config.d/server.xml, absent in embedded mode)
SERVER_ONLY_TABLES = ("system.part_log",)
DISTRIBUTED = ("ad_events", "campaign_stats_1m", "attributed_clicks", "campaign_spend_10s", "dead_letters")


def localize_ddl(sql: str) -> list[str]:
    sql = re.sub(r"--[^\n]*", "", sql).replace(" ON CLUSTER adtech", "")
    sql = re.sub(r"Replicated(\w*)MergeTree\('[^']*',\s*'\{replica\}'(?:,\s*([^)]*))?\)",
                 lambda m: f"{m.group(1)}MergeTree({m.group(2) or ''})", sql)
    sql = re.sub(r"toDateTime\(event_time\) \+ INTERVAL 7 DAY TO VOLUME 'cold',\s*", "", sql)
    sql = re.sub(r"storage_policy = 'hot_cold',\s*", "", sql)
    statements = []
    for stmt in sql.split(";"):
        s = stmt.strip()
        if not s or "ENGINE = Distributed" in s or "ENGINE = Kafka" in s or "dead_letters_mv" in s:
            continue
        if s.startswith("CREATE DICTIONARY"):
            s = re.sub(r"HOST 'localhost' PORT 9000 USER 'dict_reader' PASSWORD ''\s*", "", s)
        statements.append(s)
    return statements


def localize_query(sql: str) -> str:
    sql = re.sub(r"\$__timeFilter\((\w+)\)", r"\1 >= now() - INTERVAL 1 DAY", sql)
    sql = re.sub(r"cluster(AllReplicas)?\('adtech', (system\.\w+)\)", r"\2", sql)
    for table in DISTRIBUTED:
        sql = re.sub(rf"adtech\.{table}(\s)", rf"adtech.{table}_local\1", sql)
    return sql


def cookbook_queries() -> dict[str, str]:
    blocks = re.split(r"\n-- \[(\w+)\][^\n]*\n", COOKBOOK.read_text())
    return {name: body.split(";")[0] for name, body in zip(blocks[1::2], blocks[2::2])}


def dashboard_queries() -> dict[str, str]:
    spec = importlib.util.spec_from_file_location("dashboards", ROOT / "scripts" / "build_dashboards.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {f"panel:{k}": v for k, v in module.SQL.items()}


def main() -> int:
    failures = 0
    with tempfile.TemporaryDirectory() as tmp:
        sess = session.Session(tmp)
        sess.query("SYSTEM FLUSH LOGS")
        for file in sorted(SCHEMA.glob("*.sql")):
            for statement in localize_ddl(file.read_text()):
                sess.query(statement)
            print(f"schema   ok  {file.name}")
        for name, sql in {**cookbook_queries(), **dashboard_queries()}.items():
            if any(t in sql for t in SERVER_ONLY_TABLES):
                print(f"query  skip  {name} (needs a server system table)")
                continue
            try:
                sess.query(localize_query(sql) + "\nFORMAT Null")
                print(f"query    ok  {name}")
            except Exception as exc:  # noqa: BLE001 - report every failing query
                failures += 1
                print(f"query  FAIL  {name}: {str(exc).splitlines()[0][:200]}")
    print("FAILED" if failures else "All SQL valid.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
