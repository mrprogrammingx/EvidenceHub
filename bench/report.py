"""Builds docs/perf/baseline.md from a Locust CSV run + bench/out/explain_capture.json.

Usage:
    uv run python bench/report.py --csv-prefix bench/out/baseline \\
        --locust-users 8 --locust-duration 60s
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from bench.critical_queries import QuerySpec, load_critical_queries  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402

from evidencehub.common.config import get_settings  # noqa: E402

EXPLAIN_CAPTURE_PATH = REPO_ROOT / "bench" / "out" / "explain_capture.json"
REPORT_PATH = REPO_ROOT / "docs" / "perf" / "baseline.md"

_TABLE_SCAN_RE = re.compile(r"Table scan on \S+")
_INDEX_RE = re.compile(r"using (\w+)")


@dataclass
class LocustStats:
    request_count: int
    failure_count: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    avg_ms: float
    rps: float


def _load_locust_stats(csv_prefix: str) -> dict[str, LocustStats]:
    stats_path = Path(f"{csv_prefix}_stats.csv")
    stats: dict[str, LocustStats] = {}
    with stats_path.open(newline="") as f:
        for row in csv.DictReader(f):
            if row["Name"] == "Aggregated":
                continue
            stats[row["Name"]] = LocustStats(
                request_count=int(row["Request Count"]),
                failure_count=int(row["Failure Count"]),
                p50_ms=float(row["50%"]),
                p95_ms=float(row["95%"]),
                p99_ms=float(row["99%"]),
                avg_ms=float(row["Average Response Time"]),
                rps=float(row["Requests/s"]),
            )
    return stats


def _classify_index_usage(explain_text: str) -> str:
    if _TABLE_SCAN_RE.search(explain_text):
        return "full scan"
    index_match = _INDEX_RE.search(explain_text)
    if index_match:
        return f"index: {index_match.group(1)}"
    return "unknown (see EXPLAIN ANALYZE text)"


def _actual_row_counts() -> tuple[list[int], int]:
    settings = get_settings()
    counts = []
    for i in range(settings.shard_count):
        engine = create_engine(settings.shard_url(i))
        with engine.connect() as conn:
            counts.append(conn.execute(text("SELECT COUNT(*) FROM evidence_legacy")).scalar_one())
        engine.dispose()
    return counts, sum(counts)


def _render(
    queries: list[QuerySpec],
    locust_stats: dict[str, LocustStats],
    explain_captures: dict[str, dict[str, Any]],
    row_counts: list[int],
    total_rows: int,
    locust_users: str,
    locust_duration: str,
) -> str:
    lines: list[str] = []
    lines.append("# P1 Baseline Benchmark: evidence_legacy")
    lines.append("")
    lines.append(f"Generated: {datetime.now(UTC).isoformat()}")
    lines.append("")
    lines.append(
        "Hardware: this machine (Apple Silicon, arm64), Docker Desktop VM capped at ~7.65GB "
        "RAM shared with other unrelated containers, each MySQL 8.0 shard tuned to a 512MB "
        "InnoDB buffer pool. See docs/perf/ for how to reproduce."
    )
    lines.append("")
    lines.append(f"Dataset: **{total_rows:,} total rows** across 4 shards: {row_counts}.")
    lines.append("")
    lines.append(
        f"Load: Locust headless, {locust_users} users, {locust_duration} run time, "
        "~0-50ms think time."
    )
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(
        "| Query | SLO (p95) | p50 | p95 | p99 | Requests | Rows examined | Index used | Verdict |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")

    for spec in queries:
        ls = locust_stats.get(spec.name)
        capture = explain_captures.get(spec.name, {})
        rows_examined = capture.get("rows_examined")
        index_usage = _classify_index_usage(capture.get("explain_analyze", ""))
        if ls is None:
            lines.append(
                f"| {spec.name} | p95 < {spec.slo_ms}ms | - | - | - | 0 | "
                f"{rows_examined} | {index_usage} | NO DATA |"
            )
            continue
        verdict = "PASS" if ls.p95_ms < spec.slo_ms else "FAIL"
        lines.append(
            f"| {spec.name} | p95 < {spec.slo_ms}ms | {ls.p50_ms:.1f}ms | {ls.p95_ms:.1f}ms | "
            f"{ls.p99_ms:.1f}ms | {ls.request_count} | {rows_examined} | "
            f"{index_usage} | {verdict} |"
        )

    lines.append("")
    lines.append("## Detail per query")
    lines.append("")
    for spec in queries:
        ls = locust_stats.get(spec.name)
        capture = explain_captures.get(spec.name, {})
        lines.append(f"### `{spec.name}`")
        lines.append("")
        rel_path = spec.path.relative_to(REPO_ROOT)
        lines.append(f"SLO: p95 < {spec.slo_ms}ms. Query file: `{rel_path}`.")
        lines.append("")
        if ls is not None:
            lines.append(
                f"Locust ({ls.request_count} requests, {ls.failure_count} failures): "
                f"p50={ls.p50_ms:.1f}ms p95={ls.p95_ms:.1f}ms p99={ls.p99_ms:.1f}ms "
                f"avg={ls.avg_ms:.1f}ms rps={ls.rps:.1f}"
            )
            lines.append("")
        if capture:
            rows_returned = capture.get("rows_returned", "?")
            lines.append(
                f"Single-execution capture ({rows_returned} rows returned): "
                f"wall={capture.get('wall_ms')}ms engine={capture.get('engine_ms')}ms "
                f"rows_examined={capture.get('rows_examined')} rows_sent={capture.get('rows_sent')}"
            )
            lines.append("")
            lines.append("<details><summary>EXPLAIN ANALYZE</summary>")
            lines.append("")
            lines.append("```")
            lines.append(str(capture.get("explain_analyze", "")).strip())
            lines.append("```")
            lines.append("")
            lines.append("</details>")
            lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-prefix", default="bench/out/baseline")
    parser.add_argument("--locust-users", default="8")
    parser.add_argument("--locust-duration", default="60s")
    args = parser.parse_args()

    queries = load_critical_queries()
    locust_stats = _load_locust_stats(args.csv_prefix)
    explain_data = json.loads(EXPLAIN_CAPTURE_PATH.read_text())
    explain_captures = {q["name"]: q for q in explain_data["queries"]}

    row_counts, total_rows = _actual_row_counts()

    report = _render(
        queries,
        locust_stats,
        explain_captures,
        row_counts,
        total_rows,
        args.locust_users,
        args.locust_duration,
    )
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report)
    print(f"wrote {REPORT_PATH}")


if __name__ == "__main__":
    main()
