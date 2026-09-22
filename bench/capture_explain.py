"""Captures EXPLAIN ANALYZE plans and performance_schema stats per critical query.

Runs each of the 8 critical queries once against a single representative
shard (shard 0 -- the legacy schema and data distribution are homogeneous
across shards, so a 4x repeat wouldn't tell us anything new) and records:

  - wall_ms: real client-observed latency of one execution
  - rows_examined / rows_sent / engine_ms: from performance_schema, i.e.
    MySQL's own view of what that exact statement did
  - explain_analyze: the full EXPLAIN ANALYZE plan text (a second, separate
    execution -- EXPLAIN ANALYZE actually runs the query to get real numbers)

Output: bench/out/explain_capture.json, consumed by bench/report.py.
"""

from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from bench.critical_queries import QuerySpec, load_critical_queries  # noqa: E402
from bench.sample_pool import SamplePool, build_params, load_sample_pool  # noqa: E402
from sqlalchemy import Connection, create_engine, text  # noqa: E402

from evidencehub.common.config import get_settings  # noqa: E402

OUT_PATH = REPO_ROOT / "bench" / "out" / "explain_capture.json"
SHARD_INDEX = 0


def _capture_one(
    conn: Connection, spec: QuerySpec, pool: SamplePool, rng: random.Random
) -> dict[str, Any]:
    params = build_params(spec.name, pool, rng)

    thread_id = conn.execute(
        text(
            "SELECT THREAD_ID FROM performance_schema.threads "
            "WHERE PROCESSLIST_ID = CONNECTION_ID()"
        )
    ).scalar_one()

    start = time.perf_counter()
    result = conn.execute(text(spec.sql_body), params)
    rows = result.fetchall()
    wall_ms = (time.perf_counter() - start) * 1000

    perf_row = (
        conn.execute(
            text(
                "SELECT ROWS_EXAMINED, ROWS_SENT, TIMER_WAIT "
                "FROM performance_schema.events_statements_history "
                "WHERE THREAD_ID = :tid ORDER BY EVENT_ID DESC LIMIT 1"
            ),
            {"tid": thread_id},
        )
        .mappings()
        .first()
    )

    explain_text = conn.execute(text(f"EXPLAIN ANALYZE {spec.sql_body}"), params).scalar_one()

    return {
        "name": spec.name,
        "slo_ms": spec.slo_ms,
        "params": {k: str(v) for k, v in params.items()},
        "rows_returned": len(rows),
        "wall_ms": round(wall_ms, 3),
        "rows_examined": int(perf_row["ROWS_EXAMINED"]) if perf_row else None,
        "rows_sent": int(perf_row["ROWS_SENT"]) if perf_row else None,
        "engine_ms": round(perf_row["TIMER_WAIT"] / 1_000_000_000, 3) if perf_row else None,
        "explain_analyze": explain_text,
    }


def main() -> None:
    settings = get_settings()
    queries = load_critical_queries()
    engine = create_engine(settings.shard_url(SHARD_INDEX), pool_pre_ping=True)
    pool = load_sample_pool(engine)
    rng = random.Random(7)

    captures: list[dict[str, Any]] = []
    with engine.connect() as conn:
        for spec in queries:
            capture = _capture_one(conn, spec, pool, rng)
            captures.append(capture)
            print(
                f"captured {spec.name}: wall={capture['wall_ms']}ms "
                f"rows_examined={capture['rows_examined']}"
            )
    engine.dispose()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps({"shard_index": SHARD_INDEX, "queries": captures}, indent=2))
    print(f"wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
