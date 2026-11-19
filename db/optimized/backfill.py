"""Resumable chunked backfill: evidence_legacy -> optimized evidence tables (one shard).

Two read orders:

* ``pk`` (default): walks legacy in primary-key order (``id > :last ORDER BY id LIMIT n``).
  Every row is visited once; each chunk is one transaction that also advances the
  checkpoint in ``backfill_progress``, so a crash re-runs at most the in-flight chunk.
* ``--month YYYY-MM``: one scan of legacy for that month (no observed_at index exists, so
  this is a full table scan + filesort), then inserts in clustered-key order.

Ids are deterministic (see evidencehub.common.snowflake.backfill_id), so re-running
over rows already written fails loudly on the primary key instead of duplicating them.
Additive only: reads `evidence_legacy`, writes the new tables.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import statistics
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import Connection, create_engine  # noqa: E402

from evidencehub.common.config import get_settings  # noqa: E402
from evidencehub.migration.normalize import (  # noqa: E402
    NormalizedEvidence,
    normalize_legacy_row,
)

logger = logging.getLogger("evidencehub.backfill")
GIB = 1024**3

LEGACY_COLUMNS = (
    "id, entity_id, entity_name, source_name, source_type, evidence_type, status, "
    "confidence_score, observed_at, content_hash, payload_blob, reviewer_notes, "
    "is_deleted, created_at, updated_at"
)


class Dims:
    """In-memory dimension caches, backed by INSERT IGNORE + SELECT."""

    def __init__(self) -> None:
        self.sources: dict[tuple[str, str], int] = {}
        self.types: dict[str, int] = {}
        self.entities: set[int] = set()

    def resolve(self, conn: Connection, recs: Sequence[NormalizedEvidence]) -> None:
        new_src = {(r.source_name, r.source_type) for r in recs} - self.sources.keys()
        if new_src:
            conn.exec_driver_sql(
                "INSERT IGNORE INTO dim_source (name, source_type) VALUES (%s, %s)",
                list(new_src),
            )
            for sid, name, stype in conn.exec_driver_sql(
                "SELECT source_id, name, source_type FROM dim_source"
            ):
                self.sources[(name, stype)] = sid
        new_types = {r.evidence_type for r in recs} - self.types.keys()
        if new_types:
            conn.exec_driver_sql(
                "INSERT IGNORE INTO dim_evidence_type (name) VALUES (%s)",
                [(t,) for t in new_types],
            )
            for tid, name in conn.exec_driver_sql("SELECT type_id, name FROM dim_evidence_type"):
                self.types[name] = tid
        new_ents: dict[int, tuple[int, str, str | None]] = {}
        for r in recs:
            if r.entity_pk not in self.entities and r.entity_pk not in new_ents:
                new_ents[r.entity_pk] = (r.entity_pk, r.entity_key, r.entity_name)
        if new_ents:
            conn.exec_driver_sql(
                "INSERT IGNORE INTO dim_entity (entity_pk, entity_key, name) VALUES (%s, %s, %s)",
                list(new_ents.values()),
            )
            self.entities.update(new_ents)


def write_batch(conn: Connection, dims: Dims, recs: Sequence[NormalizedEvidence]) -> None:
    """Insert one batch (caller owns the transaction). Rows go in clustered-key order."""
    dims.resolve(conn, recs)
    ordered = sorted(recs, key=lambda r: (r.observed_at, r.id))
    conn.exec_driver_sql(
        "INSERT INTO evidence (id, observed_at, entity_pk, source_id, type_id, status, "
        "confidence_score, is_deleted, content_hash, created_at, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        [
            (
                r.id,
                r.observed_at,
                r.entity_pk,
                dims.sources[(r.source_name, r.source_type)],
                dims.types[r.evidence_type],
                r.status,
                r.confidence_score,
                r.is_deleted,
                r.content_hash,
                r.created_at,
                r.updated_at,
            )
            for r in ordered
        ],
    )
    conn.exec_driver_sql(
        "INSERT INTO evidence_payload (id, observed_at, payload, reviewer_notes) "
        "VALUES (%s,%s,%s,%s)",
        [(r.id, r.observed_at, r.payload, r.reviewer_notes) for r in ordered],
    )
    # First writer of a hash wins; later duplicates keep their evidence row but the
    # dedup table points at the earliest-inserted id.
    conn.exec_driver_sql(
        "INSERT IGNORE INTO evidence_content_hash (content_hash, evidence_id) VALUES (%s,%s)",
        [(r.content_hash, r.id) for r in ordered],
    )


def _checkpoint(conn: Connection, job: str, last_id: str, scanned: int, written: int) -> None:
    conn.exec_driver_sql(
        "INSERT INTO backfill_progress (job, last_legacy_id, rows_scanned, rows_written) "
        "VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE "
        "last_legacy_id=VALUES(last_legacy_id), "
        "rows_scanned=VALUES(rows_scanned), rows_written=VALUES(rows_written)",
        (job, last_id, scanned, written),
    )


def _disk_ok(min_free_gib: float) -> tuple[bool, float]:
    free = shutil.disk_usage("/").free / GIB
    return free >= min_free_gib, free


def _summarize(
    metrics: list[dict[str, Any]], elapsed: float, extra: dict[str, Any]
) -> dict[str, Any]:
    totals = [m["total_s"] for m in metrics]
    rates = sorted(m["rows"] / m["total_s"] for m in metrics if m["total_s"] > 0)
    rows = sum(m["rows"] for m in metrics)
    denom = max(sum(totals), 1e-9)
    return {
        "chunks": len(metrics),
        "rows_written": rows,
        "elapsed_s": round(elapsed, 3),
        "rows_per_s_overall": round(rows / elapsed, 1) if elapsed else 0,
        "chunk_rows_per_s_median": round(statistics.median(rates), 1) if rates else 0,
        "chunk_rows_per_s_p5": round(rates[int(len(rates) * 0.05)], 1) if rates else 0,
        "chunk_rows_per_s_p95": round(rates[int(len(rates) * 0.95)], 1) if rates else 0,
        "read_share": round(sum(m.get("read_s", 0) for m in metrics) / denom, 3),
        "write_share": round(sum(m["write_s"] for m in metrics) / denom, 3),
        **extra,
    }


def run_pk_order(
    shard: int, job: str, chunk: int, max_scanned: int | None, min_free_gib: float
) -> tuple[int, dict[str, Any], list[dict[str, Any]]]:
    engine = create_engine(get_settings().shard_url(shard), pool_pre_ping=True)
    dims = Dims()
    metrics: list[dict[str, Any]] = []
    with engine.connect() as conn:
        row = conn.exec_driver_sql(
            "SELECT last_legacy_id, rows_scanned, rows_written FROM backfill_progress WHERE job=%s",
            (job,),
        ).first()
        conn.rollback()
        last_id, scanned, written = (row[0], row[1], row[2]) if row else ("", 0, 0)
        logger.info("resume job=%s from last_id=%r scanned=%d", job, last_id, scanned)
        started = time.perf_counter()
        status = 0
        while max_scanned is None or scanned < max_scanned:
            ok, free = _disk_ok(min_free_gib)
            if not ok:
                logger.error("STOP: host free %.2f GiB < %.2f GiB guardrail", free, min_free_gib)
                status = 3
                break
            limit = chunk if max_scanned is None else min(chunk, max_scanned - scanned)
            t0 = time.perf_counter()
            rows = conn.exec_driver_sql(
                f"SELECT {LEGACY_COLUMNS} FROM evidence_legacy WHERE id > %s ORDER BY id LIMIT %s",
                (last_id, limit),
            ).fetchall()
            t1 = time.perf_counter()
            if not rows:
                conn.rollback()
                break
            write_batch(conn, dims, [normalize_legacy_row(r) for r in rows])
            last_id = rows[-1].id
            scanned += len(rows)
            written += len(rows)
            _checkpoint(conn, job, last_id, scanned, written)
            conn.commit()
            t2 = time.perf_counter()
            metrics.append(
                {"rows": len(rows), "read_s": t1 - t0, "write_s": t2 - t1, "total_s": t2 - t0}
            )
            if len(metrics) % 50 == 0:
                logger.info(
                    "chunks=%d scanned=%d rows/s(cum)=%.0f free=%.1fGiB",
                    len(metrics),
                    scanned,
                    scanned / (time.perf_counter() - started),
                    free,
                )
    summary = _summarize(
        metrics,
        time.perf_counter() - started,
        {"mode": "pk", "rows_scanned_total": scanned, "entities_seen": len(dims.entities)},
    )
    return status, summary, metrics


def _next_month(month: str) -> str:
    year, mon = (int(p) for p in month.split("-"))
    return f"{year + 1}-01" if mon == 12 else f"{year}-{mon + 1:02d}"


def run_month(
    shard: int, month: str, chunk: int, min_free_gib: float
) -> tuple[int, dict[str, Any], list[dict[str, Any]]]:
    engine = create_engine(get_settings().shard_url(shard), pool_pre_ping=True)
    dims = Dims()
    metrics: list[dict[str, Any]] = []
    job = f"month-{month}"
    ok, free = _disk_ok(min_free_gib)
    if not ok:
        logger.error("STOP: host free %.2f GiB < %.2f GiB guardrail", free, min_free_gib)
        return 3, {}, []
    with engine.connect() as conn:
        t0 = time.perf_counter()
        rows = conn.exec_driver_sql(
            f"SELECT {LEGACY_COLUMNS} FROM evidence_legacy "
            "WHERE observed_at >= %s AND observed_at < %s ORDER BY observed_at, id",
            (f"{month}-01 00:00:00", f"{_next_month(month)}-01 00:00:00"),
        ).fetchall()
        scan_s = time.perf_counter() - t0
        conn.rollback()
        logger.info("month %s: legacy scan returned %d rows in %.1fs", month, len(rows), scan_s)
        recs = sorted((normalize_legacy_row(r) for r in rows), key=lambda r: (r.observed_at, r.id))
        started = time.perf_counter()
        for i in range(0, len(recs), chunk):
            batch = recs[i : i + chunk]
            t1 = time.perf_counter()
            write_batch(conn, dims, batch)
            _checkpoint(conn, job, "", i + len(batch), i + len(batch))
            conn.commit()
            t2 = time.perf_counter()
            metrics.append({"rows": len(batch), "write_s": t2 - t1, "total_s": t2 - t1})
        insert_s = time.perf_counter() - started
    summary = _summarize(
        metrics,
        insert_s,
        {
            "mode": "month",
            "month": month,
            "legacy_scan_s": round(scan_s, 3),
            "legacy_rows_in_month": len(rows),
            "rows_per_s_including_scan": round(len(rows) / (scan_s + insert_s), 1),
        },
    )
    return 0, summary, metrics


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--shard", type=int, required=True, help="0-based shard index")
    p.add_argument("--job", default="sample")
    p.add_argument("--chunk", type=int, default=2000)
    p.add_argument("--max-scanned", type=int, default=None)
    p.add_argument("--month", default=None, help="YYYY-MM: month-ordered single-month load")
    p.add_argument("--min-free-gib", type=float, default=10.0)
    p.add_argument("--metrics-out", type=Path, default=None)
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    if a.month:
        status, summary, metrics = run_month(a.shard, a.month, a.chunk, a.min_free_gib)
    else:
        status, summary, metrics = run_pk_order(
            a.shard, a.job, a.chunk, a.max_scanned, a.min_free_gib
        )
    print(json.dumps(summary, indent=2))
    if a.metrics_out:
        a.metrics_out.write_text(json.dumps({"summary": summary, "chunks": metrics}))
    sys.exit(status)


if __name__ == "__main__":
    main()
