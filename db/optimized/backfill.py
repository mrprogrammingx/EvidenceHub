"""Resumable chunked backfill: evidence_legacy -> optimized evidence tables (one shard).

Two read orders, both writing through evidencehub.migration.writer.write_records (the
same path the dual-writer uses), so re-runs skip rows already loaded and id collisions
stop the run:

* ``pk`` (default): walks legacy in primary-key order (``id > :last ORDER BY id LIMIT n``).
* ``--month YYYY-MM`` / ``--months START:END``: per month, one streamed scan of legacy
  ordered by (observed_at, id). legacy has no observed_at index, so each month is a full
  table scan plus a sort. A finished month is marked completed and skipped on re-run; an
  interrupted month resumes after its last committed (observed_at, legacy id).

Each batch is one transaction that also advances the checkpoint in ``backfill_progress``.
Exit codes: 0 done, 3 stopped by the free-disk guardrail (checkpoint saved), 4 stopped on
an id collision (batch rolled back, legacy ids logged).
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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import Connection, Engine, create_engine  # noqa: E402

from evidencehub.common.config import get_settings  # noqa: E402
from evidencehub.migration.normalize import normalize_legacy_row  # noqa: E402
from evidencehub.migration.writer import Dims, IdCollisionError, write_records  # noqa: E402

logger = logging.getLogger("evidencehub.backfill")
GIB = 1024**3
EXIT_OK, EXIT_DISK, EXIT_COLLISION = 0, 3, 4

LEGACY_COLUMNS = (
    "id, entity_id, entity_name, source_name, source_type, evidence_type, status, "
    "confidence_score, observed_at, content_hash, payload_blob, reviewer_notes, "
    "is_deleted, created_at, updated_at"
)


@dataclass
class RunStats:
    batches: list[dict[str, float]] = field(default_factory=list)
    inserted: int = 0
    skipped_existing: int = 0
    duplicates_of_earlier_hash: int = 0
    collisions: list[str] = field(default_factory=list)
    started: float = field(default_factory=time.perf_counter)

    def record(self, rows: int, read_s: float, write_s: float, outcomes: list[Any]) -> None:
        self.batches.append({"rows": rows, "read_s": read_s, "write_s": write_s})
        self.inserted += sum(not o.already_present for o in outcomes)
        self.skipped_existing += sum(o.already_present for o in outcomes)
        self.duplicates_of_earlier_hash += sum(o.duplicate_of is not None for o in outcomes)

    def summary(self, **extra: Any) -> dict[str, Any]:
        elapsed = time.perf_counter() - self.started
        rates = sorted(
            b["rows"] / (b["read_s"] + b["write_s"])
            for b in self.batches
            if b["read_s"] + b["write_s"] > 0
        )
        total = sum(b["read_s"] + b["write_s"] for b in self.batches) or 1e-9
        rows = sum(int(b["rows"]) for b in self.batches)
        return {
            "batches": len(self.batches),
            "rows_processed": rows,
            "inserted": self.inserted,
            "skipped_existing": self.skipped_existing,
            "duplicates_of_earlier_hash": self.duplicates_of_earlier_hash,
            "collisions": self.collisions,
            "elapsed_s": round(elapsed, 3),
            "rows_per_s_overall": round(rows / elapsed, 1) if elapsed else 0,
            "batch_rows_per_s_median": round(statistics.median(rates), 1) if rates else 0,
            "batch_rows_per_s_p5": round(rates[int(len(rates) * 0.05)], 1) if rates else 0,
            "batch_rows_per_s_p95": round(rates[int(len(rates) * 0.95)], 1) if rates else 0,
            "read_share": round(sum(b["read_s"] for b in self.batches) / total, 3),
            **extra,
        }


def _disk_ok(min_free_gib: float) -> tuple[bool, float]:
    free = shutil.disk_usage("/").free / GIB
    return free >= min_free_gib, free


def _write_batch(conn: Connection, dims: Dims, rows: list[Any], stats: RunStats) -> list[Any]:
    """Normalize and write one batch in the caller's transaction; rolls back on collision."""
    try:
        return write_records(conn, dims, [normalize_legacy_row(r) for r in rows])
    except IdCollisionError as exc:
        conn.rollback()
        dims.discard_pending()
        stats.collisions.extend(exc.details)
        logger.error("STOP: %s. Batch rolled back; checkpoint not advanced.", exc)
        raise


def run_pk_order(
    engine: Engine, job: str, chunk: int, max_scanned: int | None, min_free_gib: float
) -> tuple[int, dict[str, Any]]:
    dims, stats = Dims(), RunStats()
    with engine.connect() as conn:
        row = conn.exec_driver_sql(
            "SELECT last_legacy_id, rows_scanned FROM backfill_progress WHERE job=%s", (job,)
        ).first()
        conn.rollback()
        last_id, scanned = (row[0], row[1]) if row else ("", 0)
        logger.info("pk job=%s resume after %r (scanned=%d)", job, last_id, scanned)
        status = EXIT_OK
        while max_scanned is None or scanned < max_scanned:
            ok, free = _disk_ok(min_free_gib)
            if not ok:
                logger.error("STOP: host free %.2f GiB < %.2f GiB guardrail", free, min_free_gib)
                status = EXIT_DISK
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
            try:
                outcomes = _write_batch(conn, dims, rows, stats)
            except IdCollisionError:
                status = EXIT_COLLISION
                break
            last_id, scanned = rows[-1].id, scanned + len(rows)
            conn.exec_driver_sql(
                "INSERT INTO backfill_progress (job, last_legacy_id, rows_scanned, rows_written) "
                "VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE last_legacy_id = %s, "
                "rows_scanned = %s, rows_written = rows_written + %s",
                (job, last_id, scanned, len(rows), last_id, scanned, len(rows)),
            )
            conn.commit()
            dims.commit_pending()
            stats.record(len(rows), t1 - t0, time.perf_counter() - t1, outcomes)
    return status, stats.summary(mode="pk", job=job, rows_scanned_total=scanned)


def _next_month(month: str) -> str:
    year, mon = (int(p) for p in month.split("-"))
    return f"{year + 1}-01" if mon == 12 else f"{year}-{mon + 1:02d}"


def month_range(start: str, end: str) -> list[str]:
    months, cur = [], start
    while cur <= end:
        months.append(cur)
        cur = _next_month(cur)
    return months


def run_month(
    engine: Engine, month: str, chunk: int, min_free_gib: float
) -> tuple[int, dict[str, Any]]:
    job = f"month-{month}"
    dims, stats = Dims(), RunStats()
    with engine.connect() as wconn:
        progress = wconn.exec_driver_sql(
            "SELECT cursor_observed_at, last_legacy_id, rows_written, completed_at "
            "FROM backfill_progress WHERE job=%s",
            (job,),
        ).first()
        wconn.rollback()
        if progress is not None and progress.completed_at is not None:
            logger.info("month %s already completed at %s; skipping", month, progress.completed_at)
            return EXIT_OK, stats.summary(mode="month", month=month, skipped_completed=True)
        cursor = (
            (progress.cursor_observed_at, progress.last_legacy_id)
            if progress is not None and progress.cursor_observed_at
            else None
        )
        written = progress.rows_written if progress is not None else 0
        sql = (
            f"SELECT {LEGACY_COLUMNS} FROM evidence_legacy "
            "WHERE observed_at >= %s AND observed_at < %s"
        )
        params: tuple[Any, ...] = (f"{month}-01 00:00:00", f"{_next_month(month)}-01 00:00:00")
        if cursor:
            sql += " AND (observed_at > %s OR (observed_at = %s AND id > %s))"
            params += (cursor[0], cursor[0], cursor[1])
        sql += " ORDER BY observed_at, id"
        logger.info("month %s: start (resume cursor=%s)", month, cursor)
        status = EXIT_OK
        scan_first_row_s: float | None = None
        with engine.connect().execution_options(stream_results=True) as rconn:
            t_scan = time.perf_counter()
            result = rconn.exec_driver_sql(sql, params)
            while True:
                ok, free = _disk_ok(min_free_gib)
                if not ok:
                    logger.error(
                        "STOP: host free %.2f GiB < %.2f GiB guardrail", free, min_free_gib
                    )
                    status = EXIT_DISK
                    break
                t0 = time.perf_counter()
                rows = result.fetchmany(chunk)
                t1 = time.perf_counter()
                if scan_first_row_s is None:
                    scan_first_row_s = t1 - t_scan
                if not rows:
                    break
                try:
                    outcomes = _write_batch(wconn, dims, rows, stats)
                except IdCollisionError:
                    status = EXIT_COLLISION
                    break
                written += len(rows)
                last = rows[-1]
                wconn.exec_driver_sql(
                    "INSERT INTO backfill_progress (job, last_legacy_id, cursor_observed_at, "
                    "rows_scanned, rows_written) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE "
                    "last_legacy_id = %s, cursor_observed_at = %s, rows_scanned = %s, "
                    "rows_written = %s",
                    (
                        job,
                        last.id,
                        last.observed_at,
                        written,
                        written,
                        *(last.id, last.observed_at, written, written),
                    ),
                )
                wconn.commit()
                dims.commit_pending()
                stats.record(len(rows), t1 - t0, time.perf_counter() - t1, outcomes)
            result.close()
        if status == EXIT_OK:
            wconn.exec_driver_sql(
                "INSERT INTO backfill_progress (job, last_legacy_id, rows_scanned, rows_written, "
                "completed_at) VALUES (%s, '', %s, %s, NOW()) "
                "ON DUPLICATE KEY UPDATE completed_at = NOW()",
                (job, written, written),
            )
            wconn.commit()
    return status, stats.summary(
        mode="month",
        month=month,
        resumed_from=cursor,
        legacy_scan_to_first_row_s=round(scan_first_row_s or 0, 3),
        rows_written_total_for_month=written,
    )


def run_months(
    engine: Engine, months: list[str], chunk: int, min_free_gib: float
) -> tuple[int, list[dict[str, Any]]]:
    summaries = []
    for month in months:
        status, summary = run_month(engine, month, chunk, min_free_gib)
        summaries.append(summary)
        logger.info("month %s: %s", month, json.dumps(summary))
        if status != EXIT_OK:
            return status, summaries
    return EXIT_OK, summaries


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--shard", type=int, required=True, help="0-based shard index")
    p.add_argument("--job", default="sample")
    p.add_argument("--chunk", type=int, default=2000)
    p.add_argument("--max-scanned", type=int, default=None)
    p.add_argument("--month", default=None, help="YYYY-MM: one month in observed_at order")
    p.add_argument("--months", default=None, help="START:END, e.g. 2023-01:2026-09")
    p.add_argument("--min-free-gib", type=float, default=10.0)
    p.add_argument("--metrics-out", type=Path, default=None)
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    engine = create_engine(get_settings().shard_url(a.shard), pool_pre_ping=True)
    out: Any
    if a.months:
        start, end = a.months.split(":")
        status, out = run_months(engine, month_range(start, end), a.chunk, a.min_free_gib)
    elif a.month:
        status, out = run_month(engine, a.month, a.chunk, a.min_free_gib)
    else:
        status, out = run_pk_order(engine, a.job, a.chunk, a.max_scanned, a.min_free_gib)
    print(json.dumps(out, indent=2, default=str))
    if a.metrics_out:
        a.metrics_out.write_text(json.dumps(out, default=str))
    sys.exit(status)


if __name__ == "__main__":
    main()
