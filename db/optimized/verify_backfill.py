"""Read-only verification of a backfill job against evidence_legacy (one shard).

Scope is the legacy PK range the job covered (``id <= backfill_progress.last_legacy_id``).

1. Per-month: COUNT(*) and SUM(CRC32(CONCAT_WS(...))) computed in SQL on both sides
   over the same normalized textual form of every column (dimensions joined back to
   names, status decoded, payload re-rendered through MySQL's JSON type).
2. Per-chunk: the same legacy PK walk as the backfill (chunks of --chunk rows); for each
   chunk, a CRC32 sum over a canonical Python rendering of legacy rows vs. the new rows
   fetched by their deterministic (id, observed_at). Covers every row in range.
3. Spot-check: N random legacy rows located on the new side through the access path the
   app uses (entity_key -> dim_entity -> evidence by entity/observed_at/hash), NOT through
   the id function, then compared field by field (including the id it should have).
4. Dimension row counts vs. distinct legacy values in range.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
import zlib
from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import Connection, create_engine  # noqa: E402

from evidencehub.common.config import get_settings  # noqa: E402
from evidencehub.migration.normalize import STATUS_NAMES, normalize_legacy_row  # noqa: E402

NULL = "\\N"

LEGACY_MONTH_SQL = """
SELECT LEFT(observed_at, 7) AS m, COUNT(*) AS n,
  SUM(CRC32(CONCAT_WS('|', entity_id, COALESCE(entity_name, '\\\\N'), source_name, source_type,
    evidence_type, status, COALESCE(confidence_score, '\\\\N'), observed_at, LOWER(content_hash),
    is_deleted, created_at, updated_at,
    COALESCE(CAST(CAST(payload_blob AS JSON) AS CHAR), '\\\\N'),
    COALESCE(reviewer_notes, '\\\\N')))) AS crc
FROM evidence_legacy WHERE id <= %s GROUP BY m ORDER BY m
"""

NEW_MONTH_SQL = """
SELECT LEFT(CAST(e.observed_at AS CHAR), 7) AS m, COUNT(*) AS n,
  SUM(CRC32(CONCAT_WS('|', d.entity_key, COALESCE(d.name, '\\\\N'), s.name, s.source_type,
    t.name, ELT(e.status, 'pending', 'under_review', 'verified', 'rejected'),
    COALESCE(CAST(e.confidence_score AS CHAR), '\\\\N'),
    CAST(e.observed_at AS CHAR), LOWER(HEX(e.content_hash)),
    IF(e.is_deleted, 'true', 'false'), e.created_at, e.updated_at,
    COALESCE(CAST(p.payload AS CHAR), '\\\\N'), COALESCE(p.reviewer_notes, '\\\\N')))) AS crc
FROM evidence e
JOIN evidence_payload p ON p.id = e.id
JOIN dim_entity d ON d.entity_pk = e.entity_pk
JOIN dim_source s ON s.source_id = e.source_id
JOIN dim_evidence_type t ON t.type_id = e.type_id
GROUP BY m ORDER BY m
"""

LEGACY_COLS = (
    "id, entity_id, entity_name, source_name, source_type, evidence_type, status, "
    "confidence_score, observed_at, content_hash, payload_blob, reviewer_notes, "
    "is_deleted, created_at, updated_at"
)

NEW_ROW_SQL = (
    "SELECT e.id, d.entity_key, d.name AS entity_name, s.name AS source_name, s.source_type, "
    "t.name AS evidence_type, e.status, e.confidence_score, e.observed_at, e.content_hash, "
    "e.is_deleted, e.created_at, e.updated_at, p.payload, p.reviewer_notes "
    "FROM evidence e JOIN evidence_payload p ON p.id = e.id "
    "JOIN dim_entity d ON d.entity_pk = e.entity_pk "
    "JOIN dim_source s ON s.source_id = e.source_id "
    "JOIN dim_evidence_type t ON t.type_id = e.type_id "
)


def _canon_json(value: str | None) -> str:
    return NULL if value is None else json.dumps(json.loads(value), sort_keys=True)


def _fmt(value: Any) -> str:
    if value is None:
        return NULL
    if isinstance(value, Decimal):
        return f"{value:.4f}"
    return str(value)


def canon_legacy(r: Any) -> tuple[str, ...]:
    return (
        r.entity_id,
        _fmt(r.entity_name),
        r.source_name,
        r.source_type,
        r.evidence_type,
        r.status,
        _fmt(r.confidence_score),
        r.observed_at,
        r.content_hash.lower(),
        r.is_deleted,
        _fmt(r.created_at),
        _fmt(r.updated_at),
        _canon_json(r.payload_blob),
        _fmt(r.reviewer_notes),
    )


def canon_new(r: Any) -> tuple[str, ...]:
    return (
        r.entity_key,
        _fmt(r.entity_name),
        r.source_name,
        r.source_type,
        r.evidence_type,
        STATUS_NAMES[r.status],
        _fmt(r.confidence_score),
        r.observed_at.strftime("%Y-%m-%d %H:%M:%S"),
        r.content_hash.hex(),
        "true" if r.is_deleted else "false",
        _fmt(r.created_at),
        _fmt(r.updated_at),
        _canon_json(r.payload),
        _fmt(r.reviewer_notes),
    )


def _crc_sum(rows: Iterable[tuple[str, ...]]) -> int:
    return sum(zlib.crc32("|".join(t).encode("utf-8")) for t in rows)


def check_months(conn: Connection, last_id: str) -> dict[str, Any]:
    legacy = {r.m: (r.n, int(r.crc)) for r in conn.exec_driver_sql(LEGACY_MONTH_SQL, (last_id,))}
    new = {r.m: (r.n, int(r.crc)) for r in conn.exec_driver_sql(NEW_MONTH_SQL)}
    mismatches = [
        {"month": m, "legacy": legacy.get(m), "new": new.get(m)}
        for m in sorted(legacy.keys() | new.keys())
        if legacy.get(m) != new.get(m)
    ]
    return {
        "months": len(legacy),
        "legacy_rows": sum(v[0] for v in legacy.values()),
        "new_rows": sum(v[0] for v in new.values()),
        "mismatched_months": mismatches,
    }


def check_chunks(conn: Connection, last_id: str, chunk: int) -> dict[str, Any]:
    cursor, chunks, rows_checked = "", 0, 0
    bad: list[dict[str, Any]] = []
    while True:
        legacy = conn.exec_driver_sql(
            f"SELECT {LEGACY_COLS} FROM evidence_legacy "
            "WHERE id > %s AND id <= %s ORDER BY id LIMIT %s",
            (cursor, last_id, chunk),
        ).fetchall()
        if not legacy:
            break
        keys = [(n.id, n.observed_at) for n in (normalize_legacy_row(r) for r in legacy)]
        placeholders = ",".join(["(%s,%s)"] * len(keys))
        flat = [v for k in keys for v in k]
        new = conn.exec_driver_sql(
            NEW_ROW_SQL + f"WHERE (e.id, e.observed_at) IN ({placeholders})", tuple(flat)
        ).fetchall()
        lsum, nsum = _crc_sum(map(canon_legacy, legacy)), _crc_sum(map(canon_new, new))
        if len(new) != len(legacy) or lsum != nsum:
            bad.append(
                {"after": cursor, "legacy_n": len(legacy), "new_n": len(new), "eq": lsum == nsum}
            )
        cursor = legacy[-1].id
        chunks += 1
        rows_checked += len(legacy)
    conn.rollback()
    return {"chunks": chunks, "rows_checked": rows_checked, "mismatched_chunks": bad}


def spot_check(conn: Connection, last_id: str, n: int, seed: int) -> dict[str, Any]:
    rng = random.Random(seed)
    failures: list[dict[str, Any]] = []
    checked = 0
    upper = uuid.UUID(last_id).int
    for _ in range(n):
        probe = str(uuid.UUID(int=rng.randrange(upper)))
        r = conn.exec_driver_sql(
            f"SELECT {LEGACY_COLS} FROM evidence_legacy WHERE id >= %s AND id <= %s "
            "ORDER BY id LIMIT 1",
            (probe, last_id),
        ).first()
        if r is None:
            continue
        expected = normalize_legacy_row(r)
        found = conn.exec_driver_sql(
            NEW_ROW_SQL + "WHERE d.entity_key = %s AND e.observed_at = %s AND e.content_hash = %s",
            (r.entity_id, expected.observed_at, expected.content_hash),
        ).fetchall()
        checked += 1
        problems = []
        if len(found) != 1:
            problems.append(f"found {len(found)} rows via entity/observed_at/hash")
        else:
            f = found[0]
            if f.id != expected.id:
                problems.append(f"id {f.id} != expected {expected.id}")
            for i, (a, b) in enumerate(zip(canon_legacy(r), canon_new(f), strict=True)):
                if a != b:
                    problems.append(f"field {i}: {a!r} != {b!r}")
        if problems:
            failures.append({"legacy_id": r.id, "problems": problems})
    conn.rollback()
    return {"checked": checked, "failures": failures}


def check_dims(conn: Connection, last_id: str) -> dict[str, Any]:
    legacy = conn.exec_driver_sql(
        "SELECT COUNT(DISTINCT entity_id) AS ents,"
        " COUNT(DISTINCT source_name, source_type) AS srcs,"
        " COUNT(DISTINCT evidence_type) AS types, COUNT(DISTINCT content_hash) AS hashes "
        "FROM evidence_legacy WHERE id <= %s",
        (last_id,),
    ).one()
    new = conn.exec_driver_sql(
        "SELECT (SELECT COUNT(*) FROM dim_entity) AS ents,"
        " (SELECT COUNT(*) FROM dim_source) AS srcs,"
        " (SELECT COUNT(*) FROM dim_evidence_type) AS types,"
        " (SELECT COUNT(*) FROM evidence_content_hash) AS hashes,"
        " (SELECT COUNT(*) FROM evidence_payload) AS payloads,"
        " (SELECT COUNT(*) FROM evidence) AS evidence"
    ).one()
    conn.rollback()
    return {"legacy_distinct": dict(legacy._mapping), "new_counts": dict(new._mapping)}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--shard", type=int, required=True)
    p.add_argument("--job", default="sample")
    p.add_argument("--chunk", type=int, default=2000)
    p.add_argument("--spot", type=int, default=200)
    p.add_argument("--seed", type=int, default=20261119)
    a = p.parse_args()
    engine = create_engine(get_settings().shard_url(a.shard), pool_pre_ping=True)
    with engine.connect() as conn:
        row = conn.exec_driver_sql(
            "SELECT last_legacy_id, rows_scanned FROM backfill_progress WHERE job=%s", (a.job,)
        ).one()
        last_id = row[0]
        conn.rollback()
        result = {
            "job": a.job,
            "last_legacy_id": last_id,
            "rows_scanned_by_job": row[1],
            "months": check_months(conn, last_id),
            "chunks": check_chunks(conn, last_id, a.chunk),
            "spot": spot_check(conn, last_id, a.spot, a.seed),
            "dims": check_dims(conn, last_id),
        }
    print(json.dumps(result, indent=2, default=str))
    ok = (
        not result["months"]["mismatched_months"]
        and not result["chunks"]["mismatched_chunks"]
        and not result["spot"]["failures"]
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
