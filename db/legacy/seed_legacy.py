"""Synthetic data generator for the legacy evidence_legacy table (P1 baseline).

Usage:
    uv run python db/legacy/seed_legacy.py            # fast dev scale (SEED_ROWS_DEFAULT)
    uv run python db/legacy/seed_legacy.py --full      # full perf-report scale (SEED_ROWS_FULL)
    uv run python db/legacy/seed_legacy.py --rows 500000

One worker process per shard; entities are routed by crc32(entity_id) % shard_count
so re-running with the same --seed reproduces the same distribution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import sys
import time
import uuid
import zlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import create_engine, text  # noqa: E402

from evidencehub.common.config import get_settings  # noqa: E402
from evidencehub.common.logging import configure_logging  # noqa: E402

logger = logging.getLogger("evidencehub.seed.legacy")

SOURCES: list[tuple[str, str]] = [
    ("rest-poller-a", "rest"),
    ("rest-poller-b", "rest"),
    ("s3-csv-drop", "file"),
    ("kafka-stream-x", "stream"),
    ("kafka-stream-y", "stream"),
]
EVIDENCE_TYPES: list[str] = [
    "document",
    "testimony",
    "photo",
    "financial_record",
    "communication_log",
    "physical_sample",
    "digital_forensics",
    "witness_statement",
]
STATUSES_WEIGHTED: list[tuple[str, int]] = [
    ("verified", 55),
    ("pending", 25),
    ("under_review", 15),
    ("rejected", 5),
]
NOTE_SNIPPETS: list[str | None] = [
    None,
    None,
    None,
    "Escalated for secondary review.",
    "Chain of custody confirmed.",
    "Awaiting corroborating source.",
    "Flagged by automated validator.",
]

INSERT_SQL = text(
    "INSERT INTO evidence_legacy "
    "(id, entity_id, entity_name, source_name, source_type, evidence_type, "
    " status, confidence_score, observed_at, content_hash, payload_blob, "
    " reviewer_notes, is_deleted) "
    "VALUES (:id, :entity_id, :entity_name, :source_name, :source_type, :evidence_type, "
    " :status, :confidence_score, :observed_at, :content_hash, :payload_blob, "
    " :reviewer_notes, :is_deleted)"
)


def _shard_index_for_entity(entity_id: str, shard_count: int) -> int:
    return zlib.crc32(entity_id.encode("utf-8")) % shard_count


def _random_datetime(rng: random.Random, start: datetime, end: datetime) -> datetime:
    delta_seconds = (end - start).total_seconds()
    return start + timedelta(seconds=rng.uniform(0, delta_seconds))


def _weighted_choice(rng: random.Random, options: list[tuple[str, int]]) -> str:
    total = sum(weight for _, weight in options)
    pick = rng.uniform(0, total)
    upto = 0.0
    for value, weight in options:
        upto += weight
        if pick <= upto:
            return value
    return options[-1][0]


def _build_row(rng: random.Random, entity_id: str) -> dict[str, Any]:
    source_name, source_type = rng.choice(SOURCES)
    observed_at = _random_datetime(rng, datetime(2023, 1, 1, tzinfo=UTC), datetime.now(UTC))
    confidence = round(rng.uniform(0.10, 0.99), 4)
    raw_source_id = str(uuid.uuid4())
    payload = {
        "raw_source_id": raw_source_id,
        "tags": rng.sample(
            ["priority", "flagged", "corroborated", "duplicate-check", "auto-ingested"],
            k=rng.randint(0, 2),
        ),
        "geo": {"lat": round(rng.uniform(-90, 90), 4), "lon": round(rng.uniform(-180, 180), 4)},
    }
    content_hash = hashlib.sha256(
        f"{entity_id}|{observed_at.isoformat()}|{raw_source_id}".encode()
    ).hexdigest()
    return {
        "id": str(uuid.uuid4()),
        "entity_id": entity_id,
        "entity_name": f"Case {entity_id.split('-')[1]}",
        "source_name": source_name,
        "source_type": source_type,
        "evidence_type": rng.choice(EVIDENCE_TYPES),
        "status": _weighted_choice(rng, STATUSES_WEIGHTED),
        "confidence_score": f"{confidence:.4f}",
        "observed_at": observed_at.strftime("%Y-%m-%d %H:%M:%S"),
        "content_hash": content_hash,
        "payload_blob": json.dumps(payload),
        "reviewer_notes": rng.choice(NOTE_SNIPPETS),
        "is_deleted": "true" if rng.random() < 0.01 else "false",
    }


def _seed_shard(
    shard_index: int,
    entity_ids: list[str],
    rows_per_entity_range: tuple[int, int],
    batch_size: int,
    seed: int,
) -> tuple[int, float]:
    settings = get_settings()
    engine = create_engine(settings.shard_url(shard_index), pool_pre_ping=True)
    rng = random.Random(seed)
    batch: list[dict[str, Any]] = []
    total = 0
    start = time.monotonic()
    try:
        with engine.connect() as conn:
            for entity_id in entity_ids:
                rows_for_entity = rng.randint(*rows_per_entity_range)
                for _ in range(rows_for_entity):
                    batch.append(_build_row(rng, entity_id))
                    if len(batch) >= batch_size:
                        with conn.begin():
                            conn.execute(INSERT_SQL, batch)
                        total += len(batch)
                        batch.clear()
            if batch:
                with conn.begin():
                    conn.execute(INSERT_SQL, batch)
                total += len(batch)
    finally:
        engine.dispose()
    return total, time.monotonic() - start


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed the legacy evidence_legacy table across all shards."
    )
    parser.add_argument("--rows", type=int, default=None, help="Target total row count.")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Use SEED_ROWS_FULL (the official perf-report scale) instead of the dev default.",
    )
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--seed", type=int, default=42, help="RNG seed, for reproducibility.")
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)

    target_rows = args.rows or (
        settings.seed_rows_full if args.full else settings.seed_rows_default
    )
    batch_size = args.batch_size or settings.seed_batch_size
    shard_count = settings.shard_count

    entity_pool_size = max(10_000, target_rows // 40)
    entity_ids = [f"entity-{i:08d}" for i in range(entity_pool_size)]
    rows_per_entity_avg = target_rows / entity_pool_size
    rows_per_entity_range = (
        max(1, int(rows_per_entity_avg * 0.4)),
        max(2, int(rows_per_entity_avg * 1.6)),
    )

    by_shard: dict[int, list[str]] = {i: [] for i in range(shard_count)}
    for entity_id in entity_ids:
        by_shard[_shard_index_for_entity(entity_id, shard_count)].append(entity_id)

    logger.info(
        "seed.start",
        extra={
            "target_rows": target_rows,
            "entity_pool_size": entity_pool_size,
            "shard_count": shard_count,
            "batch_size": batch_size,
        },
    )

    start = time.monotonic()
    total_inserted = 0
    with ProcessPoolExecutor(max_workers=shard_count) as pool_exec:
        futures = {
            pool_exec.submit(
                _seed_shard,
                shard_index,
                ids,
                rows_per_entity_range,
                batch_size,
                args.seed + shard_index,
            ): shard_index
            for shard_index, ids in by_shard.items()
        }
        for future in as_completed(futures):
            shard_index = futures[future]
            rows, elapsed = future.result()
            total_inserted += rows
            logger.info(
                "seed.shard_done",
                extra={
                    "shard_index": shard_index,
                    "rows": rows,
                    "elapsed_s": round(elapsed, 1),
                    "rows_per_s": round(rows / elapsed, 1) if elapsed else 0.0,
                },
            )

    total_elapsed = time.monotonic() - start
    logger.info(
        "seed.done",
        extra={
            "total_rows": total_inserted,
            "elapsed_s": round(total_elapsed, 1),
            "rows_per_s": round(total_inserted / total_elapsed, 1) if total_elapsed else 0.0,
        },
    )


if __name__ == "__main__":
    main()
