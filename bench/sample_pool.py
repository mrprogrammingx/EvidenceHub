"""Realistic bind-parameter sampling for the critical-query benchmark harness.

Draws a random sample of real rows from a seeded shard so benchmark queries
run against ids/entity_ids/hashes that actually exist, instead of synthetic
values that would always miss.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import Engine, text


@dataclass(frozen=True)
class SampleRow:
    id: str
    entity_id: str
    status: str
    evidence_type: str
    observed_at: str
    content_hash: str


@dataclass(frozen=True)
class SamplePool:
    rows: list[SampleRow]
    window_from: str
    window_to: str

    def random_row(self, rng: random.Random) -> SampleRow:
        return rng.choice(self.rows)


def load_sample_pool(engine: Engine, sample_size: int = 500, window_days: int = 180) -> SamplePool:
    query = text(
        "SELECT id, entity_id, status, evidence_type, observed_at, content_hash "
        "FROM evidence_legacy ORDER BY RAND() LIMIT :n"
    )
    with engine.connect() as conn:
        result = conn.execute(query, {"n": sample_size})
        rows = [SampleRow(**dict(row._mapping)) for row in result]
    if not rows:
        raise RuntimeError("evidence_legacy is empty -- run the seed generator first")
    now = datetime.now(UTC)
    window_from = (now - timedelta(days=window_days)).strftime("%Y-%m-%d %H:%M:%S")
    window_to = now.strftime("%Y-%m-%d %H:%M:%S")
    return SamplePool(rows=rows, window_from=window_from, window_to=window_to)


def build_params(query_name: str, pool: SamplePool, rng: random.Random) -> dict[str, object]:
    row = pool.random_row(rng)
    if query_name == "point_lookup_by_id":
        return {"id": row.id}
    if query_name == "latest_n_by_entity_status":
        return {"entity_id": row.entity_id, "status": row.status, "limit_n": 20}
    if query_name == "latest_n_by_entity_type_daterange":
        return {
            "entity_id": row.entity_id,
            "evidence_type": row.evidence_type,
            "from_ts": pool.window_from,
            "to_ts": pool.window_to,
            "limit_n": 20,
        }
    if query_name == "per_entity_status_aggregate":
        return {"entity_id": row.entity_id}
    if query_name == "range_aggregate_status_window":
        return {"from_ts": pool.window_from, "to_ts": pool.window_to}
    if query_name == "content_hash_dedup_lookup":
        return {"content_hash": row.content_hash}
    if query_name == "top_entities_by_volume_window":
        return {"from_ts": pool.window_from, "to_ts": pool.window_to, "limit_n": 10}
    if query_name == "keyset_paginated_feed":
        return {
            "entity_id": row.entity_id,
            "last_observed_at": row.observed_at,
            "last_id": row.id,
            "limit_n": 20,
        }
    raise ValueError(f"no param builder registered for query {query_name!r}")
