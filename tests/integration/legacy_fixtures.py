"""Helpers to seed `evidence_legacy` rows shaped like db/legacy/seed_legacy.py output."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import random
import sys
import uuid
import zlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import Engine

REPO_ROOT = Path(__file__).resolve().parents[2]


def load_script(relative: str) -> Any:
    """Import a repo script (e.g. db/optimized/backfill.py) as a module."""
    path = REPO_ROOT / relative
    name = f"_evidencehub_script_{path.stem}"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses resolve annotations via sys.modules
    spec.loader.exec_module(module)
    return module


INSERT = (
    "INSERT INTO evidence_legacy (id, entity_id, entity_name, source_name, source_type, "
    "evidence_type, status, confidence_score, observed_at, content_hash, payload_blob, "
    "reviewer_notes, is_deleted) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
)
MONTH_STARTS = [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1)]
WINDOW_END = datetime(2024, 4, 1)


def make_row(rng: random.Random, **overrides: Any) -> dict[str, Any]:
    legacy_id = str(uuid.UUID(int=rng.getrandbits(128)))
    entity = f"entity-{rng.randrange(40):08d}"
    observed = MONTH_STARTS[0] + timedelta(
        seconds=rng.randrange(int((WINDOW_END - MONTH_STARTS[0]).total_seconds()))
    )
    row: dict[str, Any] = {
        "id": legacy_id,
        "entity_id": entity,
        "entity_name": f"Case {entity[7:]}",
        "source_name": rng.choice(["rest-poller-a", "s3-csv-drop", "kafka-stream-x"]),
        "source_type": "rest",
        "evidence_type": rng.choice(["document", "photo", "testimony"]),
        "status": rng.choice(["verified", "pending", "under_review", "rejected"]),
        "confidence_score": f"{rng.uniform(0.1, 0.99):.4f}",
        "observed_at": observed.strftime("%Y-%m-%d %H:%M:%S"),
        "content_hash": hashlib.sha256(legacy_id.encode()).hexdigest(),
        "payload_blob": json.dumps({"tags": ["a"], "geo": {"lat": 1.25, "lon": -3.5}}),
        "reviewer_notes": rng.choice([None, "Chain of custody confirmed."]),
        "is_deleted": rng.choice(["false", "false", "true"]),
    }
    row.update(overrides)
    return row


def insert_rows(engine: Engine, rows: list[dict[str, Any]]) -> None:
    cols = [
        "id", "entity_id", "entity_name", "source_name", "source_type", "evidence_type",
        "status", "confidence_score", "observed_at", "content_hash", "payload_blob",
        "reviewer_notes", "is_deleted",
    ]  # fmt: skip
    with engine.begin() as conn:
        conn.exec_driver_sql(INSERT, [tuple(r[c] for c in cols) for r in rows])


def seed(engine: Engine, n: int, seed_value: int = 7) -> list[dict[str, Any]]:
    rng = random.Random(seed_value)
    rows = [make_row(rng) for _ in range(n)]
    insert_rows(engine, rows)
    return rows


def colliding_legacy_ids() -> tuple[str, str]:
    """Two legacy ids whose backfill_id hash bits match (crc32 equal mod 1000*4096).

    With the same entity and observed_at second they yield the same snowflake id. Found by
    a birthday search, so this is a natural collision, not a mocked one.
    """
    seen: dict[int, str] = {}
    i = 0
    while True:
        candidate = f"collide-{i:08d}"
        residue = zlib.crc32(candidate.encode()) % (1000 * 4096)
        if residue in seen:
            return seen[residue], candidate
        seen[residue] = candidate
        i += 1


def same_bucket_entities() -> tuple[str, str]:
    """Two distinct legacy-shaped entity keys in the same virtual bucket (crc32 % 1024)."""
    seen: dict[int, str] = {}
    i = 0
    while True:
        key = f"entity-{i:08d}"
        bucket = zlib.crc32(key.encode()) % 1024
        if bucket in seen:
            return seen[bucket], key
        seen[bucket] = key
        i += 1
