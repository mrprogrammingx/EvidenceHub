from __future__ import annotations

import hashlib
import json
import zlib
from datetime import datetime
from decimal import Decimal

import pytest
from sqlalchemy import Engine

from evidencehub.common.snowflake import bucket_for, decode_id
from evidencehub.ingestion.dual_writer import DualWriter, EvidenceInput

pytestmark = pytest.mark.integration


def _key_for_shard(shard: int, start: int = 0) -> str:
    i = start
    while True:
        key = f"entity-{i:08d}"
        if zlib.crc32(key.encode()) % 4 == shard:
            return key
        i += 1


def _item(seed: str, **overrides: object) -> EvidenceInput:
    base: dict[str, object] = {
        "entity_key": _key_for_shard(0),
        "entity_name": "Case test",
        "source_name": "kafka-stream-x",
        "source_type": "stream",
        "evidence_type": "photo",
        "status": "verified",
        "observed_at": datetime(2025, 3, 4, 5, 6, 7),
        "content_hash": hashlib.sha256(seed.encode()).hexdigest(),
        "payload": {"tags": ["a"], "geo": {"lat": 1.5, "lon": -2.25}},
        "confidence_score": "0.8750",
        "reviewer_notes": "note",
    }
    base.update(overrides)
    return EvidenceInput(**base)  # type: ignore[arg-type]


def _count(engine: Engine, sql: str, *params: object) -> int:
    with engine.connect() as conn:
        return int(conn.exec_driver_sql(sql, params or None).scalar_one())


def test_writes_both_sides_with_normalized_values(shard_engine: Engine) -> None:
    item = _item("happy")
    result = DualWriter(shard_engine, shard_index=0).write(item)
    assert result.error is None and result.duplicate_of is None and result.evidence_id
    with shard_engine.connect() as conn:
        legacy = conn.exec_driver_sql(
            "SELECT status, observed_at, is_deleted, confidence_score FROM evidence_legacy "
            "WHERE id=%s",
            (result.legacy_id,),
        ).one()
        new = conn.exec_driver_sql(
            "SELECT e.status, e.observed_at, e.is_deleted, e.confidence_score, e.entity_pk, "
            "t.name AS type_name, s.name AS source_name, p.payload, p.reviewer_notes "
            "FROM evidence e JOIN evidence_payload p ON p.id=e.id "
            "JOIN dim_evidence_type t ON t.type_id=e.type_id "
            "JOIN dim_source s ON s.source_id=e.source_id WHERE e.id=%s",
            (result.evidence_id,),
        ).one()
        claimed = conn.exec_driver_sql(
            "SELECT evidence_id FROM evidence_content_hash WHERE content_hash=UNHEX(%s)",
            (item.content_hash,),
        ).scalar_one()
    assert tuple(legacy) == ("verified", "2025-03-04 05:06:07", "false", "0.8750")
    assert new.status == 3
    assert new.observed_at == item.observed_at
    assert new.is_deleted == 0
    assert new.confidence_score == Decimal("0.8750")
    assert new.entity_pk == int(item.entity_key.split("-")[1])
    assert (new.type_name, new.source_name) == ("photo", "kafka-stream-x")
    assert json.loads(new.payload) == item.payload
    assert new.reviewer_notes == "note"
    assert claimed == result.evidence_id
    _, bucket, _ = decode_id(result.evidence_id)
    assert bucket == bucket_for(item.entity_key)


def test_duplicate_hash_keeps_legacy_row_but_skips_optimized(shard_engine: Engine) -> None:
    writer = DualWriter(shard_engine, shard_index=0)
    first = writer.write(_item("dup"))
    second = writer.write(_item("dup", status="pending"))
    assert second.duplicate_of == first.evidence_id and second.evidence_id is None
    content_hash = hashlib.sha256(b"dup").hexdigest()
    assert (
        _count(
            shard_engine, "SELECT COUNT(*) FROM evidence_legacy WHERE content_hash=%s", content_hash
        )
        == 2
    )
    assert (
        _count(
            shard_engine, "SELECT COUNT(*) FROM evidence WHERE content_hash=UNHEX(%s)", content_hash
        )
        == 1
    )


def test_normalization_failure_goes_to_outbox_not_raised(shard_engine: Engine) -> None:
    result = DualWriter(shard_engine, shard_index=0).write(_item("badstatus", status="bogus"))
    assert result.evidence_id is None and result.error and "unexpected status" in result.error
    assert (
        _count(shard_engine, "SELECT COUNT(*) FROM evidence_legacy WHERE id=%s", result.legacy_id)
        == 1
    )
    assert (
        _count(
            shard_engine,
            "SELECT COUNT(*) FROM dualwrite_outbox WHERE legacy_id=%s",
            result.legacy_id,
        )
        == 1
    )


def test_mid_transaction_failure_rolls_back_optimized_side(shard_engine: Engine) -> None:
    item = _item("atomic")
    before = _count(shard_engine, "SELECT COUNT(*) FROM evidence")
    with shard_engine.begin() as conn:
        conn.exec_driver_sql("RENAME TABLE evidence_payload TO evidence_payload_hidden")
    try:
        result = DualWriter(shard_engine, shard_index=0).write(item)
    finally:
        with shard_engine.begin() as conn:
            conn.exec_driver_sql("RENAME TABLE evidence_payload_hidden TO evidence_payload")
    assert result.error is not None
    assert _count(shard_engine, "SELECT COUNT(*) FROM evidence") == before
    assert (
        _count(
            shard_engine,
            "SELECT COUNT(*) FROM evidence_content_hash WHERE content_hash=UNHEX(%s)",
            item.content_hash,
        )
        == 0
    )
    assert (
        _count(shard_engine, "SELECT COUNT(*) FROM evidence_legacy WHERE id=%s", result.legacy_id)
        == 1
    )
    assert (
        _count(
            shard_engine,
            "SELECT COUNT(*) FROM dualwrite_outbox WHERE legacy_id=%s",
            result.legacy_id,
        )
        == 1
    )


def test_rejects_entity_routed_to_another_shard(shard_engine: Engine) -> None:
    with pytest.raises(ValueError, match="belongs to shard"):
        DualWriter(shard_engine, shard_index=0).write(_item("x", entity_key=_key_for_shard(1)))
