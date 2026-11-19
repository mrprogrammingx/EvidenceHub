from __future__ import annotations

import hashlib
import json
import zlib
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.exc import DataError

from evidencehub.common.snowflake import bucket_for, decode_id
from evidencehub.ingestion import dual_writer as dw
from evidencehub.ingestion.dual_writer import DualWriter, EvidenceInput
from evidencehub.migration.writer import write_records as real_write_records

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def shard_engine(make_schema: Callable[[str], Engine]) -> Engine:
    return make_schema("dualwrite")


def _key_for_shard(shard: int, start: int = 0) -> str:
    i = start
    while zlib.crc32(f"entity-{i:08d}".encode()) % 4 != shard:
        i += 1
    return f"entity-{i:08d}"


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


def _rename(engine: Engine, src: str, dst: str) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql(f"RENAME TABLE {src} TO {dst}")


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
    assert tuple(legacy) == ("verified", "2025-03-04 05:06:07", "false", "0.8750")
    assert (new.status, new.observed_at, new.is_deleted) == (3, item.observed_at, 0)
    assert new.confidence_score == Decimal("0.8750")
    assert new.entity_pk == int(item.entity_key.split("-")[1])
    assert (new.type_name, new.source_name) == ("photo", "kafka-stream-x")
    assert json.loads(new.payload) == item.payload
    assert new.reviewer_notes == "note"
    assert decode_id(result.evidence_id)[1] == bucket_for(item.entity_key)


def test_duplicate_hash_writes_every_row_and_points_at_first(shard_engine: Engine) -> None:
    writer = DualWriter(shard_engine, shard_index=0)
    first = writer.write(_item("dup"))
    second = writer.write(_item("dup", status="pending"))
    assert first.duplicate_of is None and first.evidence_id is not None
    assert second.evidence_id is not None and second.evidence_id != first.evidence_id
    assert second.duplicate_of == first.evidence_id
    content_hash = hashlib.sha256(b"dup").hexdigest()
    legacy_n = _count(
        shard_engine, "SELECT COUNT(*) FROM evidence_legacy WHERE content_hash=%s", content_hash
    )
    new_n = _count(
        shard_engine, "SELECT COUNT(*) FROM evidence WHERE content_hash=UNHEX(%s)", content_hash
    )
    owner = _count(
        shard_engine,
        "SELECT evidence_id FROM evidence_content_hash WHERE content_hash=UNHEX(%s)",
        content_hash,
    )
    assert (legacy_n, new_n, owner) == (2, 2, first.evidence_id)


def test_normalization_failure_goes_to_outbox_not_raised(shard_engine: Engine) -> None:
    result = DualWriter(shard_engine, shard_index=0).write(_item("badstatus", status="bogus"))
    assert result.evidence_id is None and result.error and "unexpected status" in result.error
    assert result.recorded_in == "outbox"
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


def test_mid_transaction_failure_rolls_back_and_does_not_poison_dim_cache(
    shard_engine: Engine,
) -> None:
    writer = DualWriter(shard_engine, shard_index=0)
    item = _item("atomic", source_name="brand-new-source")
    before = _count(shard_engine, "SELECT COUNT(*) FROM evidence")
    _rename(shard_engine, "evidence_payload", "evidence_payload_hidden")
    try:
        result = writer.write(item)
    finally:
        _rename(shard_engine, "evidence_payload_hidden", "evidence_payload")
    assert result.error is not None and result.recorded_in == "outbox"
    assert _count(shard_engine, "SELECT COUNT(*) FROM evidence") == before
    assert (
        _count(shard_engine, "SELECT COUNT(*) FROM dim_source WHERE name='brand-new-source'") == 0
    )  # the dimension insert was rolled back with the rest
    # The same writer must not reuse the rolled-back source_id from its cache.
    retry = writer.write(_item("atomic-retry", source_name="brand-new-source"))
    assert retry.error is None
    assert (
        _count(
            shard_engine,
            "SELECT COUNT(*) FROM evidence e JOIN dim_source s ON s.source_id=e.source_id "
            "WHERE e.id=%s AND s.name='brand-new-source'",
            retry.evidence_id,
        )
        == 1
    )


def test_legacy_failure_propagates_and_writes_nothing(shard_engine: Engine) -> None:
    long_name = "x" * 300  # legacy VARCHAR(255) in strict mode rejects it
    legacy_before = _count(shard_engine, "SELECT COUNT(*) FROM evidence_legacy")
    outbox_before = _count(shard_engine, "SELECT COUNT(*) FROM dualwrite_outbox")
    with pytest.raises(DataError, match="Data too long"):
        DualWriter(shard_engine, shard_index=0).write(_item("toolong", source_name=long_name))
    assert _count(shard_engine, "SELECT COUNT(*) FROM evidence_legacy") == legacy_before
    assert _count(shard_engine, "SELECT COUNT(*) FROM dualwrite_outbox") == outbox_before


def _killing_write_records(admin_url: str) -> Callable[..., Any]:
    def wrapper(conn: Connection, dims: Any, recs: Any) -> Any:
        thread_id = conn.exec_driver_sql("SELECT CONNECTION_ID()").scalar_one()
        admin = create_engine(admin_url)
        with admin.connect() as a:
            a.exec_driver_sql(f"KILL {int(thread_id)}")
        admin.dispose()
        return real_write_records(conn, dims, recs)

    return wrapper


def test_killed_connection_is_recorded_in_outbox_via_fresh_connection(
    shard_engine: Engine, mysql_root_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(dw, "write_records", _killing_write_records(mysql_root_url))
    result = DualWriter(shard_engine, shard_index=0).write(_item("killed"))
    assert result.error is not None and result.recorded_in == "outbox"
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


def test_killed_connection_and_failing_outbox_spools_to_disk(
    shard_engine: Engine, mysql_root_url: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spool = tmp_path / "spool.jsonl"
    monkeypatch.setattr(dw, "write_records", _killing_write_records(mysql_root_url))
    _rename(shard_engine, "dualwrite_outbox", "dualwrite_outbox_hidden")
    try:
        result = DualWriter(shard_engine, shard_index=0, spool_path=spool).write(_item("spooled"))
    finally:
        _rename(shard_engine, "dualwrite_outbox_hidden", "dualwrite_outbox")
    assert result.recorded_in == "spool"
    lines = [json.loads(line) for line in spool.read_text().splitlines()]
    assert [line["legacy_id"] for line in lines] == [result.legacy_id]
    assert "dualwrite_outbox" in lines[0]["outbox_error"]
    assert (
        _count(shard_engine, "SELECT COUNT(*) FROM evidence_legacy WHERE id=%s", result.legacy_id)
        == 1
    )


def test_rejects_entity_routed_to_another_shard(shard_engine: Engine) -> None:
    with pytest.raises(ValueError, match="belongs to shard"):
        DualWriter(shard_engine, shard_index=0).write(_item("x", entity_key=_key_for_shard(1)))
