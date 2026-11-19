"""Unit tests for the shared optimized-side writer, with a scripted fake connection."""

from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Any

import pytest

from evidencehub.migration.normalize import NormalizedEvidence
from evidencehub.migration.writer import Dims, IdCollisionError, write_records


def _rec(id_: int, legacy: str, h: bytes = b"\x01" * 32, **kw: Any) -> NormalizedEvidence:
    base = NormalizedEvidence(
        legacy_id=legacy,
        id=id_,
        observed_at=datetime(2024, 1, 1),
        entity_pk=1,
        entity_key="entity-00000001",
        entity_name=None,
        source_name="s",
        source_type="rest",
        evidence_type="photo",
        status=3,
        confidence_score=None,
        is_deleted=0,
        content_hash=h,
        payload=None,
        reviewer_notes=None,
        created_at=None,
        updated_at=None,
    )
    return dataclasses.replace(base, **kw)


class _Conn:
    """Returns scripted rows for the SELECTs write_records issues; records every statement."""

    def __init__(self, existing: list[tuple[int, bytes, int]], owners: list[tuple[bytes, int]]):
        self.existing, self.owners = existing, owners
        self.sql: list[str] = []

    def exec_driver_sql(self, sql: str, params: Any = None) -> Any:
        self.sql.append(sql)
        if sql.startswith("SELECT id, content_hash, entity_pk FROM evidence"):
            return self.existing
        if sql.startswith("SELECT content_hash, evidence_id"):
            return self.owners
        if sql.startswith("SELECT source_id"):
            return [(5, "s", "rest")]
        if sql.startswith("SELECT type_id"):
            return [(6, "photo")]
        assert "IGNORE" not in sql, sql
        return None


def test_inserts_new_rows_and_reports_duplicate_hash_owner() -> None:
    h = b"\x02" * 32
    conn = _Conn(existing=[], owners=[(h, 10)])
    out = write_records(conn, Dims(), [_rec(10, "a", h), _rec(11, "b", h)])
    assert [(o.id, o.already_present, o.duplicate_of) for o in out] == [
        (10, False, None),
        (11, False, 10),
    ]
    assert any(s.startswith("INSERT INTO evidence (") for s in conn.sql)


def test_existing_same_hash_is_skipped_not_reinserted() -> None:
    h = b"\x03" * 32
    conn = _Conn(existing=[(10, h, 1)], owners=[(h, 10)])
    (out,) = write_records(conn, Dims(), [_rec(10, "a", h)])
    assert out.already_present and out.duplicate_of is None
    assert not any(s.startswith("INSERT") for s in conn.sql)


def test_existing_different_hash_is_a_collision() -> None:
    conn = _Conn(existing=[(10, b"\x09" * 32, 1)], owners=[])
    with pytest.raises(IdCollisionError, match="legacy a already holds"):
        write_records(conn, Dims(), [_rec(10, "a")])


def test_existing_same_hash_other_entity_is_a_collision_not_a_skip() -> None:
    h = b"\x05" * 32
    conn = _Conn(existing=[(10, h, 2)], owners=[(h, 10)])
    with pytest.raises(IdCollisionError, match="entity_pk 2, not 1"):
        write_records(conn, Dims(), [_rec(10, "a", h)])
    assert not any(s.startswith("INSERT") for s in conn.sql)


def test_hash_owner_upsert_keeps_lowest_id() -> None:
    conn = _Conn(existing=[], owners=[(b"\x01" * 32, 10)])
    write_records(conn, Dims(), [_rec(10, "a")])
    (upsert,) = [s for s in conn.sql if s.startswith("INSERT INTO evidence_content_hash")]
    assert "LEAST(evidence_id, VALUES(evidence_id))" in upsert


def test_in_batch_duplicate_id_names_both_legacy_ids() -> None:
    with pytest.raises(IdCollisionError) as info:
        write_records(_Conn([], []), Dims(), [_rec(10, "a"), _rec(10, "b", b"\x04" * 32)])
    assert "legacy a and b" in str(info.value) and info.value.details


def test_dims_pending_only_promoted_on_commit() -> None:
    dims = Dims()
    write_records(_Conn([], [(b"\x01" * 32, 10)]), dims, [_rec(10, "a")])
    assert dims.sources == {} and dims.source_id("s", "rest") == 5
    dims.discard_pending()
    with pytest.raises(KeyError):
        dims.source_id("s", "rest")
    write_records(_Conn([], [(b"\x01" * 32, 11)]), dims, [_rec(11, "b")])
    dims.commit_pending()
    assert dims.sources == {("s", "rest"): 5} and dims.types == {"photo": 6}
    assert dims.entities == {1}


def test_empty_batch_is_a_no_op() -> None:
    assert write_records(_Conn([], []), Dims(), []) == []
