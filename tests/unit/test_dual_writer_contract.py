"""Contract-level unit tests for DualWriter using a recording fake connection.

These pin the ordering and branching (legacy commit first, dedup skip, outbox on
failure). Real SQL behaviour is covered by tests/integration/test_dual_writer.py.
"""

from __future__ import annotations

import hashlib
import zlib
from datetime import datetime
from types import TracebackType
from typing import Any

import pytest

from evidencehub.common.snowflake import LiveIdGenerator
from evidencehub.ingestion.dual_writer import DualWriter, EvidenceInput


class _Result:
    def __init__(self, rowcount: int = 1, scalar: Any = None) -> None:
        self.rowcount = rowcount
        self._scalar = scalar

    def scalar_one(self) -> Any:
        return self._scalar


class _FakeConn:
    def __init__(self, claim_rowcount: int = 1, fail_on: str | None = None) -> None:
        self.log: list[str] = []
        self.claim_rowcount = claim_rowcount
        self.fail_on = fail_on

    def exec_driver_sql(self, sql: str, params: Any = None) -> _Result:
        self.log.append(sql.split("(")[0].strip())
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("boom")
        if sql.startswith("INSERT IGNORE INTO evidence_content_hash"):
            return _Result(rowcount=self.claim_rowcount)
        if sql.startswith("SELECT evidence_id"):
            return _Result(scalar=42)
        return _Result()

    def commit(self) -> None:
        self.log.append("COMMIT")

    def rollback(self) -> None:
        self.log.append("ROLLBACK")

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(
        self, et: type[BaseException] | None, e: BaseException | None, tb: TracebackType | None
    ) -> None:
        return None


class _FakeEngine:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def connect(self) -> _FakeConn:
        return self.conn


def _key(shard: int) -> str:
    i = 0
    while zlib.crc32(f"entity-{i:08d}".encode()) % 4 != shard:
        i += 1
    return f"entity-{i:08d}"


def _item(**overrides: Any) -> EvidenceInput:
    base: dict[str, Any] = {
        "entity_key": _key(0),
        "source_name": "s",
        "source_type": "rest",
        "evidence_type": "photo",
        "status": "verified",
        "observed_at": datetime(2025, 1, 1),
        "content_hash": hashlib.sha256(b"x").hexdigest(),
        "payload": {"a": 1},
    }
    base.update(overrides)
    return EvidenceInput(**base)


def _writer(conn: _FakeConn) -> DualWriter:
    return DualWriter(_FakeEngine(conn), shard_index=0, id_gen=LiveIdGenerator())  # type: ignore[arg-type]


def test_legacy_committed_before_optimized_side() -> None:
    conn = _FakeConn()
    result = _writer(conn).write(_item())
    assert result.evidence_id and result.error is None
    assert conn.log[:2] == ["INSERT INTO evidence_legacy", "COMMIT"]
    assert conn.log[-1] == "COMMIT"
    assert "INSERT INTO evidence_payload" in conn.log


def test_duplicate_claim_skips_optimized_inserts() -> None:
    conn = _FakeConn(claim_rowcount=0)
    result = _writer(conn).write(_item())
    assert result.duplicate_of == 42 and result.evidence_id is None
    assert "INSERT INTO evidence" not in conn.log


def test_failure_rolls_back_and_records_outbox() -> None:
    conn = _FakeConn(fail_on="evidence_payload")
    result = _writer(conn).write(_item())
    assert result.error == "RuntimeError: boom"
    assert conn.log[-3:] == ["ROLLBACK", "INSERT INTO dualwrite_outbox", "COMMIT"]


def test_wrong_shard_rejected_before_any_write() -> None:
    conn = _FakeConn()
    with pytest.raises(ValueError, match="belongs to shard"):
        _writer(conn).write(_item(entity_key=_key(2)))
    assert conn.log == []
