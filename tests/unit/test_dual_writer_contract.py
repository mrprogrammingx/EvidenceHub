"""Contract-level unit tests for DualWriter: ordering and the failure-recording chain.

The optimized write (write_records) is replaced with stubs here; real SQL behaviour is
covered by tests/integration/test_dual_writer.py.
"""

from __future__ import annotations

import hashlib
import zlib
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Any

import pytest

from evidencehub.ingestion import dual_writer as dw
from evidencehub.ingestion.dual_writer import DualWriter, EvidenceInput, ReconciliationLostError
from evidencehub.migration.writer import WriteOutcome


class _FakeConn:
    def __init__(self, log: list[str], fail_outbox: bool = False, fail_rollback: bool = False):
        self.log = log
        self.fail_outbox = fail_outbox
        self.fail_rollback = fail_rollback

    def exec_driver_sql(self, sql: str, params: Any = None) -> None:
        self.log.append(sql.split("(")[0].strip())
        if self.fail_outbox and "dualwrite_outbox" in sql:
            raise RuntimeError("outbox down")

    def commit(self) -> None:
        self.log.append("COMMIT")

    def rollback(self) -> None:
        self.log.append("ROLLBACK")
        if self.fail_rollback:
            raise RuntimeError("connection lost")

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(
        self, et: type[BaseException] | None, e: BaseException | None, tb: TracebackType | None
    ) -> None:
        return None


class _FakeEngine:
    def __init__(self, **flags: bool) -> None:
        self.log: list[str] = []
        self.connections = 0
        self.flags = flags

    def connect(self) -> _FakeConn:
        self.connections += 1
        return _FakeConn(self.log, **self.flags)


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


def _ok(conn: Any, dims: Any, recs: Any) -> list[WriteOutcome]:
    conn.exec_driver_sql("WRITE_RECORDS")
    return [
        WriteOutcome(
            id=recs[0].id, legacy_id=recs[0].legacy_id, already_present=False, duplicate_of=7
        )
    ]


def _boom(conn: Any, dims: Any, recs: Any) -> list[WriteOutcome]:
    raise RuntimeError("boom")


def test_legacy_committed_before_optimized_side(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dw, "write_records", _ok)
    engine = _FakeEngine()
    result = DualWriter(engine, shard_index=0).write(_item())  # type: ignore[arg-type]
    assert result.evidence_id is not None and result.duplicate_of == 7 and result.error is None
    assert engine.log == ["INSERT INTO evidence_legacy", "COMMIT", "WRITE_RECORDS", "COMMIT"]


def test_failure_goes_to_outbox_on_a_fresh_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dw, "write_records", _boom)
    engine = _FakeEngine(fail_rollback=True)  # the original connection is dead
    result = DualWriter(engine, shard_index=0).write(_item())  # type: ignore[arg-type]
    assert (result.error, result.recorded_in) == ("RuntimeError: boom", "outbox")
    assert engine.connections == 2
    assert engine.log[-2:] == ["INSERT INTO dualwrite_outbox", "COMMIT"]


def test_outbox_failure_spools(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(dw, "write_records", _boom)
    spool = tmp_path / "nested" / "spool.jsonl"
    writer = DualWriter(_FakeEngine(fail_outbox=True), shard_index=0, spool_path=spool)  # type: ignore[arg-type]
    result = writer.write(_item())
    assert result.recorded_in == "spool"
    assert result.legacy_id in spool.read_text()


def test_outbox_and_spool_failure_raises_with_legacy_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(dw, "write_records", _boom)
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("")
    writer = DualWriter(
        _FakeEngine(fail_outbox=True),  # type: ignore[arg-type]
        shard_index=0,
        spool_path=blocker / "spool.jsonl",
    )
    with pytest.raises(ReconciliationLostError) as info:
        writer.write(_item())
    assert info.value.legacy_id in str(info.value)


def test_wrong_shard_rejected_before_any_write() -> None:
    engine = _FakeEngine()
    with pytest.raises(ValueError, match="belongs to shard"):
        DualWriter(engine, shard_index=0).write(_item(entity_key=_key(2)))  # type: ignore[arg-type]
    assert engine.log == []
