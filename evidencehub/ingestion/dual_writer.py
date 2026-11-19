"""P2 test writer for the dual-write contract (the real ingestion path lands in P4).

Contract, per logical evidence row:

1. Write ``evidence_legacy`` first and commit. Legacy stays the source of truth until
   cutover, so a request never fails because of the new schema. If this step fails, the
   exception propagates: nothing was written anywhere.
2. In a second transaction, write the optimized side through
   :func:`evidencehub.migration.writer.write_records`, the same path the backfill uses.
   Every row is written; a repeated ``content_hash`` is reported as ``duplicate_of`` the
   first id for that hash on this shard (ADR 0004).
3. If step 2 fails for any reason, the legacy row is already committed, so it must be
   recorded for reconciliation. In order:
   a. ``dualwrite_outbox`` on a **fresh** connection (the failing one may be dead);
   b. if that fails too, an fsync'd JSON line in a local spool file;
   c. if even that fails, raise :class:`ReconciliationLostError` carrying the legacy id,
      so the caller can never mistake an unrecorded divergence for success.

Dedup is per shard: placement is by entity, so identical content on entities that live
on different shards is not detected until P3's global lookup index (ADR 0004).
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

from sqlalchemy import Connection, Engine

from evidencehub.common.snowflake import LiveIdGenerator, bucket_for, shard_for_bucket
from evidencehub.migration.normalize import LEGACY_TS_FORMAT, normalize_legacy_row
from evidencehub.migration.writer import Dims, write_records

logger = logging.getLogger("evidencehub.ingestion.dual_writer")

DEFAULT_SPOOL = Path("var/dualwrite_spool.jsonl")


class ReconciliationLostError(RuntimeError):
    """Legacy committed, optimized write failed, and neither outbox nor spool recorded it."""

    def __init__(self, legacy_id: str, cause: str) -> None:
        self.legacy_id = legacy_id
        super().__init__(
            f"legacy row {legacy_id} committed but not recorded for reconciliation: {cause}"
        )


@dataclass(frozen=True)
class EvidenceInput:
    entity_key: str
    source_name: str
    source_type: str
    evidence_type: str
    status: str
    observed_at: datetime
    content_hash: str  # hex sha256
    payload: dict[str, Any] | None = None
    confidence_score: str | None = None
    entity_name: str | None = None
    reviewer_notes: str | None = None
    is_deleted: bool = False


@dataclass(frozen=True)
class WriteResult:
    legacy_id: str
    evidence_id: int | None = None
    duplicate_of: int | None = None
    error: str | None = None
    recorded_in: Literal["outbox", "spool"] | None = None


class DualWriter:
    def __init__(
        self,
        engine: Engine,
        shard_index: int,
        shard_count: int = 4,
        id_gen: LiveIdGenerator | None = None,
        spool_path: Path = DEFAULT_SPOOL,
    ) -> None:
        self._engine = engine
        self._shard_index = shard_index
        self._shard_count = shard_count
        self._ids = id_gen or LiveIdGenerator()
        self._dims = Dims()
        self._spool_path = spool_path

    def write(self, item: EvidenceInput) -> WriteResult:
        bucket = bucket_for(item.entity_key)
        owner = shard_for_bucket(bucket, self._shard_count)
        if owner != self._shard_index:
            raise ValueError(f"{item.entity_key} belongs to shard {owner}, not {self._shard_index}")
        legacy_row = self._legacy_row(str(uuid.uuid4()), item)
        failure: str
        with self._engine.connect() as conn:
            self._insert_legacy(conn, legacy_row)
            conn.commit()
            try:
                result = self._write_optimized(conn, legacy_row, bucket)
                conn.commit()
                self._dims.commit_pending()
                return result
            except Exception as exc:
                self._dims.discard_pending()
                failure = f"{type(exc).__name__}: {exc}"[:1024]
                try:
                    conn.rollback()
                except Exception:
                    logger.warning("rollback after optimized-side failure also failed")
        return self._record_failure(legacy_row.id, failure)

    def _record_failure(self, legacy_id: str, failure: str) -> WriteResult:
        logger.warning(
            "dual-write optimized side failed: %s", failure, extra={"legacy_id": legacy_id}
        )
        try:
            with self._engine.connect() as fresh:
                fresh.exec_driver_sql(
                    "INSERT INTO dualwrite_outbox (legacy_id, error) VALUES (%s, %s)",
                    (legacy_id, failure),
                )
                fresh.commit()
            return WriteResult(legacy_id=legacy_id, error=failure, recorded_in="outbox")
        except Exception as outbox_exc:
            outbox_error = f"{type(outbox_exc).__name__}: {outbox_exc}"
            logger.error("outbox insert failed, spooling: %s", outbox_error)
        try:
            self._spool_path.parent.mkdir(parents=True, exist_ok=True)
            line = json.dumps(
                {
                    "legacy_id": legacy_id,
                    "error": failure,
                    "outbox_error": outbox_error,
                    "at": datetime.now(UTC).isoformat(),
                }
            )
            with self._spool_path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            return WriteResult(legacy_id=legacy_id, error=failure, recorded_in="spool")
        except OSError as spool_exc:
            raise ReconciliationLostError(
                legacy_id, f"{failure}; outbox: {outbox_error}; spool: {spool_exc}"
            ) from spool_exc

    @staticmethod
    def _legacy_row(legacy_id: str, item: EvidenceInput) -> SimpleNamespace:
        return SimpleNamespace(
            id=legacy_id,
            entity_id=item.entity_key,
            entity_name=item.entity_name,
            source_name=item.source_name,
            source_type=item.source_type,
            evidence_type=item.evidence_type,
            status=item.status,
            confidence_score=item.confidence_score,
            observed_at=item.observed_at.strftime(LEGACY_TS_FORMAT),
            content_hash=item.content_hash,
            payload_blob=None if item.payload is None else json.dumps(item.payload),
            reviewer_notes=item.reviewer_notes,
            is_deleted="true" if item.is_deleted else "false",
        )

    @staticmethod
    def _insert_legacy(conn: Connection, r: SimpleNamespace) -> None:
        conn.exec_driver_sql(
            "INSERT INTO evidence_legacy (id, entity_id, entity_name, source_name, source_type, "
            "evidence_type, status, confidence_score, observed_at, content_hash, payload_blob, "
            "reviewer_notes, is_deleted) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                r.id,
                r.entity_id,
                r.entity_name,
                r.source_name,
                r.source_type,
                r.evidence_type,
                r.status,
                r.confidence_score,
                r.observed_at,
                r.content_hash,
                r.payload_blob,
                r.reviewer_notes,
                r.is_deleted,
            ),
        )

    def _write_optimized(
        self, conn: Connection, legacy: SimpleNamespace, bucket: int
    ) -> WriteResult:
        norm = dataclasses.replace(normalize_legacy_row(legacy), id=self._ids.next_id(bucket))
        (outcome,) = write_records(conn, self._dims, [norm])
        return WriteResult(
            legacy_id=legacy.id, evidence_id=outcome.id, duplicate_of=outcome.duplicate_of
        )
