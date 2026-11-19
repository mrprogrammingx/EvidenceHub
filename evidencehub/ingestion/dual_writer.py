"""P2 test writer for the dual-write contract (the real ingestion path lands in P4).

Contract, per logical evidence row:

1. Write ``evidence_legacy`` first and commit. Legacy stays the source of truth until
   cutover, so a request never fails because of the new schema.
2. In a second transaction, write the optimized side atomically: dedup claim in
   ``evidence_content_hash``, dimensions, ``evidence``, ``evidence_payload``.
   * If the content hash is already claimed on this shard, the optimized write is
     skipped and the existing id is returned (legacy keeps its row, as it always has).
3. If step 2 fails for any reason, roll it back, record the legacy id in
   ``dualwrite_outbox``, and return the error instead of raising.

Dedup is per shard: placement is by entity, so identical content on entities that live
on different shards is not detected until P3's global lookup index (ADR 0004).
"""

from __future__ import annotations

import dataclasses
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from sqlalchemy import Connection, Engine

from evidencehub.common.snowflake import LiveIdGenerator, bucket_for, shard_for_bucket
from evidencehub.migration.normalize import LEGACY_TS_FORMAT, normalize_legacy_row

logger = logging.getLogger("evidencehub.ingestion.dual_writer")


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


class DualWriter:
    def __init__(
        self,
        engine: Engine,
        shard_index: int,
        shard_count: int = 4,
        id_gen: LiveIdGenerator | None = None,
    ) -> None:
        self._engine = engine
        self._shard_index = shard_index
        self._shard_count = shard_count
        self._ids = id_gen or LiveIdGenerator()

    def write(self, item: EvidenceInput) -> WriteResult:
        bucket = bucket_for(item.entity_key)
        if shard_for_bucket(bucket, self._shard_count) != self._shard_index:
            raise ValueError(
                f"{item.entity_key} belongs to shard {shard_for_bucket(bucket, self._shard_count)}"
                f", not {self._shard_index}"
            )
        legacy_id = str(uuid.uuid4())
        legacy_row = self._legacy_row(legacy_id, item)
        with self._engine.connect() as conn:
            self._insert_legacy(conn, legacy_row)
            conn.commit()
            try:
                result = self._write_optimized(conn, legacy_row, bucket)
                conn.commit()
                return result
            except Exception as exc:
                conn.rollback()
                message = f"{type(exc).__name__}: {exc}"[:1024]
                logger.warning("dual-write optimized side failed", extra={"legacy_id": legacy_id})
                conn.exec_driver_sql(
                    "INSERT INTO dualwrite_outbox (legacy_id, error) VALUES (%s, %s)",
                    (legacy_id, message),
                )
                conn.commit()
                return WriteResult(legacy_id=legacy_id, error=message)

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
        claimed = conn.exec_driver_sql(
            "INSERT IGNORE INTO evidence_content_hash (content_hash, evidence_id) VALUES (%s, %s)",
            (norm.content_hash, norm.id),
        ).rowcount
        if claimed == 0:
            existing = conn.exec_driver_sql(
                "SELECT evidence_id FROM evidence_content_hash WHERE content_hash = %s",
                (norm.content_hash,),
            ).scalar_one()
            return WriteResult(legacy_id=legacy.id, duplicate_of=int(existing))
        conn.exec_driver_sql(
            "INSERT IGNORE INTO dim_entity (entity_pk, entity_key, name) VALUES (%s, %s, %s)",
            (norm.entity_pk, norm.entity_key, norm.entity_name),
        )
        conn.exec_driver_sql(
            "INSERT IGNORE INTO dim_source (name, source_type) VALUES (%s, %s)",
            (norm.source_name, norm.source_type),
        )
        conn.exec_driver_sql(
            "INSERT IGNORE INTO dim_evidence_type (name) VALUES (%s)", (norm.evidence_type,)
        )
        inserted = conn.exec_driver_sql(
            "INSERT INTO evidence (id, observed_at, entity_pk, source_id, type_id, status, "
            "confidence_score, is_deleted, content_hash) "
            "SELECT %s, %s, %s, s.source_id, t.type_id, %s, %s, %s, %s "
            "FROM dim_source s JOIN dim_evidence_type t ON t.name = %s "
            "WHERE s.name = %s AND s.source_type = %s",
            (
                norm.id,
                norm.observed_at,
                norm.entity_pk,
                norm.status,
                norm.confidence_score,
                norm.is_deleted,
                norm.content_hash,
                norm.evidence_type,
                norm.source_name,
                norm.source_type,
            ),
        ).rowcount
        if inserted != 1:
            raise RuntimeError(
                f"evidence insert affected {inserted} rows (dimension lookup failed)"
            )
        conn.exec_driver_sql(
            "INSERT INTO evidence_payload (id, observed_at, payload, reviewer_notes) "
            "VALUES (%s, %s, %s, %s)",
            (norm.id, norm.observed_at, norm.payload, norm.reviewer_notes),
        )
        return WriteResult(legacy_id=legacy.id, evidence_id=norm.id)
