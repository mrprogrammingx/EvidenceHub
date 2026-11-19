"""The single optimized-side write path, shared by the backfill and the dual-writer.

Rules (ADR 0004):

* **Every legacy row gets an optimized row.** A duplicate ``content_hash`` never drops a
  row. ``evidence_content_hash`` points at the lowest id holding that hash, independent
  of load order; other rows report it as ``duplicate_of``.
* **Re-runs are safe.** A record whose ``(id, observed_at)`` already exists with the same
  ``content_hash`` and ``entity_pk`` is the same legacy row loaded before; it is skipped.
* **Id collisions stop the write.** Two different rows with one id (same hash-derived
  bits, see ADR 0004) raise :class:`IdCollisionError` naming the legacy ids. Nothing from
  the batch is written (the caller rolls back). Quarantining instead would leave a legacy
  row with no optimized counterpart.
* **No INSERT IGNORE.** IGNORE downgrades strict-mode errors (truncation, NOT NULL) to
  warnings. Inserts that may legitimately hit an existing key use a no-op
  ``ON DUPLICATE KEY UPDATE`` instead, so every other error still raises.

The caller owns the transaction. Call :meth:`Dims.commit_pending` after a successful
commit and :meth:`Dims.discard_pending` after a rollback, so the dimension cache never
holds ids that a rolled-back transaction created.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from evidencehub.migration.normalize import NormalizedEvidence


class _Conn(Protocol):
    def exec_driver_sql(self, statement: str, parameters: Any = ..., /) -> Any: ...


class IdCollisionError(RuntimeError):
    def __init__(self, details: list[str]) -> None:
        self.details = details
        super().__init__("snowflake id collision: " + "; ".join(details))


@dataclass(frozen=True)
class WriteOutcome:
    id: int
    legacy_id: str
    already_present: bool
    duplicate_of: int | None


class Dims:
    """Dimension id cache with commit/rollback-aware pending entries."""

    def __init__(self) -> None:
        self.sources: dict[tuple[str, str], int] = {}
        self.types: dict[str, int] = {}
        self.entities: set[int] = set()
        self._p_sources: dict[tuple[str, str], int] = {}
        self._p_types: dict[str, int] = {}
        self._p_entities: set[int] = set()

    def source_id(self, name: str, source_type: str) -> int:
        key = (name, source_type)
        return self._p_sources.get(key) or self.sources[key]

    def type_id(self, name: str) -> int:
        return self._p_types.get(name) or self.types[name]

    def resolve(self, conn: _Conn, recs: Sequence[NormalizedEvidence]) -> None:
        known_src = self.sources.keys() | self._p_sources.keys()
        new_src = sorted({(r.source_name, r.source_type) for r in recs} - known_src)
        if new_src:
            conn.exec_driver_sql(
                "INSERT INTO dim_source (name, source_type) VALUES (%s, %s) "
                "ON DUPLICATE KEY UPDATE name = name",
                new_src,
            )
            for sid, name, stype in conn.exec_driver_sql(
                "SELECT source_id, name, source_type FROM dim_source"
            ):
                if (name, stype) not in self.sources:
                    self._p_sources[(name, stype)] = sid
        known_types = self.types.keys() | self._p_types.keys()
        new_types = sorted({r.evidence_type for r in recs} - known_types)
        if new_types:
            conn.exec_driver_sql(
                "INSERT INTO dim_evidence_type (name) VALUES (%s) "
                "ON DUPLICATE KEY UPDATE name = name",
                [(t,) for t in new_types],
            )
            for tid, name in conn.exec_driver_sql("SELECT type_id, name FROM dim_evidence_type"):
                if name not in self.types:
                    self._p_types[name] = tid
        new_ents: dict[int, tuple[int, str, str | None]] = {}
        for r in recs:
            if r.entity_pk in self.entities or r.entity_pk in self._p_entities:
                continue
            new_ents.setdefault(r.entity_pk, (r.entity_pk, r.entity_key, r.entity_name))
        if new_ents:
            conn.exec_driver_sql(
                "INSERT INTO dim_entity (entity_pk, entity_key, name) VALUES (%s, %s, %s) "
                "ON DUPLICATE KEY UPDATE entity_pk = entity_pk",
                list(new_ents.values()),
            )
            self._p_entities.update(new_ents)

    def commit_pending(self) -> None:
        self.sources.update(self._p_sources)
        self.types.update(self._p_types)
        self.entities.update(self._p_entities)
        self.discard_pending()

    def discard_pending(self) -> None:
        self._p_sources.clear()
        self._p_types.clear()
        self._p_entities.clear()


def _row_placeholders(n: int) -> str:
    return ",".join(["(%s,%s)"] * n)


def write_records(
    conn: _Conn, dims: Dims, recs: Sequence[NormalizedEvidence]
) -> list[WriteOutcome]:
    if not recs:
        return []
    by_id: dict[int, NormalizedEvidence] = {}
    collisions: list[str] = []
    for r in recs:
        other = by_id.get(r.id)
        if other is not None:
            collisions.append(f"id {r.id} for legacy {other.legacy_id} and {r.legacy_id}")
        by_id[r.id] = r
    if collisions:
        raise IdCollisionError(collisions)

    # The optimized schema does not store legacy_id, so "same legacy row loaded before"
    # is judged on the stored identity: (id, observed_at) via the IN, plus content_hash and
    # entity_pk. A twin that differs in either is a collision. A twin identical in all of
    # them is indistinguishable here; verify_backfill's per-month COUNT catches it (ADR 0004).
    existing: dict[int, tuple[bytes, int]] = {
        int(eid): (bytes(h), int(epk))
        for eid, h, epk in conn.exec_driver_sql(
            "SELECT id, content_hash, entity_pk FROM evidence WHERE (id, observed_at) IN "
            f"({_row_placeholders(len(recs))})",
            tuple(v for r in recs for v in (r.id, r.observed_at)),
        )
    }
    for r in recs:
        if r.id not in existing:
            continue
        old_hash, old_entity = existing[r.id]
        if old_hash != r.content_hash:
            collisions.append(
                f"id {r.id} for legacy {r.legacy_id} already holds a row with a different hash"
            )
        elif old_entity != r.entity_pk:
            collisions.append(
                f"id {r.id} for legacy {r.legacy_id} already holds a row for entity_pk "
                f"{old_entity}, not {r.entity_pk}"
            )
    if collisions:
        raise IdCollisionError(collisions)

    todo = sorted((r for r in recs if r.id not in existing), key=lambda r: (r.observed_at, r.id))
    if todo:
        dims.resolve(conn, todo)
        conn.exec_driver_sql(
            "INSERT INTO evidence (id, observed_at, entity_pk, source_id, type_id, status, "
            "confidence_score, is_deleted, content_hash, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,COALESCE(%s, NOW()),COALESCE(%s, NOW()))",
            [
                (
                    r.id,
                    r.observed_at,
                    r.entity_pk,
                    dims.source_id(r.source_name, r.source_type),
                    dims.type_id(r.evidence_type),
                    r.status,
                    r.confidence_score,
                    r.is_deleted,
                    r.content_hash,
                    r.created_at,
                    r.updated_at,
                )
                for r in todo
            ],
        )
        conn.exec_driver_sql(
            "INSERT INTO evidence_payload (id, observed_at, payload, reviewer_notes) "
            "VALUES (%s,%s,%s,%s)",
            [(r.id, r.observed_at, r.payload, r.reviewer_notes) for r in todo],
        )
        # Lowest id owns the hash, whatever order rows arrive in. For backfilled ids the
        # time bits are observed_at, so this is "earliest observed_at, ties by id" (ADR 0004).
        # VALUES() (not a row alias) keeps pymysql's multi-row executemany rewrite working.
        conn.exec_driver_sql(
            "INSERT INTO evidence_content_hash (content_hash, evidence_id) VALUES (%s,%s) "
            "ON DUPLICATE KEY UPDATE evidence_id = LEAST(evidence_id, VALUES(evidence_id))",
            [(r.content_hash, r.id) for r in todo],
        )

    hashes = sorted({r.content_hash for r in recs})
    owner: dict[bytes, int] = {
        bytes(h): int(eid)
        for h, eid in conn.exec_driver_sql(
            "SELECT content_hash, evidence_id FROM evidence_content_hash WHERE content_hash IN "
            f"({','.join(['%s'] * len(hashes))})",
            tuple(hashes),
        )
    }
    return [
        WriteOutcome(
            id=r.id,
            legacy_id=r.legacy_id,
            already_present=r.id in existing,
            duplicate_of=None if owner[r.content_hash] == r.id else owner[r.content_hash],
        )
        for r in recs
    ]
