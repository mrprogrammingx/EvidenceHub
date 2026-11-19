"""Legacy -> optimized value normalization, shared by the backfill and the dual-writer.

Pure functions only (no DB access), so they are unit-testable and both write paths
are guaranteed to normalize identically.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from evidencehub.common.snowflake import backfill_id

STATUS_CODES: dict[str, int] = {"pending": 1, "under_review": 2, "verified": 3, "rejected": 4}
STATUS_NAMES: dict[int, str] = {code: name for name, code in STATUS_CODES.items()}
LEGACY_TS_FORMAT = "%Y-%m-%d %H:%M:%S"
_ENTITY_RE = re.compile(r"^entity-(\d{8})$")


def entity_pk_for(entity_key: str) -> int:
    """`entity-00086197` -> 86197. Fails loudly on any other shape."""
    match = _ENTITY_RE.match(entity_key)
    if not match:
        raise ValueError(f"unexpected entity_id format: {entity_key!r}")
    return int(match.group(1))


def parse_observed_at(value: str) -> datetime:
    return datetime.strptime(value, LEGACY_TS_FORMAT)


def parse_is_deleted(value: str) -> int:
    if value not in ("true", "false"):
        raise ValueError(f"unexpected is_deleted value: {value!r}")
    return 1 if value == "true" else 0


def status_code(value: str) -> int:
    try:
        return STATUS_CODES[value]
    except KeyError:
        raise ValueError(f"unexpected status: {value!r}") from None


def parse_confidence(value: str | None) -> Decimal | None:
    if value is None:
        return None
    score = Decimal(value)
    if not Decimal("0") <= score < Decimal("10"):
        raise ValueError(f"confidence_score out of DECIMAL(5,4) range: {value!r}")
    return score.quantize(Decimal("0.0001"))


@dataclass(frozen=True)
class NormalizedEvidence:
    """One logical evidence row, with dimension *names* still unresolved to ids."""

    legacy_id: str
    id: int
    observed_at: datetime
    entity_pk: int
    entity_key: str
    entity_name: str | None
    source_name: str
    source_type: str
    evidence_type: str
    status: int
    confidence_score: Decimal | None
    is_deleted: int
    content_hash: bytes
    payload: str | None
    reviewer_notes: str | None
    created_at: datetime | None
    updated_at: datetime | None


def normalize_legacy_row(row: Any) -> NormalizedEvidence:
    """Normalize a row shaped like `evidence_legacy` (attribute access)."""
    observed = parse_observed_at(row.observed_at)
    return NormalizedEvidence(
        legacy_id=row.id,
        id=backfill_id(observed, row.entity_id, row.id),
        observed_at=observed,
        entity_pk=entity_pk_for(row.entity_id),
        entity_key=row.entity_id,
        entity_name=row.entity_name,
        source_name=row.source_name,
        source_type=row.source_type,
        evidence_type=row.evidence_type,
        status=status_code(row.status),
        confidence_score=parse_confidence(row.confidence_score),
        is_deleted=parse_is_deleted(row.is_deleted),
        content_hash=bytes.fromhex(row.content_hash),
        payload=row.payload_blob,
        reviewer_notes=row.reviewer_notes,
        created_at=getattr(row, "created_at", None),
        updated_at=getattr(row, "updated_at", None),
    )
