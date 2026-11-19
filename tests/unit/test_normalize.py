from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from evidencehub.common.snowflake import backfill_id
from evidencehub.migration import normalize as n


def _legacy(**overrides: object) -> SimpleNamespace:
    row = {
        "id": "000000f3-26a2-4e47-b06b-1b837e73e533",
        "entity_id": "entity-00005803",
        "entity_name": "Case 00005803",
        "source_name": "kafka-stream-x",
        "source_type": "stream",
        "evidence_type": "photo",
        "status": "under_review",
        "confidence_score": "0.1000",
        "observed_at": "2023-01-19 05:34:05",
        "content_hash": "ab" * 32,
        "payload_blob": '{"tags": []}',
        "reviewer_notes": None,
        "is_deleted": "true",
    }
    row.update(overrides)
    return SimpleNamespace(**row)


def test_normalize_legacy_row() -> None:
    rec = n.normalize_legacy_row(_legacy())
    assert rec.entity_pk == 5803
    assert rec.status == 2
    assert rec.confidence_score == Decimal("0.1000")
    assert rec.is_deleted == 1
    assert rec.observed_at == datetime(2023, 1, 19, 5, 34, 5)
    assert rec.content_hash == bytes.fromhex("ab" * 32)
    assert rec.id == backfill_id(rec.observed_at, "entity-00005803", _legacy().id)
    assert rec.created_at is None


def test_status_names_roundtrip() -> None:
    assert {n.STATUS_NAMES[c] for c in n.STATUS_CODES.values()} == set(n.STATUS_CODES)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("entity_id", "case-1", "entity_id format"),
        ("status", "bogus", "unexpected status"),
        ("is_deleted", "yes", "is_deleted"),
        ("confidence_score", "12.5", "DECIMAL"),
    ],
)
def test_rejects_unexpected_values(field: str, value: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        n.normalize_legacy_row(_legacy(**{field: value}))


def test_null_confidence_allowed() -> None:
    assert n.parse_confidence(None) is None
