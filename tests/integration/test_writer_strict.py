"""Fix 5: the shared writer must surface strict-mode errors instead of hiding them."""

from __future__ import annotations

import dataclasses
import random
from collections.abc import Callable

import pytest
from sqlalchemy import Engine
from sqlalchemy.exc import DataError, IntegrityError

from evidencehub.migration.normalize import NormalizedEvidence, normalize_legacy_row
from evidencehub.migration.writer import Dims, write_records

from .legacy_fixtures import make_row

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def engine(make_schema: Callable[[str], Engine]) -> Engine:
    return make_schema("writer_strict")


def _rec(**overrides: object) -> NormalizedEvidence:
    from types import SimpleNamespace

    base = normalize_legacy_row(SimpleNamespace(**make_row(random.Random(9))))
    return dataclasses.replace(base, **overrides)  # type: ignore[arg-type]


def test_too_long_dimension_value_raises(engine: Engine) -> None:
    with engine.connect() as conn, pytest.raises(DataError, match="Data too long"):
        write_records(conn, Dims(), [_rec(source_name="x" * 300)])


def test_null_into_not_null_dimension_raises(engine: Engine) -> None:
    with engine.connect() as conn, pytest.raises(IntegrityError, match="cannot be null"):
        write_records(conn, Dims(), [_rec(source_type=None)])


def test_insert_ignore_would_have_truncated_silently(engine: Engine) -> None:
    """Why IGNORE was removed: same value, IGNORE variant, succeeds with a warning."""
    with engine.connect() as conn:
        conn.exec_driver_sql(
            "INSERT IGNORE INTO dim_source (name, source_type) VALUES (%s, %s)", ("y" * 300, "rest")
        )
        warnings = conn.exec_driver_sql("SHOW WARNINGS").fetchall()
        stored = conn.exec_driver_sql(
            "SELECT CHAR_LENGTH(name) FROM dim_source WHERE name LIKE 'yyy%%'"
        ).scalar_one()
        conn.rollback()
    assert any("Data truncated" in str(w) for w in warnings)
    assert stored == 255
