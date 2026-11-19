from __future__ import annotations

import random
from collections.abc import Callable
from typing import Any

import pytest
from sqlalchemy import Engine

from evidencehub.common.snowflake import backfill_id
from evidencehub.migration.normalize import parse_observed_at

from .legacy_fixtures import (
    colliding_legacy_ids,
    insert_rows,
    load_script,
    make_row,
    same_bucket_entities,
    seed,
)

pytestmark = pytest.mark.integration

bf = load_script("db/optimized/backfill.py")
vf = load_script("db/optimized/verify_backfill.py")
MONTHS = ["2024-01", "2024-02", "2024-03"]


def _scalar(engine: Engine, sql: str, *params: Any) -> Any:
    with engine.connect() as conn:
        return conn.exec_driver_sql(sql, params or None).scalar_one()


def _verify(engine: Engine) -> dict[str, Any]:
    with engine.connect() as conn:
        return vf.verify(conn, vf.MAX_LEGACY_ID, 50, 40, 1)


def _month_checksums(engine: Engine) -> dict[str, Any]:
    with engine.connect() as conn:
        return {r.m: (r.n, int(r.crc)) for r in conn.exec_driver_sql(vf.NEW_MONTH_SQL)}


def test_month_load_twice_and_over_pk_order_overlap(make_schema: Callable[[str], Engine]) -> None:
    engine = make_schema("bf_overlap")
    rows = seed(engine, 600)
    first_250 = sorted(r["id"] for r in rows)[:250]
    feb_overlap = sum(
        1 for r in rows if r["id"] in set(first_250) and r["observed_at"].startswith("2024-02")
    )
    feb_total = sum(1 for r in rows if r["observed_at"].startswith("2024-02"))

    status, s = bf.run_pk_order(engine, "sample", 100, 250, 0.0)
    assert (status, s["inserted"]) == (0, 250)

    status, s = bf.run_month(engine, "2024-02", 50, 0.0)
    assert status == 0
    assert s["skipped_existing"] == feb_overlap and s["inserted"] == feb_total - feb_overlap
    after_first = _month_checksums(engine)

    status, s = bf.run_month(engine, "2024-02", 50, 0.0)
    assert status == 0 and s["skipped_completed"] is True

    with engine.begin() as conn:  # forget the checkpoint: a full re-run must be a no-op
        conn.exec_driver_sql("DELETE FROM backfill_progress WHERE job='month-2024-02'")
    status, s = bf.run_month(engine, "2024-02", 50, 0.0)
    assert (status, s["inserted"], s["skipped_existing"]) == (0, 0, feb_total)
    assert _month_checksums(engine) == after_first

    status, _ = bf.run_months(engine, MONTHS, 50, 0.0)
    assert status == 0
    assert _scalar(engine, "SELECT COUNT(*) FROM evidence") == 600
    assert _scalar(engine, "SELECT COUNT(*) FROM evidence_payload") == 600
    result = _verify(engine)
    assert result["ok"], result
    assert result["months"]["legacy_rows"] == result["months"]["new_rows"] == 600


def test_interrupted_month_resumes_from_checkpoint(
    make_schema: Callable[[str], Engine], monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = make_schema("bf_resume")
    rows = seed(engine, 400, seed_value=11)
    jan = sum(1 for r in rows if r["observed_at"].startswith("2024-01"))
    calls = {"n": 0}
    real = bf._disk_ok

    def disk_fails_on_third(min_free: float) -> tuple[bool, float]:
        calls["n"] += 1
        return (False, 0.0) if calls["n"] == 3 else real(min_free)

    monkeypatch.setattr(bf, "_disk_ok", disk_fails_on_third)
    status, s = bf.run_month(engine, "2024-01", 30, 0.0)
    assert status == 3 and s["inserted"] == 60
    cursor = _scalar(
        engine, "SELECT cursor_observed_at FROM backfill_progress WHERE job='month-2024-01'"
    )
    assert cursor is not None
    monkeypatch.setattr(bf, "_disk_ok", real)

    status, s = bf.run_month(engine, "2024-01", 30, 0.0)
    assert status == 0
    assert s["resumed_from"] is not None
    assert (s["inserted"], s["skipped_existing"]) == (jan - 60, 0)  # resumed, not re-read
    assert _scalar(engine, "SELECT COUNT(*) FROM evidence") == jan


@pytest.mark.parametrize("chunk", [100, 1], ids=["same-batch", "across-batches"])
def test_id_collision_stops_and_names_legacy_ids(
    make_schema: Callable[[str], Engine], chunk: int
) -> None:
    engine = make_schema(f"bf_collide_{chunk}")
    a, b = colliding_legacy_ids()
    rng = random.Random(3)
    twin = {"entity_id": "entity-00000001", "observed_at": "2024-02-10 12:00:00"}
    insert_rows(engine, [make_row(rng, id=a, **twin), make_row(rng, id=b, **twin)])
    observed = parse_observed_at(twin["observed_at"])
    assert backfill_id(observed, twin["entity_id"], a) == backfill_id(
        observed, twin["entity_id"], b
    )

    status, s = bf.run_pk_order(engine, f"collide-{chunk}", chunk, None, 0.0)
    assert status == 4
    message = " ".join(s["collisions"])
    assert b in message and (a in message or chunk == 1)
    # The colliding batch was rolled back; only an earlier, separate batch may have landed.
    assert _scalar(engine, "SELECT COUNT(*) FROM evidence") == (0 if chunk == 100 else 1)
    with engine.connect() as conn:
        last = conn.exec_driver_sql(
            "SELECT last_legacy_id FROM backfill_progress WHERE job=%s", (f"collide-{chunk}",)
        ).scalar()
    assert last == (None if chunk == 100 else a)  # checkpoint never passes the collision


def test_same_id_and_hash_for_another_entity_is_a_collision_not_a_skip(
    make_schema: Callable[[str], Engine],
) -> None:
    """Two different legacy rows, one snowflake id, one content_hash, two entities.

    Before the entity_pk comparison the second row was silently skipped as "already loaded".
    """
    engine = make_schema("bf_twin_entity")
    a, b = colliding_legacy_ids()
    e1, e2 = same_bucket_entities()
    observed = "2024-02-10 12:00:00"
    same = "cd" * 32
    rng = random.Random(9)
    insert_rows(
        engine,
        [
            make_row(rng, id=a, entity_id=e1, observed_at=observed, content_hash=same),
            make_row(rng, id=b, entity_id=e2, observed_at=observed, content_hash=same),
        ],
    )
    ts = parse_observed_at(observed)
    assert backfill_id(ts, e1, a) == backfill_id(ts, e2, b)

    status, s = bf.run_pk_order(engine, "twin-entity", 1, None, 0.0)  # one row per batch
    assert status == 4
    assert b in " ".join(s["collisions"]) and "entity_pk" in " ".join(s["collisions"])
    assert _scalar(engine, "SELECT COUNT(*) FROM evidence") == 1


@pytest.mark.parametrize("order", ["forward", "reverse"])
def test_duplicate_hash_rows_all_loaded_and_earliest_owns_hash(
    make_schema: Callable[[str], Engine], order: str
) -> None:
    engine = make_schema(f"bf_duphash_{order}")
    rng = random.Random(5)
    same = "ab" * 32
    early = make_row(rng, content_hash=same, observed_at="2024-01-05 00:00:00")
    late = make_row(rng, content_hash=same, observed_at="2024-03-05 00:00:00")
    insert_rows(engine, [early, late])
    months = MONTHS if order == "forward" else list(reversed(MONTHS))
    status, s = bf.run_months(engine, months, 50, 0.0)
    assert status == 0
    assert _scalar(engine, "SELECT COUNT(*) FROM evidence WHERE content_hash=UNHEX(%s)", same) == 2
    owner = _scalar(
        engine, "SELECT evidence_id FROM evidence_content_hash WHERE content_hash=UNHEX(%s)", same
    )
    early_id = backfill_id(parse_observed_at(early["observed_at"]), early["entity_id"], early["id"])
    assert owner == early_id  # same owner whichever month is loaded first
    # duplicate_of is the owner at write time: in reverse order the late row was written
    # first and was the owner then, so it is not reported.
    assert sum(m["duplicates_of_earlier_hash"] for m in s) == (1 if order == "forward" else 0)
    assert _verify(engine)["ok"]


def test_verify_month_passes_on_overlap_and_catches_missing_or_altered_rows(
    make_schema: Callable[[str], Engine],
) -> None:
    engine = make_schema("bf_verify_month")
    seed(engine, 500, seed_value=21)
    assert bf.run_pk_order(engine, "sample", 100, 200, 0.0)[0] == 0  # PK-order overlap first
    assert bf.run_month(engine, "2024-02", 40, 0.0)[0] == 0
    good = vf.verify_month(engine, "2024-02", 40, 25, 1)
    assert good["ok"], good
    assert good["legacy_rows"] == good["new_rows"] > 0
    assert good["duplicate_rows_by_entity_observed_hash"] == 0

    with engine.begin() as conn:  # alter one stored value
        conn.exec_driver_sql(
            "UPDATE evidence SET status = IF(status = 1, 2, 1) "
            "WHERE observed_at >= '2024-02-01' AND observed_at < '2024-03-01' ORDER BY id LIMIT 1"
        )
    altered = vf.verify_month(engine, "2024-02", 40, 25, 1)
    assert not altered["ok"] and altered["mismatched_chunks"] and not altered["sql_checksum_equal"]

    with engine.begin() as conn:  # drop one row
        conn.exec_driver_sql(
            "DELETE FROM evidence "
            "WHERE observed_at >= '2024-02-01' AND observed_at < '2024-03-01' ORDER BY id LIMIT 1"
        )
    missing = vf.verify_month(engine, "2024-02", 40, 25, 1)
    assert not missing["ok"] and missing["new_rows"] == missing["legacy_rows"] - 1


def test_pre_cursor_progress_row_rescan_does_not_double_count_rows_written(
    make_schema: Callable[[str], Engine],
) -> None:
    """A progress row from before migration 0003 has rows_written but no cursor/completed_at.

    The month is rescanned from the start (every row skipped); rows_written must end at the
    month's row count, not the stale count plus the rescan.
    """
    engine = make_schema("bf_stale_progress")
    rows = seed(engine, 300, seed_value=31)
    feb = sum(1 for r in rows if r["observed_at"].startswith("2024-02"))
    assert bf.run_month(engine, "2024-02", 40, 0.0)[0] == 0
    with engine.begin() as conn:  # the shape a pre-0003 row has after the migration adds columns
        conn.exec_driver_sql(
            "UPDATE backfill_progress SET cursor_observed_at = NULL, completed_at = NULL, "
            "last_legacy_id = '' WHERE job = 'month-2024-02'"
        )
    status, s = bf.run_month(engine, "2024-02", 40, 0.0)
    assert (status, s["inserted"], s["skipped_existing"]) == (0, 0, feb)
    assert s["rows_written_total_for_month"] == feb
    assert (
        _scalar(engine, "SELECT rows_written FROM backfill_progress WHERE job='month-2024-02'")
        == feb
    )
    assert _scalar(engine, "SELECT COUNT(*) FROM evidence") == feb


def _binlog_bytes(engine: Engine) -> int:
    with engine.connect() as conn:
        return sum(int(r[1]) for r in conn.exec_driver_sql("SHOW BINARY LOGS"))


def test_session_no_binlog_writes_rows_but_no_binlog(
    make_schema: Callable[[str], Engine], mysql_root_url: str
) -> None:
    from sqlalchemy import create_engine

    engine = make_schema("bf_nobinlog")
    seed(engine, 200, seed_value=41)
    quiet = create_engine(engine.url)
    bf.disable_session_binlog(quiet)
    before = _binlog_bytes(engine)
    assert bf.run_month(quiet, "2024-01", 40, 0.0)[0] == 0
    assert _scalar(engine, "SELECT COUNT(*) FROM evidence") > 0
    assert _binlog_bytes(engine) == before  # nothing logged by the quiet sessions
    assert bf.run_month(engine, "2024-02", 40, 0.0)[0] == 0
    assert _binlog_bytes(engine) > before  # control: a normal session does log


def test_session_no_binlog_without_privilege_refuses(
    make_schema: Callable[[str], Engine], mysql_root_url: str
) -> None:
    from sqlalchemy import create_engine

    engine = make_schema("bf_nobinlog_denied")
    admin = create_engine(mysql_root_url)
    with admin.begin() as conn:
        conn.exec_driver_sql("DROP USER IF EXISTS 'appuser'@'%%'")
        conn.exec_driver_sql("CREATE USER 'appuser'@'%%' IDENTIFIED BY 'app-test-pw'")
        conn.exec_driver_sql("GRANT ALL PRIVILEGES ON bf_nobinlog_denied.* TO 'appuser'@'%%'")
    app = create_engine(engine.url.set(username="appuser", password="app-test-pw"))
    with pytest.raises(bf.SessionBinlogError, match="1227"):
        bf.disable_session_binlog(app)
