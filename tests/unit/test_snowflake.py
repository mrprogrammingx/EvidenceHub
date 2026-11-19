from __future__ import annotations

import zlib
from datetime import datetime

import pytest

from evidencehub.common import snowflake as sf


def test_roundtrip() -> None:
    ts = sf.EPOCH_MS + 123_456_789
    assert sf.decode_id(sf.make_id(ts, 1023, 4095)) == (ts, 1023, 4095)


def test_bucket_mod_4_matches_crc32_mod_4() -> None:
    for i in range(2000):
        key = f"entity-{i:08d}"
        assert sf.shard_for_bucket(sf.bucket_for(key)) == zlib.crc32(key.encode()) % 4


def test_backfill_id_is_deterministic_and_time_ordered() -> None:
    a = sf.backfill_id(datetime(2023, 1, 1, 0, 0, 1), "entity-00000001", "u-1")
    assert a == sf.backfill_id(datetime(2023, 1, 1, 0, 0, 1), "entity-00000001", "u-1")
    later = sf.backfill_id(datetime(2023, 1, 1, 0, 0, 2), "entity-00000001", "u-1")
    assert later > a


def test_out_of_range_rejected() -> None:
    with pytest.raises(ValueError):
        sf.make_id(sf.EPOCH_MS - 1, 0, 0)
    with pytest.raises(ValueError):
        sf.make_id(sf.EPOCH_MS, 1024, 0)


def test_live_generator_is_monotonic_and_borrows_next_ms() -> None:
    clock = iter([sf.EPOCH_MS + 10] * (sf.SEQ_MAX + 3) + [sf.EPOCH_MS + 5])
    gen = sf.LiveIdGenerator(clock=lambda: next(clock))
    ids = [gen.next_id(7) for _ in range(sf.SEQ_MAX + 4)]
    assert ids == sorted(ids) and len(set(ids)) == len(ids)
    assert sf.decode_id(ids[sf.SEQ_MAX])[0] == sf.EPOCH_MS + 10
    assert sf.decode_id(ids[sf.SEQ_MAX + 1]) == (sf.EPOCH_MS + 11, 7, 0)
    # clock stepped backwards: still increasing, no reuse
    assert ids[-1] > ids[-2]


def test_live_generator_buckets_are_independent() -> None:
    gen = sf.LiveIdGenerator(clock=lambda: sf.EPOCH_MS + 1)
    assert sf.decode_id(gen.next_id(1))[1:] == (1, 0)
    assert sf.decode_id(gen.next_id(2))[1:] == (2, 0)
