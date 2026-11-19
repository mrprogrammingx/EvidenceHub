"""Snowflake-style 64-bit ids: 41-bit time | 10-bit virtual bucket | 12-bit sequence.

The middle 10 bits are the *virtual bucket* (``crc32(entity_id) % 1024``), not the
physical shard. The initial bucket -> shard map is ``bucket % shard_count``. Because
1024 is divisible by 4, ``crc32 % 1024 % 4 == crc32 % 4``, which is the placement
the legacy seeder already used, so P3 resharding never has to re-issue ids.
"""

from __future__ import annotations

import threading
import time
import zlib
from collections.abc import Callable
from datetime import UTC, datetime

EPOCH_MS = 1_640_995_200_000  # 2022-01-01T00:00:00Z; must precede the oldest observed_at
TIME_BITS = 41
BUCKET_BITS = 10
SEQ_BITS = 12
BUCKET_COUNT = 1 << BUCKET_BITS
SEQ_MAX = (1 << SEQ_BITS) - 1
_MAX_OFFSET = (1 << TIME_BITS) - 1


def bucket_for(entity_key: str) -> int:
    return zlib.crc32(entity_key.encode("utf-8")) % BUCKET_COUNT


def shard_for_bucket(bucket: int, shard_count: int = 4) -> int:
    return bucket % shard_count


def make_id(ts_ms: int, bucket: int, seq: int) -> int:
    offset = ts_ms - EPOCH_MS
    if not 0 <= offset <= _MAX_OFFSET:
        raise ValueError(f"timestamp {ts_ms} outside the 41-bit range from the epoch")
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"bucket {bucket} out of range")
    if not 0 <= seq <= SEQ_MAX:
        raise ValueError(f"sequence {seq} out of range")
    return (offset << (BUCKET_BITS + SEQ_BITS)) | (bucket << SEQ_BITS) | seq


def decode_id(snowflake: int) -> tuple[int, int, int]:
    """Return ``(ts_ms, bucket, seq)``."""
    seq = snowflake & SEQ_MAX
    bucket = (snowflake >> SEQ_BITS) & (BUCKET_COUNT - 1)
    offset = snowflake >> (BUCKET_BITS + SEQ_BITS)
    return offset + EPOCH_MS, bucket, seq


def backfill_id(observed_at: datetime, entity_key: str, legacy_id: str) -> int:
    """Deterministic id for a backfilled row: time comes from ``observed_at``.

    The sub-second offset and sequence are derived from ``crc32(legacy_id)``, so a
    re-run yields identical ids. A (ms, bucket, seq) collision is possible but
    ~1e-4 per shard; the primary key rejects it loudly rather than hiding it.
    """
    h = zlib.crc32(legacy_id.encode("utf-8"))
    seconds = int(observed_at.replace(tzinfo=UTC).timestamp())
    return make_id(seconds * 1000 + h % 1000, bucket_for(entity_key), (h // 1000) % (SEQ_MAX + 1))


def _wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000


class LiveIdGenerator:
    """Ingest-time ids for live writes, unique and monotonic per bucket *within one process*.

    The 41|10|12 layout has no worker bits, so two processes issuing ids for the same
    bucket in the same millisecond can collide. Because the evidence PK is
    (id, observed_at), such a collision is only rejected by the database when observed_at
    also matches. Until P4 assigns bucket ownership to a single writer, run one generator
    per bucket (see ADR 0004).
    """

    def __init__(self, clock: Callable[[], int] = _wall_clock_ms) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._last: dict[int, tuple[int, int]] = {}

    def next_id(self, bucket: int) -> int:
        with self._lock:
            now = self._clock()
            last_ms, seq = self._last.get(bucket, (-1, -1))
            if now <= last_ms:
                # Same ms, or the clock stepped backwards: stay monotonic on last_ms and
                # borrow the next millisecond once the sequence is exhausted.
                now, seq = (last_ms, seq + 1) if seq < SEQ_MAX else (last_ms + 1, 0)
            else:
                seq = 0
            self._last[bucket] = (now, seq)
            return make_id(now, bucket, seq)
