# ADR 0004: Snowflake ID scheme, virtual buckets, and per-shard dedup scope

## Status

Accepted (P2). Code: `evidencehub/common/snowflake.py`.

## Decision

### Bit layout

`41-bit time (ms) | 10-bit virtual bucket | 12-bit sequence`, unsigned 64-bit.

- **Epoch:** 2022-01-01T00:00:00Z. It must precede the oldest `observed_at`
  (2023-01-01), because backfilled ids take their time from `observed_at`. 41 bits of
  milliseconds lasts about 69 years from the epoch.
- **Bucket:** `crc32(entity_id) % 1024`. This is a *virtual* bucket, not the physical shard.
- **Initial bucket → shard map:** `bucket % 4`. Because 1024 is divisible by 4,
  `crc32(entity_id) % 1024 % 4 == crc32(entity_id) % 4`, which is exactly the placement
  `db/legacy/seed_legacy.py` already used (`_shard_index_for_entity`). P3 can move buckets
  between shards by changing the map only; no id is ever re-issued.

### Time component

- **Live writes:** ingest time (`LiveIdGenerator`). Ids are roughly monotonic, so live
  inserts append to the right edge of the clustered index.
- **Backfilled rows:** `observed_at` (`backfill_id`). The clustered key is
  `(id, observed_at)`; taking the id time from `observed_at` makes id order follow
  `observed_at` order, so a load in `observed_at` order is a sequential insert.
- Backfill ids are **deterministic**: the sub-second offset is `crc32(legacy_id) % 1000`,
  the sequence is `(crc32(legacy_id) // 1000) % 4096`. Re-running the backfill over a row
  produces the same id, so a resumed or repeated run cannot create duplicates; it fails on
  the primary key instead.

### Collision analysis

- **Backfill:** two backfilled rows can only share an id if they fall in the same
  second (time bits), the same bucket, and the same 22 hash-derived bits. Expected
  collisions per ~5.2M-row shard are about 1e-4. A collision always has identical
  `observed_at` seconds, so the `(id, observed_at)` PK rejects it loudly. The 1M-row sample
  on shard 1 wrote 1,000,000 rows with no PK error.
- **Live writes:** unique and monotonic per bucket **within one process** (a lock plus
  per-bucket last-ms and sequence; clock steps backwards are absorbed). The layout has no
  worker bits, so **two processes generating ids for the same bucket can collide**, and
  because the PK is `(id, observed_at)` such a collision is only rejected when
  `observed_at` also matches. Otherwise two rows silently share an id.
  **Constraint until P4:** exactly one id-generating writer per bucket. P4's ingestion
  design must either assign bucket ownership to a single writer or add an id registry.
  The P2 dual-writer is a single-process test writer, so it meets this constraint.

### Dedup scope

`evidence_content_hash (content_hash BINARY(32) PRIMARY KEY, evidence_id)` is
non-partitioned, so it can enforce uniqueness where `evidence` cannot (ADR 0002).

It is **per shard**. Placement is by entity, so identical content attached to two
entities on different shards is not detected. **Cross-shard dedup is out of scope until
P3's global lookup index.** The legacy data has no duplicate hashes in the shard-1 sample
(1,000,000 rows, 1,000,000 distinct hashes; the hash includes the entity id and a random
source id, see the seeder), so this does not affect the before/after comparison.

## Consequences

- Shard routing in P3 maps `bucket → shard` from a catalog table, defaulting to
  `bucket % 4`.
- `decode_id` recovers `(ts_ms, bucket, seq)`. For backfilled rows the time bits equal
  `observed_at` (to the second), which lets a point lookup target one partition.
