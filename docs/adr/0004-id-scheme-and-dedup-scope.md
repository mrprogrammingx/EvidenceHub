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
  produces the same id, so a resumed or repeated run recognises the row as already loaded
  and skips it instead of duplicating it.

### Collision analysis

- **Backfill:** two backfilled rows can only share an id if they fall in the same
  second (time bits), the same bucket, and the same 22 hash-derived bits. Expected
  collisions per ~5.2M-row shard are about 1e-4. The 1M-row sample on shard 1 wrote
  1,000,000 rows with none.
- **What happens on a collision (decided 2026-11-19): stop, never quarantine.**
  `write_records` raises `IdCollisionError` naming the legacy ids, whether the twin is in
  the same batch or already in `evidence` with a different `content_hash`. The batch rolls
  back, the checkpoint does not advance, and the backfill exits with code 4. Quarantining
  and continuing would leave a legacy row with no optimized row, which the cutover
  forbids. Resolution is a manual, reviewed remap of the later row; an automatic
  re-salt would make ids depend on load order and break re-run idempotency.
  Tested with a natural collision found by birthday search
  (`tests/integration/test_backfill.py::test_id_collision_stops_and_names_legacy_ids`).
- **Re-runs vs. collisions.** An existing `(id, observed_at)` with the *same*
  `content_hash` **and the same `entity_pk`** is the same legacy row loaded earlier
  (PK-order sample, an interrupted month, a forgotten checkpoint) and is skipped. A
  different hash or a different `entity_pk` is a collision and raises. Tested with a
  natural twin (colliding legacy ids, two entities in one bucket, one hash):
  `test_same_id_and_hash_for_another_entity_is_a_collision_not_a_skip`.
- **Residual limit.** `evidence` does not store `legacy_id` (it would cost ~36+ bytes/row
  on a disk budget that is already tight), so two legacy rows identical in id, hash and
  entity cannot be told apart from a re-run, and the second would be skipped. That needs
  a crc32 collision on the legacy id, the same second, the same entity *and* the same
  content hash. If it happens, `verify_backfill.py` reports that month's `COUNT` as one
  short, so it cannot pass verification unnoticed.
- **Live writes:** unique and monotonic per bucket **within one process** (a lock plus
  per-bucket last-ms and sequence; clock steps backwards are absorbed). The layout has no
  worker bits, so **two processes generating ids for the same bucket can collide**, and
  because the PK is `(id, observed_at)` such a collision is only rejected when
  `observed_at` also matches. Otherwise two rows silently share an id.
  **Constraint until P4:** exactly one id-generating writer per bucket. P4's ingestion
  design must either assign bucket ownership to a single writer or add an id registry.
  The P2 dual-writer is a single-process test writer, so it meets this constraint.

### Duplicate content hashes: one rule for both write paths

**Every legacy row gets an optimized row; `evidence_content_hash` is owned by the lowest
id.** The backfill and the dual-writer both write through
`evidencehub/migration/writer.py::write_records`, so the rule cannot drift between them:

- the `evidence` and `evidence_payload` rows are always written;
- `evidence_content_hash` points at the **lowest** id holding that hash
  (`ON DUPLICATE KEY UPDATE evidence_id = LEAST(evidence_id, VALUES(evidence_id))`). The
  upsert is atomic under the row lock, so the owner does not depend on load order or on
  which writer gets there first. Changed 2026-11-19 from first-writer-wins, which made the
  owner depend on load order;
- for backfilled ids the time bits are `observed_at`, so lowest id means **earliest
  `observed_at`, ties broken by id**. Tested both ways round:
  `test_duplicate_hash_rows_all_loaded_and_earliest_owns_hash[forward|reverse]`;
- `duplicate_of` in a write's result is the owner *at write time*. If an earlier-observed
  row arrives later, it takes ownership and the row written first was not reported as a
  duplicate. Ingestion (P4) decides what to do with that signal; the storage layer never
  drops a row.

**Known limit (live ids).** Live ids take their time from ingest time, not `observed_at`.
Between a live row and a backfilled row with the same hash, the backfilled row (older id
time) wins even if the live row has an earlier `observed_at`. Making that exact needs the
owner's `observed_at` stored in `evidence_content_hash`, which is deferred to P4 with the
real ingestion path.

The P2 dual-writer previously skipped the optimized row on a duplicate. That was changed
so the optimized table holds every legacy row, as the cutover requires.

### No `INSERT IGNORE`

`INSERT IGNORE` turns strict-mode errors (data too long, NULL into NOT NULL) into warnings
and stores truncated or default values. All inserts that can legitimately meet an
existing key use a no-op `ON DUPLICATE KEY UPDATE`, which only absorbs the duplicate key;
every other error still raises (shards run with `STRICT_TRANS_TABLES`). Duplicate
detection reads the owning id back instead of trusting `rowcount`, because SQLAlchemy's
pymysql dialect sets `CLIENT_FOUND_ROWS`, which makes a no-op upsert report one row.

### Dedup scope

`evidence_content_hash (content_hash BINARY(32) PRIMARY KEY, evidence_id)` is
non-partitioned, so it can enforce uniqueness where `evidence` cannot (ADR 0002).

It is **per shard**. Placement is by entity, so identical content attached to two
entities on different shards is not detected. **Cross-shard dedup is out of scope until
P3's global lookup index.** Measured on the fully loaded shards: shard 1 has 5,247,983
rows and 5,247,983 distinct hashes, shard 2 has 5,238,270 and 5,238,270, so no row on
either shard is a duplicate and per-shard scope does not affect the before/after
comparison there.

### `entity_pk` allocation (known limit)

`entity_pk_for` derives `entity_pk` by parsing `entity-########` (8 digits, so at most
99,999,999). It rejects any other shape loudly rather than guessing. This is deterministic
and global with no lookup, which is what the backfill needs, but it only covers the
legacy key format.

For P4, keys of any other shape are allocated from a global registry in the catalog DB
(P3): `entity_registry(entity_key VARCHAR(255) UNIQUE, entity_pk BIGINT UNSIGNED)`,
issuing `entity_pk` values from **100,000,000 upward**, so they can never overlap a parsed
legacy value. Legacy-shaped keys keep their parsed value and are registered with it.
Until the registry exists, the dual-writer and backfill accept legacy-shaped keys only.

## Consequences

- Shard routing in P3 maps `bucket → shard` from a catalog table, defaulting to
  `bucket % 4`.
- `decode_id` recovers `(ts_ms, bucket, seq)`. For backfilled rows the time bits equal
  `observed_at` (to the second), which lets a point lookup target one partition.
