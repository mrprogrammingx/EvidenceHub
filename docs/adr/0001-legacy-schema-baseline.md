# ADR 0001: Legacy schema as a deliberate baseline, and the evidence/evidence_legacy naming convention

## Status

Accepted (P1). Revisited when P2 lands the optimized schema.

## Context

EvidenceHub exists to demonstrate a real before/after MySQL optimization story, not just an
optimized schema in isolation. That requires an intentionally bad "before" to measure against,
and it requires the "before" and "after" to coexist during the P2 online migration (dual-write
period) without naming collisions.

## Naming convention (resolves the P0 gate's naming ambiguity)

- **`evidence_legacy`** (this phase) is the deliberately bad baseline table, on all 4 shards.
  It is never renamed and never migrated in place; it exists to be measured against and, once
  P2's migration is verified, retired.
- **`evidence`** (P2) is the optimized table's name -- the clean, canonical name is reserved for
  the schema meant to be the long-term production table, not the legacy one. This also means no
  rename is needed mid-migration: P2 builds `evidence` alongside `evidence_legacy`, dual-writes,
  backfills, verifies, then cuts reads over -- `evidence_legacy` can simply be dropped once the
  cutover is verified, with no window where the production name points at unverified data.
- **Companion tables introduced in P2** (payload side table, append-only status history, etc.)
  follow the pattern `evidence_<concern>`, e.g. `evidence_payload`, `evidence_status_history`.
  Exact shape is a P2 design decision, not fixed here.
- **Catalog/shard-map tables** (P3) live in the separate catalog database and are not prefixed
  with `evidence_` at all, since they describe routing/topology, not evidence data itself.

## Decision: why the legacy schema is shaped this way

`evidence_legacy` (`db/legacy/versions/0001_legacy_evidence.py`) intentionally reproduces the
anti-patterns named in the project brief, each chosen to cause a specific, measurable problem
rather than being generically "bad":

| Anti-pattern | Concrete cost it causes |
|---|---|
| `varchar(36)` UUID primary key | Random insert order fragments the InnoDB clustered index; every secondary index carries a 36-byte copy of the PK instead of 8 bytes for a BIGINT |
| `confidence_score`, `observed_at` stored as VARCHAR | No numeric/temporal indexing or range-scan efficiency; range/aggregate queries can't use an index even if one existed |
| `is_deleted` stored as VARCHAR('true'/'false') | Extra bytes per row at 20M+ scale; can't use a compact TINYINT(1) flag efficiently |
| Payload crammed into a LONGTEXT blob | No structured access to payload fields without full deserialization; bloats row size, hurts buffer-pool hit ratio |
| `utf8` (utf8mb3) charset | Can silently fail/truncate on 4-byte characters (e.g. emoji) that real evidence text could contain |
| `ix_evidence_legacy_status` + `ix_evidence_legacy_status_entity` | Redundant: the single-column index is a strict prefix of the composite one, so it's dead weight on every write for zero read benefit |
| `ix_evidence_legacy_entity_name` | Indexes a low-cardinality denormalized display column that no critical query actually filters on |
| No index with `entity_id` as the leading column | The single most common access pattern -- "latest evidence for this entity" -- has no supporting index at all |
| No index on `observed_at`, no unique index on `content_hash` | Range queries and ingest-time dedup checks are both full table scans |
| No partitioning | No partition pruning is possible on any date-bounded query, however large the table gets |

## Baseline benchmark findings (P1)

See `docs/perf/baseline.md` for the full, measured results (20,987,598 rows across 4 shards --
104.9% of the 20M target, reached by the seed generator's own random per-entity row-count
variance, not a partial/truncated run). Summary of what the run confirmed as the biggest
offenders P2 needs to fix, ranked by how far each query landed from its SLO (p95, single-shot
wall-clock where noted -- see baseline.md for why 6 of the 8 queries could not be concurrency
load-tested):

| Query | SLO | Measured p95 | Factor over SLO | Index used |
|---|---|---|---|---|
| per_entity_status_aggregate | 50ms | 124,318ms | ~2,486x | `ix_evidence_legacy_status` (index scan, not seek -- no leading `entity_id`) |
| range_aggregate_status_window | 300ms | 132,526ms | ~442x | `ix_evidence_legacy_status` (index scan, not seek -- no leading `observed_at`) |
| keyset_paginated_feed | 50ms | 55,473ms | ~1,109x | full table scan |
| top_entities_by_volume_window | 300ms | 64,947ms | ~216x | full table scan |
| content_hash_dedup_lookup | 50ms | 21,412ms | ~428x | full table scan |
| latest_n_by_entity_type_daterange | 50ms | 34,508ms | ~690x | full table scan |
| latest_n_by_entity_status | 50ms | 16ms | PASS | `ix_evidence_legacy_status_entity` (index seek -- both predicates are equality, so the "wrong-leading-column" index still serves it) |
| point_lookup_by_id | 50ms | 4ms | PASS | primary key (const lookup) |

One correction to the anti-pattern table above: `latest_n_by_entity_status` was predicted to have
"no supporting index at all," but `ix_evidence_legacy_status_entity` (status, entity_id) *does*
serve it efficiently here because both `status` and `entity_id` are equality predicates -- a
composite index can be used for an equality match on a non-leading column as long as every column
before it in the index is also equality-constrained. The index is still mis-shaped for the more
general "recent evidence for entity X, any status" access pattern, which is not measured here.

**Root cause confirmed, not assumed:** on `evidencehub-mysql-shard-1`, `innodb_buffer_pool_size` =
512 MiB exactly against a ~5.08 GiB `evidence_legacy` table (~10x the pool, per shard). An
isolated forced full clustered-index scan (`FORCE INDEX (PRIMARY)`, 5,247,983 rows, no concurrent
load) measured a **50.2% InnoDB buffer pool hit ratio** via before/after
`Innodb_buffer_pool_read%` deltas, and `SHOW ENGINE INNODB STATUS` immediately after showed the
pool 100% occupied (0 free buffers) and I/O overwhelmingly read-dominated (215,318 OS file reads
vs. 332 writes). Full details and raw capture: `docs/perf/baseline.md` and
`docs/perf/innodb_status_shard1_20260922.txt`. This directly motivates P2's scope: a narrower,
better-clustered `evidence` table (BIGINT PK instead of random UUID, payload moved out of the hot
row) plus indexes with `entity_id`/`observed_at` leading, so the working set for the hot access
patterns -- not the whole table -- is what has to fit in a buffer pool sized for this machine's
Docker VM budget. Simply enlarging the buffer pool was deliberately not tried here (P1's job is to
measure the deliberately-bad baseline honestly, not patch it); P2 should treat buffer pool sizing
as one lever among several, not a substitute for fixing the PK/index design.

## Consequences

- `evidence_legacy` is throwaway once P2's migration is verified; no code outside `db/legacy/`
  and the P1 benchmark harness should ever come to depend on its shape.
- The P2 before/after report in `docs/perf/` compares `evidence_legacy` (before) against
  `evidence` (after) directly by name, with no ambiguity about which table is which.
