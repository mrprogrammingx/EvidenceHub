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

See `docs/perf/baseline.md` for the full, measured results. Summary of what the run confirmed as
the biggest offenders P2 needs to fix, ranked by how far each query landed from its SLO:

_(filled in after the P1 benchmark run completes -- see docs/perf/baseline.md for the numbers
this table is built from)_

## Consequences

- `evidence_legacy` is throwaway once P2's migration is verified; no code outside `db/legacy/`
  and the P1 benchmark harness should ever come to depend on its shape.
- The P2 before/after report in `docs/perf/` compares `evidence_legacy` (before) against
  `evidence` (after) directly by name, with no ambiguity about which table is which.
