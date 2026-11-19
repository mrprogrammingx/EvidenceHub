# ADR 0002: Partition key, primary key shape, and partition list for `evidence`

## Status

Accepted (P2). Migration: `db/optimized/versions/0001_evidence_optimized.py`.

## Context

Five of the eight critical queries (`queries/critical/02, 03, 05, 07, 08`) are bounded or
ordered by `observed_at`. The two cross-entity range aggregates (05, 07) are the ones only
partition pruning can bound, since they touch every entity. MySQL requires the
partitioning column to be part of every unique key on a partitioned table, including the
primary key.

`observed_at` is not ingest time: the legacy data was generated with `observed_at` spread
uniformly from 2023-01-01, while every row's `created_at` is 2026-09-22 (see
`docs/perf/baseline.md` and the P2 read-only scans below).

## Decision

- `PARTITION BY RANGE COLUMNS(observed_at)`, one partition per calendar month.
- Primary key `(id, observed_at)`. `id` is still unique by construction (ADR 0004), but the
  database can only enforce uniqueness of the pair.
- Partition list: `p202301` .. `p202712` (60 monthly partitions) plus `pmax` (MAXVALUE),
  61 in total, so ingest never fails on an out-of-range date. Partitions for 2028+ are
  added by `REORGANIZE PARTITION pmax` ahead of time (runbook, P5).

### Evidence for the range

Read-only scans of `evidence_legacy` on all 4 shards (2026-11-19):

- `MIN(observed_at)` = `2023-01-01 00:00:00` .. `00:00:01`, `MAX(observed_at)` =
  `2026-09-22 10:xx` on every shard: 45 populated months (2023-01 .. 2026-09).
- Rows per month per shard: roughly 107k (February) to 120k; 2026-09 is about 82k because
  the data stops on 09-22.

## Alternatives rejected

- **RANGE on the snowflake id** (PK `(id)` alone). Only prunes if id time tracks
  `observed_at`. Live ids use ingest time (ADR 0004), and late-arriving evidence is the
  normal case, so queries 05 and 07 would not prune.
- **HASH/KEY partitioning on entity.** Helps nothing that the `entity_pk`-leading indexes do
  not already serve, and gives no pruning for the range aggregates.

## Consequences

- A point lookup by `id` alone (query 01) probes all 61 partitions, one B-tree dive each.
  The baseline for 01 is p95 4.0ms; the P2 after-run must show it did not regress. Callers
  that know `observed_at` (for example from `evidence_payload`, or from the id's time bits
  for backfilled rows) should pass it.
- No unique index on `content_hash` is possible on `evidence`; dedup lives in the
  non-partitioned `evidence_content_hash` table (ADR 0004).
- Each partition carries its own copy of every index tree. With 61 partitions × 6 index
  trees, small partitions have noticeable fixed overhead. On the 1M-row sample, the
  `evidence` files were 881 bytes/row against 518 bytes/row of used pages
  (`information_schema`), partly due to that overhead.
