# ADR 0003: Hand-rolled dual-write and backfill; per-shard read-flag cutover

## Status

Accepted (P2). Code: `db/optimized/backfill.py`, `db/optimized/verify_backfill.py`,
`evidencehub/ingestion/dual_writer.py`, `db/optimized/versions/0002_dualwrite_outbox.py`.

## Context

Moving from `evidence_legacy` to `evidence` is a transform, not an `ALTER`:

- the primary key changes (VARCHAR(36) UUID → `(BIGINT snowflake, observed_at)`);
- the table splits into `evidence`, `evidence_payload`, `evidence_content_hash`, and
  three dimension tables;
- every column changes type, and dimension values become foreign ids;
- the target is partitioned (ADR 0002).

## Decision: no gh-ost, no pt-online-schema-change

Both tools copy one table into a shadow table of the same logical shape, then rename.

- Neither can split one table into six or resolve dimension ids during the copy.
- gh-ost does not support tables with foreign keys or triggers, and its row copy is keyed
  on a shared unique key between old and new shape. The PK change here leaves no shared
  unique key.
- pt-osc keeps the shadow table in sync with triggers on the source. That adds write
  amplification on a buffer pool P1 measured as saturated (512 MiB pool against a
  ~5.08 GiB table per shard).

Instead:

1. **Alembic** creates the new tables additively (`db/optimized`, own version table
   `alembic_version_optimized`, explicit `-x shard=` with no implicit fan-out).
2. **Dual-write** (P2: a single-process test writer; P4: the real ingestion path). The
   legacy write commits first and stays the source of truth. The optimized write is one
   transaction. If it fails, it rolls back and the legacy id goes to `dualwrite_outbox`,
   so no request fails because of the new schema.
3. **Resumable chunked backfill.** Each chunk is one transaction that also updates
   `backfill_progress`. Ids are deterministic (ADR 0004), so re-runs are safe.
4. **Verification** (`verify_backfill.py`): per-month `COUNT` and
   `SUM(CRC32(CONCAT_WS(...)))` in SQL on both sides; a per-chunk CRC over a canonical
   rendering of every row in range; a field-by-field spot-check of random rows located
   through the app's access path; dimension counts against distinct legacy values.

## Decision: per-shard read-flag cutover, not an atomic rename swap

**This deviates from the project brief**, which specifies an "atomic rename swap".

- ADR 0001 already gives the two schemas different names (`evidence_legacy`, `evidence`),
  so nothing needs to be renamed for the new table to be addressable.
- The schemas are not interchangeable: callers need different SQL for each (dimension
  joins, status codes, `entity_pk`). A `RENAME TABLE` cannot switch application SQL, so
  it would not make a cutover atomic in any useful sense.
- A read flag per shard lets each shard be cut over, observed, and rolled back on its own.
  Rollback is flipping the flag back: legacy is still written by the dual-writer, so it
  loses nothing.
- `evidence_legacy` is kept until the owner signs off. Dropping it is a separate,
  explicitly approved step.

## Decision: load order (go/no-go on month-ordered loading)

Measured on 2026-11-19, partition `p202406`:

| | Shard 1 (PK order, 1M-row sample) | Shard 2 (month order, one month) |
|---|---|---|
| Rows in partition | 21,938 | 115,687 |
| `evidence` file bytes/row | 860.4 | 398.8 (−53.6%) |
| `evidence` page-stat bytes/row | 482.5 | 309.3 (−35.9%) |
| All three tables, page stats | 924.8 | 627.6 (−32.1%) |
| Legacy read for the batch | 14% of 209s for 1M rows | one 135.1s scan for the month |
| Insert rate | 4,775 rows/s overall, falling to about 2,400 at the end | 15,989 rows/s |

The row counts differ by about 5.3x, so fixed per-partition overhead is spread over more
rows on shard 2. That inflates the file-based gain; the page-stat figures are the less
biased comparison.

**Caveat (2026-11-20).** The page-stat figures in this table were read from
`information_schema` without `information_schema_stats_expiry=0`, so they may be cached
values. They are kept as the record of the go/no-go decision, not as size claims. The
full-shard sizes in `docs/perf/backfill.md` supersede them: they are measured both as
`.ibd` file size (`stat`) and as fresh pages in use (`stats_expiry=0`).

Go/no-go rule (owner, 2026-11-19): month-ordered only if bytes/row drops ≥25% **and** the
per-month legacy scan is about 2 minutes or less. Bytes/row passes clearly. The scan
measured 135.1s, about 15s over. **Outcome: pending the owner's decision**, because the
disk projection shows only month-ordered loading comes close to fitting.

**Decided by the owner (2026-11-19): month-ordered for shards 1–3, with the 2-minute scan
bar waived** (bytes/row passed; the miss was 15s of wall-clock). Each shard is verified
(`verify_backfill.py --all`), then its binlogs are purged. No legacy table is dropped. After
shard 3, shard 4 is re-projected from the measured growth and put back to the owner
(free other space on the host, or stop at three shards).

Month-order mechanics: each month is one streamed legacy scan ordered by
`(observed_at, id)`, checkpointed per batch as `(cursor_observed_at, last_legacy_id)`
(migration `0003_backfill_month_cursor`). A completed month is skipped without rescanning;
an interrupted one resumes after its cursor. Rows already loaded by the PK-order sample on
shard 1 are skipped by `write_records`. On shard 1 the month-ordered rows still interleave
with the 1M sample rows already in each partition, so its density will land between the
two measured figures.

## Results and binary logging (2026-11-20)

Measured per-shard results are in `docs/perf/backfill.md`. Shards 1 and 2 are loaded and
pass `verify_backfill --all`. On shard 2, loaded in month order from almost empty, the
optimized tables are 0.696 of legacy by file size and 0.605 by pages in use. Shard 1 is
1.025 / 0.922, because the 1M sample rows loaded in PK order fragment its tables (most of
all `evidence_payload`).

**Session-only binary logging off.** `backfill.py --session-no-binlog` runs
`SET SESSION sql_log_bin=0` on every connection the backfill opens and checks
`@@session.sql_log_bin = 0` before reading anything; if the server refuses, the run exits
5 and does nothing. It never changes a global setting. It requires
`SESSION_VARIABLES_ADMIN`, which the app user does not hold; for shard 2 the owner
approved granting it to `evidencehub@%` on that shard only for the duration of the load,
and it was revoked when the load exited. With it, shard 2's 5,122,583-row load added no
binlog bytes; shard 1's load with binlogs on wrote 427.6 B per inserted row.
Consequence: rows loaded this way are not in the binlog, so a replica or point-in-time
recovery from binlogs would not contain them. There are no replicas in this setup; any
future one must be seeded from a fresh snapshot taken after the load.
