# P2 backfill: measured results per shard

Everything here is measured. Where a figure is a projection it says so.
Code: `db/optimized/backfill.py`, `db/optimized/verify_backfill.py`. Decisions: ADR 0003.

Shard names are 1-based (container `evidencehub-mysql-shard-N`); the CLI `--shard` index is
0-based. Shard 1 = index 0 = `evidencehub_shard_0`; shard 2 = index 1 = `evidencehub_shard_1`.

## Status

| Shard | Loaded | `verify_backfill --all` | Binlogs |
|---|---|---|---|
| 1 | 5,247,983 rows, 2026-11-19 | passed | grew 2,120,745,786 B; purged to `binlog.000010` |
| 2 | 5,238,270 rows, 2026-11-19/20 | passed | did not grow (session `sql_log_bin=0`); not purged |
| 3 | not started | — | — |
| 4 | not started | — | — |

`evidence_legacy` is untouched on every shard. Nothing is pushed.

## Shard 1 (index 0)

- Order: the 1M-row PK-order sample was already loaded; then month order, 2023-01..2026-09.
- 2024-06 was loaded and verified first as the overlap test: 115,881 rows processed,
  93,943 inserted, 21,938 skipped (exactly the sample rows in `p202406`);
  `verify_backfill --month 2024-06` passed (115,881 = 115,881, SQL checksum equal,
  58/58 chunks, 0 duplicates by (entity_pk, observed_at, content_hash), 200/200 spot).
- Remaining 44 months: 5,132,102 processed, 4,154,040 inserted, 978,062 skipped.
  Skipped total 978,062 + 21,938 = 1,000,000 = the sample. 0 collisions.
- Load wall clock 3,110 s. Legacy scan to first row per month: min 33.6 s, median 36.9 s,
  max 46.1 s (1,646 s in all; legacy has no `observed_at` index).
- Binlogs were on: `binlog.000009` + `binlog.000010` = 2,120,745,786 B after the load, of
  which 304,450,063 B was already in `binlog.000009` from the sample. The month-order load
  wrote 1,816,295,723 B for 4,247,983 inserted rows: 427.6 B/row. `PURGE BINARY LOGS TO 'binlog.000010'` freed 1,048,836 KiB inside the VM;
  `binlog.000010` (1,046,764,370 B) is the current file and stays.
- `verify_backfill --all`: 45/45 months, 2,624/2,624 chunks (5,247,983 rows),
  200/200 spot, dims 125,000 / 5 / 8, hashes 5,247,983. Wall clock about 49 min.

## Shard 2 (index 1)

- Before: 115,687 rows from the earlier one-month test (all 2024-06), migration 0001.
  Upgraded to `0003_backfill_month_cursor`.
- Binary logging was turned off **for the backfill's own sessions only**
  (`--session-no-binlog` → `SET SESSION sql_log_bin=0` on every connection, verified
  before any read). That needs `SESSION_VARIABLES_ADMIN`, which the app user does not
  have. Owner-approved: granted to `evidencehub@%` on this shard only, before the load,
  and revoked when the load exited (2026-11-19T21:27:01Z); `SHOW GRANTS` afterwards
  equals the original three lines. No global setting was changed (`log_bin=1`,
  `binlog_expire_logs_seconds=2592000`).
- 45 months: 5,238,270 processed, 5,122,583 inserted, 115,687 skipped (= the pre-existing
  rows). 0 collisions, 0 duplicate hashes. 2024-06 progress `rows_written` = 115,687 (the
  stale pre-0003 count is no longer added; fixed in `b5fb5a2`).
- Load wall clock 3,541.8 s. Legacy scan to first row per month: min 34.8 s, median
  39.9 s, max 57.6 s (1,840.8 s in all). Per-batch insert rate, median of monthly
  medians: 3,381.8 rows/s.
- Binlog: `binlog.000009` 37,438,520 B before the load, 37,438,520 B after it, 37,438,777 B
  after the REVOKE. No growth from the load, so no purge.
- `verify_backfill --all`: 45/45 months (5,238,270 = 5,238,270), 2,620/2,620 chunks,
  200/200 spot, dims 125,000 / 5 / 8, hashes 5,238,270, payloads 5,238,270, outbox 0.
  The Mac was asleep for most of this run (lid closed 01:38:59 to 10:36 local), so its
  wall clock (34,401 s) is not a performance figure.

## Size (2026-11-20)

Two different measures, both read on the live shards:

- **File**: `.ibd` size via `stat -c %s` inside the container (all partitions summed for
  `evidence`). This is what the disk uses and what the guardrail sees.
- **Pages in use**: `data_length + index_length` from `information_schema.tables` with
  `SET SESSION information_schema_stats_expiry=0`. `data_free` is shown separately.
  Without `stats_expiry=0` these values are cached and can be stale; do not quote them.

### Shard 1 (5,247,983 rows)

| Table | File B | File B/row | Pages in use B | Pages B/row | data_free B |
|---|---:|---:|---:|---:|---:|
| evidence | 2,905,604,096 | 553.7 | 2,432,499,712 | 463.5 | 262,144,000 |
| evidence_payload | 2,101,346,304 | 400.4 | 2,002,649,088 | 381.6 | 6,291,456 |
| evidence_content_hash | 557,842,432 | 106.3 | 515,899,392 | 98.3 | 6,291,456 |
| dim_* (3 tables) | 27,525,120 | 5.2 | 18,972,672 | 3.6 | 4,194,304 |
| **optimized total** | **5,592,317,952** | **1,065.6** | **4,970,020,864** | **947.0** | |
| evidence_legacy | 5,456,789,504 | 1,039.8 | 5,391,564,800 | 1,027.4 | 7,340,032 |

Optimized / legacy: file 1.025, pages in use 0.922.

### Shard 2 (5,238,270 rows)

| Table | File B | File B/row | Pages in use B | Pages B/row | data_free B |
|---|---:|---:|---:|---:|---:|
| evidence | 2,070,937,600 | 395.3 | 1,635,516,416 | 312.2 | 234,881,024 |
| evidence_payload | 1,128,267,776 | 215.4 | 1,063,256,064 | 203.0 | 6,291,456 |
| evidence_content_hash | 553,648,128 | 105.7 | 531,628,032 | 101.5 | 4,194,304 |
| dim_* (3 tables) | 26,476,544 | 5.1 | 17,924,096 | 3.4 | 4,194,304 |
| **optimized total** | **3,779,330,048** | **721.5** | **3,248,324,608** | **620.1** | |
| evidence_legacy | 5,431,623,680 | 1,036.9 | 5,370,560,512 | 1,025.3 | 4,194,304 |

Optimized / legacy: file 0.696, pages in use 0.605.

### Why the shards differ

Shard 1 holds the 1M sample rows loaded in legacy PK (UUID) order, i.e. random id order,
spread through every month; month-ordered rows were then inserted between them. The
effect is largest in `evidence_payload` (not partitioned, clustered by id): 400.4 vs
215.4 file B/row. Shard 2, loaded in month order from almost empty, is the representative
figure for shards 3 and 4. Rebuilding shard 1's tables (`OPTIMIZE TABLE`) would likely
reclaim the difference but needs temporary disk; not done, not measured.

## Host disk (`/`, KiB free; `df -k`)

| Point | Shard 1 | Shard 2 |
|---|---:|---:|
| Before load | 29,347,284 (before 2024-06) | 21,942,860 |
| After load | 21,368,236 | 17,054,704 |
| 5 min after | — | 16,010,396 |
| Before purge / right after | 21,329,468 / 21,329,648 | no purge |
| 5 min after purge | 22,387,704 | — |

`Docker.raw` allocated size (`du -k`): shard 2 load 37,878,496 → 41,448,448 KiB; 5 min after
41,449,172; next morning 41,451,220.

Net cost per inserted row, shard 2 (5,122,583 rows), three readings:

| Basis | Bytes | B/row |
|---|---:|---:|
| Host free drop during the load | 5,005,471,744 | 977.1 |
| Host free drop to 5 min after | 6,074,843,136 | 1,185.9 |
| `Docker.raw` growth during the load | 3,655,630,848 | 713.6 |

Host free space also moves for reasons outside the project: it fell 1,044,308 KiB in the
5 minutes after the load while `Docker.raw` grew 724 KiB, and was 19,927,304 KiB the next
morning (+3.9 GB) with `Docker.raw` unchanged. macOS had OS-update local snapshots present
(`com.apple.os.update-*`, `MSUPrepareUpdate`). The host-based rates therefore include
non-project activity; `Docker.raw` growth is the rate attributable to the load.

## Projection for shards 3 and 4 (projection, not a measurement)

From host free 19,927,304 KiB (19.00 GiB, 2026-11-20T07:01Z), legacy rows 5,252,709
(shard 3) and 5,248,636 (shard 4) from `baseline.md`, binlogs off as on shard 2:

| Rate (B/row) | Free after shard 3 | Free after shard 4 |
|---|---:|---:|
| 977.1 (host, during load) | 14.22 GiB, fits | 9.45 GiB, short 0.55 GiB |
| 1,185.9 (host, incl. 5 min after) | 13.20 GiB, fits | 7.41 GiB, short 2.59 GiB |
| 713.6 (`Docker.raw`) | 15.51 GiB, fits | 12.02 GiB, fits |

With binlogs on (adding shard 1's measured 427.6 B/row), shard 3 would land at
11.11–13.42 GiB.
Shard 3 fits above the 10 GiB floor on every measured rate. Shard 4 fits only on the
`Docker.raw` rate. The guardrail stops a run cleanly at 10 GiB (exit 3) and it resumes.
Free space on this host has varied by several GB over hours without project activity, so
the projection must be redone from a fresh reading immediately before each shard.
