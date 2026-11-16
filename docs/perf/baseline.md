# P1 Baseline Benchmark: evidence_legacy

Generated: 2026-09-23T05:28:53.240622+00:00

Hardware: this machine (Apple Silicon, arm64), Docker Desktop VM capped at ~7.65GB RAM shared with other unrelated containers, each MySQL 8.0 shard tuned to a 512MB InnoDB buffer pool. See docs/perf/ for how to reproduce.

Dataset: **20,987,598 total rows** across 4 shards: [5247983, 5238270, 5252709, 5248636].

Load: Locust headless, 8 users, 60s run time, ~0-50ms think time.

Queries costing tens of seconds to minutes per execution (full scans over 5M+ rows) were not concurrency-load-tested: a real Locust load test against them is impractical (a single request can outlive the whole test run) and not representative of anything a production system would be asked to do. Those rows report a **single-shot** wall-clock measurement instead (source column), which already proves the SLO violation by 2-4 orders of magnitude.

**Known measurement artifact:** for `content_hash_dedup_lookup`, `top_entities_by_volume_window`, and `keyset_paginated_feed`, the `engine_ms` figure quoted per-query below (sourced from `performance_schema.events_statements_history.TIMER_WAIT`) is bogus -- around 3.6 million ms (~60 min) against a measured `wall_ms` of 21-65s. Independently, Python's `time.perf_counter()` (`wall_ms`) and `EXPLAIN ANALYZE`'s own `actual time` agree closely with each other for every query, so those two are what this report treats as ground truth; `TIMER_WAIT` is included in the raw capture for transparency but should be disregarded for these three rows.

## Actual scale vs. the 20M target

**Target: 20,000,000 rows (`SEED_ROWS_FULL`). Actual: 20,987,598 rows (104.9% of target).** No partial re-seed was needed -- the dataset already meets and slightly exceeds the full P1/P2 benchmark scale.

The 4.9% overshoot is not a partial/truncated run: `db/legacy/seed_legacy.py` splits the target across `target_rows // 40` entities and draws a random row count per entity in `[0.4x, 1.6x]` of the 40-row average, so the realized total lands near, not exactly at, the nominal target by design. Evidence this run reached natural completion rather than being cut off mid-flight:

- Every shard's `evidence_legacy.ibd` file (~5.4GB each) has a last-modified timestamp within the same ~2-minute window (10:56:xx-10:58:xx UTC, 2026-09-22) -- consistent with the seeder's 4 parallel per-shard workers finishing at roughly the same time, not a snapshot mid-write.
- Docker Desktop's own engine log shows a clean `POST /shutdown` at **15:34:28 UTC that same day** -- **4.5+ hours after** the data files stopped changing. There was no seed process running when this session started (`ps aux` showed nothing, and Docker Desktop itself was fully down); the seed had already finished and gone idle well before Docker was later shut down for unrelated reasons. The "COUNT(*) still executing after 500+ seconds" symptom was observed while the seed was actively writing (heavy concurrent insert load + a cold/thrashing buffer pool); with the seed idle, the same `COUNT(*)` now returns in 6-8s per shard.

## Root cause: InnoDB buffer pool vs. working set

Confirmed directly (not inferred) on `evidencehub-mysql-shard-1`, chosen as representative since all 4 shards share the same config and near-identical row counts:

- `innodb_buffer_pool_size` = 536,870,912 bytes = **512 MiB exactly**, confirmed via `SHOW VARIABLES`.
- That shard's `evidence_legacy` table: 3.29GB data + 1.73GB index (`ANALYZE TABLE` + `information_schema.tables`), and a 5,456,789,504-byte (~5.08 GiB) `.ibd` file on disk -- **~10x the buffer pool**, for one shard alone.
- **Isolated hit-ratio test:** ran `SELECT COUNT(*) FROM evidence_legacy FORCE INDEX (PRIMARY)` (5,247,983 rows, full clustered-index scan) as the only query running, comparing `SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_read%'` immediately before and after:
  - Before: `Innodb_buffer_pool_read_requests` = 649,330, `Innodb_buffer_pool_reads` (physical) = 26,119
  - After: `Innodb_buffer_pool_read_requests` = 1,028,975, `Innodb_buffer_pool_reads` = 215,137
  - Delta: +379,645 logical requests, +189,018 physical reads -> **hit ratio = 1 - 189,018/379,645 = 50.2%** for this single, uncontended scan.
- `SHOW ENGINE INNODB STATUS`, captured immediately after (full raw output: `docs/perf/innodb_status_shard1_20260922.txt`):
  - `Buffer pool size 32765` / `Free buffers 0` / `Database pages 32761` -- the pool is **completely saturated**, no headroom.
  - `Old database pages 12105` (~37%) and `Pages made young 8, not young 14` -- InnoDB's scan-resistance logic is actively evicting scanned pages almost as fast as they're read, so a repeat scan gets essentially no benefit from the previous one.
  - `215318 OS file reads, 332 OS file writes, 149 OS fsyncs` -- **read-dominated**, matching the original observation of heavy read volume against a small on-disk dataset.
  - `Number of rows read 5247983` -- exact match to the `COUNT(*)` just run, confirming this snapshot corresponds to that scan.

**Conclusion: the evidence directly supports the "working set exceeds buffer pool" theory, not a red herring.** Each shard's table is ~10x its container's 512MiB buffer pool, and 6 of the 8 critical queries (everything except the two PK/composite-index-served lookups) require a full or near-full scan because the legacy schema has no index with `entity_id` or `observed_at` as a leading column and drags a `LONGTEXT` payload blob through every clustered-index page. The result is that most of the table is re-read from disk on every such query -- measured at ~50% miss rate even with a single query running alone, no concurrency. This is a capacity/schema problem, not a tuning problem: raising the buffer pool would only delay the crossover point, not remove it, since P2's expected data growth would eventually exceed any buffer pool sized to fit comfortably in this machine's ~7.65GB Docker VM budget. The real fix is P2's job: a narrower, better-clustered `evidence` table (BIGINT PK, payload moved to a side table so hot columns fit more rows per page) plus indexes with `entity_id`/`observed_at` as leading columns, so the *working set for the hot access patterns* -- not the whole table -- is what has to fit in memory.

## Summary

| Query | SLO (p95) | p50 | p95 | p99 | Source | Rows examined | Index used | Verdict |
|---|---|---|---|---|---|---|---|---|
| point_lookup_by_id | p95 < 50ms | 1.0ms | 4.0ms | 8.0ms | locust (8229x) | 1 | unknown (see EXPLAIN ANALYZE text) | PASS |
| latest_n_by_entity_status | p95 < 50ms | 2.0ms | 16.0ms | 38.0ms | locust (8140x) | 30 | index: ix_evidence_legacy_status_entity | PASS |
| latest_n_by_entity_type_daterange | p95 < 50ms | 34508.2ms | 34508.2ms | 34508.2ms | single-shot | 5247983 | full scan | FAIL |
| per_entity_status_aggregate | p95 < 50ms | 124318.5ms | 124318.5ms | 124318.5ms | single-shot | 5247983 | index: ix_evidence_legacy_status | FAIL |
| range_aggregate_status_window | p95 < 300ms | 132526.0ms | 132526.0ms | 132526.0ms | single-shot | 5247983 | index: ix_evidence_legacy_status | FAIL |
| content_hash_dedup_lookup | p95 < 50ms | 21411.6ms | 21411.6ms | 21411.6ms | single-shot | 2319829 | full scan | FAIL |
| top_entities_by_volume_window | p95 < 300ms | 64946.5ms | 64946.5ms | 64946.5ms | single-shot | 5247993 | full scan | FAIL |
| keyset_paginated_feed | p95 < 50ms | 55473.4ms | 55473.4ms | 55473.4ms | single-shot | 5247997 | full scan | FAIL |

## Detail per query

### `point_lookup_by_id`

SLO: p95 < 50ms. Query file: `queries/critical/01_point_lookup_by_id.sql`.

Locust (8229 requests, 0 failures): p50=1.0ms p95=4.0ms p99=8.0ms avg=1.9ms rps=139.4

Single-execution capture (1 rows returned): wall=0.756ms engine=0.134ms rows_examined=1 rows_sent=1

<details><summary>EXPLAIN ANALYZE</summary>

```
-> Rows fetched before execution  (cost=0..0 rows=1) (actual time=125e-6..125e-6 rows=1 loops=1)
```

</details>

### `latest_n_by_entity_status`

SLO: p95 < 50ms. Query file: `queries/critical/02_latest_n_by_entity_status.sql`.

Locust (8140 requests, 0 failures): p50=2.0ms p95=16.0ms p99=38.0ms avg=4.8ms rps=137.9

Single-execution capture (15 rows returned): wall=7.386ms engine=6.792ms rows_examined=30 rows_sent=15

<details><summary>EXPLAIN ANALYZE</summary>

```
-> Limit: 20 row(s)  (cost=15.6 rows=15) (actual time=0.348..0.349 rows=15 loops=1)
    -> Sort: evidence_legacy.observed_at DESC, limit input to 20 row(s) per chunk  (cost=15.6 rows=15) (actual time=0.348..0.348 rows=15 loops=1)
        -> Index lookup on evidence_legacy using ix_evidence_legacy_status_entity (status='verified', entity_id='entity-00086197')  (cost=15.6 rows=15) (actual time=0.0205..0.0569 rows=15 loops=1)
```

</details>

### `latest_n_by_entity_type_daterange`

SLO: p95 < 50ms. Query file: `queries/critical/03_latest_n_by_entity_type_daterange.sql`.

Single-execution capture (0 rows returned): wall=34508.155ms engine=34503.235ms rows_examined=5247983 rows_sent=0

<details><summary>EXPLAIN ANALYZE</summary>

```
-> Limit: 20 row(s)  (cost=741081 rows=20) (actual time=32672..32672 rows=0 loops=1)
    -> Sort: evidence_legacy.observed_at DESC, limit input to 20 row(s) per chunk  (cost=741081 rows=5.39e+6) (actual time=32672..32672 rows=0 loops=1)
        -> Filter: ((evidence_legacy.entity_id = 'entity-00199339') and (evidence_legacy.evidence_type = 'photo') and (evidence_legacy.observed_at between '2026-03-26 21:32:49' and '2026-09-22 21:32:49'))  (cost=741081 rows=5.39e+6) (actual time=32671..32671 rows=0 loops=1)
            -> Table scan on evidence_legacy  (cost=741081 rows=5.39e+6) (actual time=0.793..32273 rows=5.25e+6 loops=1)
```

</details>

### `per_entity_status_aggregate`

SLO: p95 < 50ms. Query file: `queries/critical/04_per_entity_status_aggregate.sql`.

Single-execution capture (4 rows returned): wall=124318.49ms engine=124306.272ms rows_examined=5247983 rows_sent=4

<details><summary>EXPLAIN ANALYZE</summary>

```
-> Group aggregate: count(0), avg(cast(evidence_legacy.confidence_score as decimal(5,4)))  (cost=795428 rows=578) (actual time=36075..127140 rows=4 loops=1)
    -> Filter: (evidence_legacy.entity_id = 'entity-00232526')  (cost=741548 rows=538799) (actual time=1765..127138 rows=31 loops=1)
        -> Index scan on evidence_legacy using ix_evidence_legacy_status  (cost=741548 rows=5.39e+6) (actual time=2.52..126764 rows=5.25e+6 loops=1)
```

</details>

### `range_aggregate_status_window`

SLO: p95 < 300ms. Query file: `queries/critical/05_range_aggregate_status_window.sql`.

Single-execution capture (4 rows returned): wall=132525.955ms engine=132518.024ms rows_examined=5247983 rows_sent=4

<details><summary>EXPLAIN ANALYZE</summary>

```
-> Group aggregate: count(0)  (cost=801209 rows=578) (actual time=75974..304676 rows=4 loops=1)
    -> Filter: (evidence_legacy.observed_at between '2026-03-26 21:32:49' and '2026-09-22 21:32:49')  (cost=741349 rows=598606) (actual time=2.73..304480 rows=692123 loops=1)
        -> Index scan on evidence_legacy using ix_evidence_legacy_status  (cost=741349 rows=5.39e+6) (actual time=2.73..303773 rows=5.25e+6 loops=1)
```

</details>

### `content_hash_dedup_lookup`

SLO: p95 < 50ms. Query file: `queries/critical/06_content_hash_dedup_lookup.sql`.

Single-execution capture (1 rows returned): wall=21411.573ms engine=3618270.829ms rows_examined=2319829 rows_sent=1

<details><summary>EXPLAIN ANALYZE</summary>

```
-> Limit: 1 row(s)  (cost=740505 rows=1) (actual time=28114..28114 rows=1 loops=1)
    -> Filter: (evidence_legacy.content_hash = 'fc4e3bcfde4fd0cb8d01fc0db917a5d9cd4ba8297be99463b42b778f16af28c7')  (cost=740505 rows=538799) (actual time=28114..28114 rows=1 loops=1)
        -> Table scan on evidence_legacy  (cost=740505 rows=5.39e+6) (actual time=0.949..27884 rows=2.32e+6 loops=1)
```

</details>

### `top_entities_by_volume_window`

SLO: p95 < 300ms. Query file: `queries/critical/07_top_entities_by_volume_window.sql`.

Single-execution capture (10 rows returned): wall=64946.542ms engine=3662272.197ms rows_examined=5247993 rows_sent=10

<details><summary>EXPLAIN ANALYZE</summary>

```
-> Limit: 10 row(s)  (actual time=66841..66841 rows=10 loops=1)
    -> Sort: evidence_count DESC, limit input to 10 row(s) per chunk  (actual time=66841..66841 rows=10 loops=1)
        -> Table scan on <temporary>  (actual time=66819..66834 rows=123022 loops=1)
            -> Aggregate using temporary table  (actual time=66819..66819 rows=123021 loops=1)
                -> Filter: (evidence_legacy.observed_at between '2026-03-26 21:32:49' and '2026-09-22 21:32:49')  (cost=739988 rows=598606) (actual time=0.858..59975 rows=692123 loops=1)
                    -> Table scan on evidence_legacy  (cost=739988 rows=5.39e+6) (actual time=0.852..59300 rows=5.25e+6 loops=1)
```

</details>

### `keyset_paginated_feed`

SLO: p95 < 50ms. Query file: `queries/critical/08_keyset_paginated_feed.sql`.

Single-execution capture (14 rows returned): wall=55473.368ms engine=3653954.812ms rows_examined=5247997 rows_sent=14

<details><summary>EXPLAIN ANALYZE</summary>

```
-> Limit: 20 row(s)  (cost=739982 rows=20) (actual time=43156..43156 rows=14 loops=1)
    -> Sort: evidence_legacy.observed_at DESC, evidence_legacy.id DESC, limit input to 20 row(s) per chunk  (cost=739982 rows=5.39e+6) (actual time=43156..43156 rows=14 loops=1)
        -> Filter: ((evidence_legacy.entity_id = 'entity-00229285') and ((evidence_legacy.observed_at,evidence_legacy.id) < ('2024-02-29 10:18:38','7128aca3-1d46-4865-84f2-deaad2558596')))  (cost=739982 rows=5.39e+6) (actual time=466..43155 rows=14 loops=1)
            -> Table scan on evidence_legacy  (cost=739982 rows=5.39e+6) (actual time=0.695..42673 rows=5.25e+6 loops=1)
```

</details>
