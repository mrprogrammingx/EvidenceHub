-- name: top_entities_by_volume_window
-- slo: p95 < 300ms
-- shape: range aggregate (top-N)
-- params: :from_ts, :to_ts, :limit_n
-- "Busiest cases this week" leaderboard. Cross-entity, so it shares the
-- range-aggregate SLO. Groups on the un-indexed entity_id after an
-- un-indexed observed_at range filter -- a worst case for the legacy
-- schema: full scan, temp table, filesort.
SELECT
    entity_id,
    COUNT(*) AS evidence_count
FROM evidence_legacy
WHERE observed_at BETWEEN :from_ts AND :to_ts
GROUP BY entity_id
ORDER BY evidence_count DESC
LIMIT :limit_n;
