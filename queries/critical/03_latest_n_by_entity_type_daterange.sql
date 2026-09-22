-- name: latest_n_by_entity_type_daterange
-- slo: p95 < 50ms
-- shape: latest-N for entity, filtered by type + date range
-- params: :entity_id, :evidence_type, :from_ts, :to_ts, :limit_n
-- observed_at is stored as VARCHAR(30) in "YYYY-MM-DD HH:MM:SS" format,
-- which happens to sort correctly lexicographically, so the range
-- predicate is at least *correct* -- but with no index on entity_id or
-- observed_at it can't be *fast*: full scan plus filesort.
SELECT
    id,
    entity_id,
    evidence_type,
    status,
    confidence_score,
    observed_at
FROM evidence_legacy
WHERE entity_id = :entity_id
  AND evidence_type = :evidence_type
  AND observed_at BETWEEN :from_ts AND :to_ts
ORDER BY observed_at DESC
LIMIT :limit_n;
