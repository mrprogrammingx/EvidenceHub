-- name: latest_n_by_entity_status
-- slo: p95 < 50ms
-- shape: latest-N for entity, filtered by status
-- params: :entity_id, :status, :limit_n
-- The single most common Evidence read: an investigator's "show me the
-- latest N pieces of evidence for this case, filtered by status" screen.
-- The legacy schema has no index with entity_id as the leading column
-- (ix_evidence_legacy_status_entity puts status first), so this falls
-- back to a full table scan filtered in memory.
SELECT
    id,
    entity_id,
    evidence_type,
    status,
    confidence_score,
    observed_at
FROM evidence_legacy
WHERE entity_id = :entity_id
  AND status = :status
ORDER BY observed_at DESC
LIMIT :limit_n;
