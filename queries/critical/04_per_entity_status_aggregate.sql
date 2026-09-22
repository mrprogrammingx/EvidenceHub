-- name: per_entity_status_aggregate
-- slo: p95 < 50ms
-- shape: per-entity aggregate
-- params: :entity_id
-- Case-summary widget: evidence counts and average confidence per
-- status, for one entity. Bounded by a single entity's row set, so it
-- should be cheap -- but only if the planner can seek to that entity's
-- rows instead of scanning the whole table to find them.
SELECT
    status,
    COUNT(*) AS evidence_count,
    AVG(CAST(confidence_score AS DECIMAL(5, 4))) AS avg_confidence
FROM evidence_legacy
WHERE entity_id = :entity_id
GROUP BY status;
