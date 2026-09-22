-- name: point_lookup_by_id
-- slo: p95 < 50ms
-- shape: point lookup
-- params: :id (evidence_legacy.id, varchar(36))
-- Single-row lookup by primary key. On the legacy schema the PK is a
-- random varchar(36) UUID, so even a PK hit pays for a wide, poorly
-- clustered B-tree walk instead of a tight BIGINT range scan.
SELECT
    id,
    entity_id,
    source_name,
    evidence_type,
    status,
    confidence_score,
    observed_at,
    content_hash
FROM evidence_legacy
WHERE id = :id;
