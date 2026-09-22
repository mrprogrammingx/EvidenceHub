-- name: keyset_paginated_feed
-- slo: p95 < 50ms
-- shape: latest-N for entity (keyset pagination, page 2+)
-- params: :entity_id, :last_observed_at, :last_id, :limit_n
-- Keyset ("seek") pagination for one entity's evidence feed -- no
-- OFFSET, so cost shouldn't grow with page depth. On the legacy schema
-- there is no covering (entity_id, observed_at, id) index to seek
-- through, so every page still costs a full scan; P2 is expected to
-- make this genuinely O(limit_n) instead of O(table size).
SELECT
    id,
    entity_id,
    evidence_type,
    status,
    observed_at
FROM evidence_legacy
WHERE entity_id = :entity_id
  AND (observed_at, id) < (:last_observed_at, :last_id)
ORDER BY observed_at DESC, id DESC
LIMIT :limit_n;
