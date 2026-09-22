-- name: content_hash_dedup_lookup
-- slo: p95 < 50ms
-- shape: point lookup (dedup)
-- params: :content_hash
-- Ingestion-time dedup check ("have we already seen this exact
-- content?"). content_hash has no unique index (or any index) on the
-- legacy schema, so every single ingested row pays for a full scan
-- before it can be accepted -- this is the query most directly
-- responsible for capping legacy ingest throughput.
SELECT id
FROM evidence_legacy
WHERE content_hash = :content_hash
LIMIT 1;
