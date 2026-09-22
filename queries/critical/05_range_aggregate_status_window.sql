-- name: range_aggregate_status_window
-- slo: p95 < 300ms
-- shape: range aggregate (cross-entity)
-- params: :from_ts, :to_ts
-- Ops dashboard widget: evidence volume by status across ALL entities
-- in a date window. This one is allowed to cost more (300ms) because
-- it is a genuine cross-entity scan -- but on the legacy schema it is
-- an unbounded full scan with no partition pruning available at all,
-- since there is no partitioning and no index on observed_at.
SELECT
    status,
    COUNT(*) AS evidence_count
FROM evidence_legacy
WHERE observed_at BETWEEN :from_ts AND :to_ts
GROUP BY status;
