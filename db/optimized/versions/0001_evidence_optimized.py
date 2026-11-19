"""Optimized evidence schema: partitioned fact table, dimensions, payload side table.

Revision ID: 0001_evidence_optimized
Revises:
Create Date: 2026-11-19

Additive only: never touches `evidence_legacy`.

Status codes (TINYINT): 1=pending, 2=under_review, 3=verified, 4=rejected.
Partitioning: RANGE COLUMNS(observed_at), monthly. PK is (id, observed_at) because
MySQL requires the partition column in every unique key.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0001_evidence_optimized"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

FIRST_MONTH = (2023, 1)
LAST_MONTH = (2027, 12)  # inclusive; pmax catches anything later


def _partition_clause() -> str:
    parts: list[str] = []
    year, month = FIRST_MONTH
    while (year, month) <= LAST_MONTH:
        ny, nm = (year + 1, 1) if month == 12 else (year, month + 1)
        parts.append(f"PARTITION p{year}{month:02d} VALUES LESS THAN ('{ny}-{nm:02d}-01 00:00:00')")
        year, month = ny, nm
    parts.append("PARTITION pmax VALUES LESS THAN (MAXVALUE)")
    return ",\n  ".join(parts)


_TAIL = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"


def upgrade() -> None:
    op.execute(
        f"""CREATE TABLE dim_entity (
  entity_pk BIGINT UNSIGNED NOT NULL,
  entity_key VARCHAR(36) NOT NULL,
  name VARCHAR(255) NULL,
  PRIMARY KEY (entity_pk),
  UNIQUE KEY uq_entity_key (entity_key)
) {_TAIL}"""
    )
    op.execute(
        f"""CREATE TABLE dim_source (
  source_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  source_type VARCHAR(50) NOT NULL,
  PRIMARY KEY (source_id),
  UNIQUE KEY uq_source (name, source_type)
) {_TAIL}"""
    )
    op.execute(
        f"""CREATE TABLE dim_evidence_type (
  type_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  PRIMARY KEY (type_id),
  UNIQUE KEY uq_type (name)
) {_TAIL}"""
    )
    op.execute(
        f"""CREATE TABLE evidence (
  id BIGINT UNSIGNED NOT NULL,
  observed_at DATETIME NOT NULL,
  entity_pk BIGINT UNSIGNED NOT NULL,
  source_id INT UNSIGNED NOT NULL,
  type_id SMALLINT UNSIGNED NOT NULL,
  status TINYINT UNSIGNED NOT NULL,
  confidence_score DECIMAL(5,4) NULL,
  is_deleted TINYINT(1) NOT NULL DEFAULT 0,
  content_hash BINARY(32) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id, observed_at),
  KEY ix_entity_obs (entity_pk, observed_at, id),
  KEY ix_entity_stat (entity_pk, status, observed_at, id, confidence_score),
  KEY ix_entity_type (entity_pk, type_id, observed_at, id),
  KEY ix_obs_status (observed_at, status),
  KEY ix_obs_entity (observed_at, entity_pk)
) {_TAIL}
PARTITION BY RANGE COLUMNS(observed_at) (
  {_partition_clause()}
)"""
    )
    op.execute(
        f"""CREATE TABLE evidence_payload (
  id BIGINT UNSIGNED NOT NULL,
  observed_at DATETIME NOT NULL,
  payload JSON NULL,
  reviewer_notes TEXT NULL,
  PRIMARY KEY (id)
) ROW_FORMAT=DYNAMIC {_TAIL}"""
    )
    op.execute(
        f"""CREATE TABLE evidence_content_hash (
  content_hash BINARY(32) NOT NULL,
  evidence_id BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (content_hash)
) {_TAIL}"""
    )
    op.execute(
        f"""CREATE TABLE backfill_progress (
  job VARCHAR(64) NOT NULL,
  last_legacy_id VARCHAR(36) NOT NULL,
  rows_scanned BIGINT NOT NULL,
  rows_written BIGINT NOT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (job)
) {_TAIL}"""
    )


def downgrade() -> None:
    # Destroys optimized data: only run deliberately, never as part of a sample undo
    # without explicit sign-off. evidence_legacy is never touched.
    for table in (
        "backfill_progress",
        "evidence_content_hash",
        "evidence_payload",
        "evidence",
        "dim_evidence_type",
        "dim_source",
        "dim_entity",
    ):
        op.execute(f"DROP TABLE IF EXISTS {table}")
