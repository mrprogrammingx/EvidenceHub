"""Dual-write reconciliation outbox.

Revision ID: 0002_dualwrite_outbox
Revises: 0001_evidence_optimized
Create Date: 2026-11-19

When the legacy write succeeds but the optimized-side write fails, the dual-writer
records the legacy id here instead of failing the request. A reconciler (or a re-run
of the backfill over that row) replays it. Additive only.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002_dualwrite_outbox"
down_revision: str | None = "0001_evidence_optimized"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """CREATE TABLE dualwrite_outbox (
  outbox_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  legacy_id VARCHAR(36) NOT NULL,
  error VARCHAR(1024) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  resolved_at DATETIME NULL,
  PRIMARY KEY (outbox_id),
  KEY ix_unresolved (resolved_at, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS dualwrite_outbox")
