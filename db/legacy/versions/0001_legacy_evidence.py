"""Deliberately bad legacy evidence table: baseline for P1/P2 comparison.

Revision ID: 0001_legacy_evidence
Revises:
Create Date: 2026-09-22
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "0001_legacy_evidence"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "evidence_legacy",
        # varchar(36) UUID PK instead of BIGINT: fragments the InnoDB
        # clustered index and bloats every secondary index (each carries a
        # copy of the PK).
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("entity_id", sa.String(36), nullable=False),
        # Denormalized attributes on a wide table instead of a dim_entity /
        # dim_source side table.
        sa.Column("entity_name", sa.String(255), nullable=True),
        sa.Column("source_name", sa.String(255), nullable=False),
        sa.Column("source_type", sa.String(50), nullable=False),
        sa.Column("evidence_type", sa.String(100), nullable=False),
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        # Wrong types: should be DECIMAL(5,4) and DATETIME(6).
        sa.Column("confidence_score", sa.String(20), nullable=True),
        sa.Column("observed_at", sa.String(30), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        # JSON/TEXT blob instead of a normalized payload side table.
        sa.Column("payload_blob", mysql.LONGTEXT(), nullable=True),
        sa.Column("reviewer_notes", sa.Text(), nullable=True),
        # Wrong type: boolean stored as a string instead of TINYINT(1).
        sa.Column("is_deleted", sa.String(5), nullable=False, server_default="false"),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
        ),
        # Wrong charset: utf8 (utf8mb3, 3-byte max) instead of utf8mb4 --
        # cannot store 4-byte characters (e.g. emoji) without silent
        # truncation/errors. Contrast with the optimized schema in P2.
        mysql_engine="InnoDB",
        mysql_charset="utf8",
        mysql_collate="utf8_general_ci",
    )

    # Redundant/overlapping indexes: idx_status is a strict leftmost prefix
    # of idx_status_entity, so it never gets chosen by the optimizer once
    # the composite index exists -- pure write-amplification and storage
    # cost with zero read benefit.
    op.create_index("ix_evidence_legacy_status", "evidence_legacy", ["status"])
    op.create_index("ix_evidence_legacy_status_entity", "evidence_legacy", ["status", "entity_id"])
    # Indexing a low-cardinality denormalized display column that is never
    # used as a filter in the critical queries.
    op.create_index("ix_evidence_legacy_entity_name", "evidence_legacy", ["entity_name"])
    # Deliberately missing: an index with entity_id as the LEADING column
    # (the actual mission-critical "latest-N for entity" access pattern),
    # an index on observed_at for range queries, and a unique index on
    # content_hash for dedup lookups. See docs/adr/0001-legacy-schema-baseline.md.


def downgrade() -> None:
    op.drop_table("evidence_legacy")
