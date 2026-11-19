"""Month-order backfill checkpoint: resume cursor and completion marker.

Revision ID: 0003_backfill_month_cursor
Revises: 0002_dualwrite_outbox
Create Date: 2026-11-19

A month job streams legacy rows ordered by (observed_at, id). The checkpoint stores the
last committed (observed_at, legacy id) pair, so a restart resumes after it, and
completed_at, so a finished month is skipped without rescanning legacy. Additive only.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0003_backfill_month_cursor"
down_revision: str | None = "0002_dualwrite_outbox"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE backfill_progress "
        "ADD COLUMN cursor_observed_at VARCHAR(19) NULL AFTER last_legacy_id, "
        "ADD COLUMN completed_at DATETIME NULL AFTER rows_written"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE backfill_progress DROP COLUMN cursor_observed_at, DROP COLUMN completed_at"
    )
