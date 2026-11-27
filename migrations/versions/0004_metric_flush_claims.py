"""Add metric_flush_claims table for exactly-once metric bucket persistence

Revision ID: 0004_metric_flush_claims
Revises: 0003_import_job_cancelled
Create Date: 2026-11-27 16:05:00.000000

"""

from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0004_metric_flush_claims"
down_revision: Union[str, None] = "0003_import_job_cancelled"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "metric_flush_claims",
        sa.Column(
            "claim_id",
            sa.String(length=64),
            nullable=False,
            comment="Redis inflight 認領冪等鍵 (<ts>:<claim_id>)",
        ),
        sa.Column(
            "bucket_timestamp",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="對應之秒級桶時間戳",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("claim_id"),
    )
    op.create_index(
        op.f("ix_metric_flush_claims_bucket_timestamp"),
        "metric_flush_claims",
        ["bucket_timestamp"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_metric_flush_claims_bucket_timestamp"),
        table_name="metric_flush_claims",
    )
    op.drop_table("metric_flush_claims")
