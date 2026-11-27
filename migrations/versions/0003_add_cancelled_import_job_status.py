"""Add cancelled status to import_jobs check constraint

Revision ID: 0003_add_cancelled_import_job_status
Revises: 0002_restrict_seat_zone_delete
Create Date: 2026-11-27 11:40:00.000000

"""

from typing import Sequence, Union
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0003_import_job_cancelled"
down_revision: Union[str, None] = "0002_restrict_seat_zone_delete"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("ck_import_jobs_status", "import_jobs", type_="check")
    op.create_check_constraint(
        "ck_import_jobs_status",
        "import_jobs",
        "status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')",
    )


def downgrade() -> None:
    op.execute("UPDATE import_jobs SET status = 'failed' WHERE status = 'cancelled'")
    op.drop_constraint("ck_import_jobs_status", "import_jobs", type_="check")
    op.create_check_constraint(
        "ck_import_jobs_status",
        "import_jobs",
        "status IN ('pending', 'processing', 'completed', 'failed')",
    )
