"""Restrict zone deletion when seats exist

Revision ID: 0002_restrict_seat_zone_delete
Revises: 0001_initial_schema
Create Date: 2026-11-27 10:05:00.000000

"""

from typing import Sequence, Union
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0002_restrict_seat_zone_delete"
down_revision: Union[str, None] = "0001_initial_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("fk_seats_zones_event_zone", "seats", type_="foreignkey")
    op.create_foreign_key(
        "fk_seats_zones_event_zone",
        "seats",
        "zones",
        ["event_id", "zone_code"],
        ["event_id", "zone_code"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint("fk_seats_zones_event_zone", "seats", type_="foreignkey")
    op.create_foreign_key(
        "fk_seats_zones_event_zone",
        "seats",
        "zones",
        ["event_id", "zone_code"],
        ["event_id", "zone_code"],
        ondelete="CASCADE",
    )
