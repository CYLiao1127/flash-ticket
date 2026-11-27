"""Initial schema with constraints and partial unique index

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-11-27 00:00:00.000000

"""

from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
  # 1. users table
  op.create_table(
      "users",
      sa.Column("id", sa.Integer(), nullable=False),
      sa.Column("email", sa.String(length=255), nullable=False),
      sa.Column("hashed_password", sa.String(length=255), nullable=False),
      sa.Column(
          "role",
          sa.String(length=32),
          nullable=False,
          server_default="user",
      ),
      sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
      sa.Column(
          "token_version", sa.Integer(), nullable=False, server_default="1"
      ),
      sa.Column(
          "created_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.Column(
          "updated_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          server_onupdate=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.PrimaryKeyConstraint("id"),
      sa.CheckConstraint(
          "role IN ('admin', 'user', 'viewer')", name="ck_users_role"
      ),
  )
  op.create_index(op.f("ix_users_id"), "users", ["id"], unique=False)
  op.create_index(op.f("ix_users_email"), "users", ["email"], unique=True)

  # 2. events table
  op.create_table(
      "events",
      sa.Column("id", sa.Integer(), nullable=False),
      sa.Column("creator_id", sa.Integer(), nullable=False),
      sa.Column("title", sa.String(length=255), nullable=False),
      sa.Column("description", sa.Text(), nullable=True),
      sa.Column("category", sa.String(length=100), nullable=False),
      sa.Column("venue", sa.String(length=255), nullable=False),
      sa.Column("start_time", sa.DateTime(timezone=True), nullable=False),
      sa.Column("end_time", sa.DateTime(timezone=True), nullable=False),
      sa.Column(
          "status",
          sa.String(length=32),
          nullable=False,
          server_default="draft",
      ),
      sa.Column(
          "last_seq", sa.BigInteger(), nullable=False, server_default="0"
      ),
      sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
      sa.Column(
          "created_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.Column(
          "updated_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          server_onupdate=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.ForeignKeyConstraint(["creator_id"], ["users.id"], ondelete="RESTRICT"),
      sa.PrimaryKeyConstraint("id"),
      sa.CheckConstraint(
          "status IN ('draft', 'published')", name="ck_events_status"
      ),
  )
  op.create_index(op.f("ix_events_id"), "events", ["id"], unique=False)
  op.create_index(
      op.f("ix_events_category"), "events", ["category"], unique=False
  )
  op.create_index(op.f("ix_events_status"), "events", ["status"], unique=False)

  # 3. zones table
  op.create_table(
      "zones",
      sa.Column("id", sa.Integer(), nullable=False),
      sa.Column("event_id", sa.Integer(), nullable=False),
      sa.Column("zone_code", sa.String(length=50), nullable=False),
      sa.Column("name", sa.String(length=100), nullable=False),
      sa.Column("price", sa.Numeric(precision=10, scale=2), nullable=False),
      sa.Column(
          "created_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="CASCADE"),
      sa.PrimaryKeyConstraint("id"),
      sa.UniqueConstraint(
          "event_id", "zone_code", name="uq_event_zone_code"
      ),
  )
  op.create_index(op.f("ix_zones_id"), "zones", ["id"], unique=False)

  # 4. seats table
  op.create_table(
      "seats",
      sa.Column("id", sa.BigInteger(), nullable=False),
      sa.Column("event_id", sa.Integer(), nullable=False),
      sa.Column("zone_code", sa.String(length=50), nullable=False),
      sa.Column("row_label", sa.String(length=50), nullable=False),
      sa.Column("seat_number", sa.String(length=50), nullable=False),
      sa.Column(
          "status",
          sa.String(length=32),
          nullable=False,
          server_default="available",
      ),
      sa.Column("held_by", sa.Integer(), nullable=True),
      sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
      sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
      sa.Column(
          "created_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.Column(
          "updated_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          server_onupdate=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="CASCADE"),
      sa.ForeignKeyConstraint(["held_by"], ["users.id"], ondelete="SET NULL"),
      # Composite Foreign Key to zones (event_id, zone_code)
      sa.ForeignKeyConstraint(
          ["event_id", "zone_code"],
          ["zones.event_id", "zones.zone_code"],
          ondelete="CASCADE",
          name="fk_seats_zones_event_zone",
      ),
      sa.PrimaryKeyConstraint("id"),
      sa.CheckConstraint(
          "status IN ('available', 'held', 'sold')", name="ck_seats_status"
      ),
      sa.UniqueConstraint(
          "event_id",
          "zone_code",
          "row_label",
          "seat_number",
          name="uq_event_seat_location",
      ),
  )
  op.create_index(op.f("ix_seats_id"), "seats", ["id"], unique=False)
  op.create_index(
      "ix_seats_allocation",
      "seats",
      ["event_id", "zone_code", "status", "row_label", "seat_number"],
      unique=False,
  )
  op.create_index(
      "ix_seats_held_expiry", "seats", ["status", "expires_at"], unique=False
  )

  # 5. orders table
  op.create_table(
      "orders",
      sa.Column("id", sa.BigInteger(), nullable=False),
      sa.Column("user_id", sa.Integer(), nullable=False),
      sa.Column("event_id", sa.Integer(), nullable=False),
      sa.Column(
          "total_ticket_amount", sa.Numeric(precision=12, scale=2), nullable=False
      ),
      sa.Column(
          "service_fee", sa.Numeric(precision=12, scale=2), nullable=False
      ),
      sa.Column(
          "total_amount", sa.Numeric(precision=12, scale=2), nullable=False
      ),
      sa.Column(
          "status",
          sa.String(length=32),
          nullable=False,
          server_default="pending",
      ),
      sa.Column("idempotency_key", sa.String(length=128), nullable=True),
      sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
      sa.Column(
          "created_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.Column(
          "updated_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          server_onupdate=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="RESTRICT"),
      sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="RESTRICT"),
      sa.PrimaryKeyConstraint("id"),
      sa.CheckConstraint(
          "status IN ('pending', 'paid', 'cancelled',"
          " 'cancelled_refund_required')",
          name="ck_orders_status",
      ),
      sa.UniqueConstraint("idempotency_key", name="uq_orders_idempotency_key"),
  )
  op.create_index(op.f("ix_orders_id"), "orders", ["id"], unique=False)
  op.create_index(op.f("ix_orders_status"), "orders", ["status"], unique=False)
  # Index 1: User order history optimization
  op.create_index(
      "ix_orders_user_created", "orders", ["user_id", "created_at"], unique=False
  )
  # Index 2: Partial Index for cleanup worker scanning expired pending orders
  op.create_index(
      "ix_orders_pending_expiry",
      "orders",
      ["expires_at"],
      unique=False,
      postgresql_where=sa.text("status = 'pending'"),
  )

  # 6. order_items table with Partial Unique Index & Composite UK
  op.create_table(
      "order_items",
      sa.Column("id", sa.BigInteger(), nullable=False),
      sa.Column("order_id", sa.BigInteger(), nullable=False),
      sa.Column("seat_id", sa.BigInteger(), nullable=False),
      sa.Column("price", sa.Numeric(precision=10, scale=2), nullable=False),
      sa.Column(
          "status",
          sa.String(length=32),
          nullable=False,
          server_default="held",
      ),
      sa.Column(
          "created_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.ForeignKeyConstraint(["order_id"], ["orders.id"], ondelete="CASCADE"),
      sa.ForeignKeyConstraint(["seat_id"], ["seats.id"], ondelete="RESTRICT"),
      sa.PrimaryKeyConstraint("id"),
      sa.CheckConstraint(
          "status IN ('held', 'paid', 'cancelled')", name="ck_order_items_status"
      ),
      sa.UniqueConstraint("order_id", "seat_id", name="uq_order_item_seat"),
  )
  op.create_index(op.f("ix_order_items_id"), "order_items", ["id"], unique=False)
  op.create_index(
      "ix_order_items_order_id", "order_items", ["order_id"], unique=False
  )

  # PARTIAL UNIQUE INDEX: Crucial second line of defense against overselling!
  op.create_index(
      "uq_active_seat_order",
      "order_items",
      ["seat_id"],
      unique=True,
      postgresql_where=sa.text("status IN ('held', 'paid')"),
  )

  # 7. event_outbox table
  op.create_table(
      "event_outbox",
      sa.Column("id", sa.BigInteger(), nullable=False),
      sa.Column("event_id", sa.Integer(), nullable=False),
      sa.Column("sequence_id", sa.BigInteger(), nullable=False),
      sa.Column("event_type", sa.String(length=100), nullable=False),
      sa.Column(
          "payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False
      ),
      sa.Column(
          "status",
          sa.String(length=32),
          nullable=False,
          server_default="pending",
      ),
      sa.Column(
          "created_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="CASCADE"),
      sa.PrimaryKeyConstraint("id"),
      sa.CheckConstraint(
          "status IN ('pending', 'published')", name="ck_event_outbox_status"
      ),
      sa.UniqueConstraint(
          "event_id", "sequence_id", name="uq_event_outbox_sequence"
      ),
  )
  op.create_index(
      op.f("ix_event_outbox_id"), "event_outbox", ["id"], unique=False
  )
  op.create_index(
      op.f("ix_event_outbox_status"), "event_outbox", ["status"], unique=False
  )
  # PARTIAL INDEX: Only index pending outbox events for optimal relay polling speed!
  op.create_index(
      "ix_outbox_pending_relay",
      "event_outbox",
      ["created_at"],
      unique=False,
      postgresql_where=sa.text("status = 'pending'"),
  )

  # 8. import_jobs table
  op.create_table(
      "import_jobs",
      sa.Column("id", sa.BigInteger(), nullable=False),
      sa.Column("event_id", sa.Integer(), nullable=False),
      sa.Column("user_id", sa.Integer(), nullable=False),
      sa.Column("file_sha256", sa.String(length=64), nullable=False),
      sa.Column("filename", sa.String(length=255), nullable=False),
      sa.Column(
          "total_rows", sa.Integer(), nullable=False, server_default="0"
      ),
      sa.Column(
          "imported_rows", sa.Integer(), nullable=False, server_default="0"
      ),
      sa.Column(
          "error_rows", sa.Integer(), nullable=False, server_default="0"
      ),
      sa.Column(
          "mode",
          sa.String(length=32),
          nullable=False,
          server_default="all_or_nothing",
      ),
      sa.Column(
          "status",
          sa.String(length=32),
          nullable=False,
          server_default="pending",
      ),
      sa.Column(
          "errors",
          postgresql.JSONB(astext_type=sa.Text()),
          nullable=False,
          server_default="[]",
      ),
      sa.Column(
          "created_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.Column(
          "updated_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          server_onupdate=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="CASCADE"),
      sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="RESTRICT"),
      sa.PrimaryKeyConstraint("id"),
      sa.CheckConstraint(
          "mode IN ('all_or_nothing', 'skip_invalid')",
          name="ck_import_jobs_mode",
      ),
      sa.CheckConstraint(
          "status IN ('pending', 'processing', 'completed', 'failed')",
          name="ck_import_jobs_status",
      ),
      sa.UniqueConstraint(
          "event_id", "file_sha256", name="uq_event_file_sha256_job"
      ),
  )
  op.create_index(
      op.f("ix_import_jobs_id"), "import_jobs", ["id"], unique=False
  )

  # 9. access_logs table
  op.create_table(
      "access_logs",
      sa.Column("id", sa.BigInteger(), nullable=False),
      sa.Column("request_id", sa.String(length=64), nullable=False),
      sa.Column("user_id", sa.Integer(), nullable=True),
      sa.Column("method", sa.String(length=10), nullable=False),
      sa.Column("path", sa.String(length=255), nullable=False),
      sa.Column("status_code", sa.Integer(), nullable=False),
      sa.Column(
          "latency_ms", sa.Numeric(precision=10, scale=2), nullable=False
      ),
      sa.Column("client_ip", sa.String(length=45), nullable=True),
      sa.Column(
          "created_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.PrimaryKeyConstraint("id"),
  )
  op.create_index(
      op.f("ix_access_logs_id"), "access_logs", ["id"], unique=False
  )
  op.create_index(
      op.f("ix_access_logs_request_id"),
      "access_logs",
      ["request_id"],
      unique=False,
  )
  op.create_index(
      op.f("ix_access_logs_created_at"),
      "access_logs",
      ["created_at"],
      unique=False,
  )

  # 10. metrics_per_second table
  op.create_table(
      "metrics_per_second",
      sa.Column("id", sa.BigInteger(), nullable=False),
      sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
      sa.Column(
          "total_requests", sa.Integer(), nullable=False, server_default="0"
      ),
      sa.Column(
          "error_requests", sa.Integer(), nullable=False, server_default="0"
      ),
      sa.Column(
          "p50_latency_ms",
          sa.Numeric(precision=10, scale=2),
          nullable=False,
          server_default="0",
      ),
      sa.Column(
          "p95_latency_ms",
          sa.Numeric(precision=10, scale=2),
          nullable=False,
          server_default="0",
      ),
      sa.Column(
          "p99_latency_ms",
          sa.Numeric(precision=10, scale=2),
          nullable=False,
          server_default="0",
      ),
      sa.Column(
          "active_connections",
          sa.Integer(),
          nullable=False,
          server_default="0",
      ),
      sa.Column(
          "created_at",
          sa.DateTime(timezone=True),
          server_default=sa.text("CURRENT_TIMESTAMP"),
          nullable=False,
      ),
      sa.PrimaryKeyConstraint("id"),
      sa.UniqueConstraint("timestamp", name="uq_metrics_timestamp"),
  )
  op.create_index(
      op.f("ix_metrics_per_second_id"),
      "metrics_per_second",
      ["id"],
      unique=False,
  )


def downgrade() -> None:
  op.drop_table("metrics_per_second")
  op.drop_table("access_logs")
  op.drop_table("import_jobs")
  op.drop_index("ix_outbox_pending_relay", table_name="event_outbox")
  op.drop_table("event_outbox")
  op.drop_index("uq_active_seat_order", table_name="order_items")
  op.drop_table("order_items")
  op.drop_index("ix_orders_pending_expiry", table_name="orders")
  op.drop_index("ix_orders_user_created", table_name="orders")
  op.drop_table("orders")
  op.drop_table("seats")
  op.drop_table("zones")
  op.drop_table("events")
  op.drop_table("users")
