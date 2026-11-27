"""資料庫 ORM 模型定義 (SQLAlchemy 2.0 Declarative Models)

本模組定義 flash-ticket 系統之 10 張核心資料表：
1. User: 使用者與角色（Admin, User, Viewer），支援 token_version 立即失效
2. Event: 活動主表，包含 last_seq (活動專屬單調遞增序號) 與 version (樂觀鎖)
3. Zone: 活動區域與票價設定，具備 (event_id, zone_code) 唯一限制
4. Seat: 座位庫存，包含 status (available/held/sold)、複合外鍵限制至 Zone、防重複匯入約束
5. Order: 訂單主表，包含 5% 服務費四捨五入計算、用戶訂單索引與 pending 部分索引
6. OrderItem: 訂單明細，掛載關鍵防線部分唯一索引 uq_active_seat_order 與 (order_id, seat_id) 唯一限制
7. EventOutbox: Transactional Outbox 表，保證座位狀態變更與推播事件之原子性，含 pending 部分索引
8. ImportJob: 批量匯入座位非同步任務，含 (event_id, file_sha256) 去重鍵
9. AccessLog: 全鏈路 HTTP 存取日誌，支援依 request_id 跨服務查詢
10. SystemMetric: 每秒監控指標 (RPS, 延遲百分位數, 錯誤率, 連線數)
"""

import enum
from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    Text,
    Boolean,
    DateTime,
    Numeric,
    ForeignKey,
    ForeignKeyConstraint,
    UniqueConstraint,
    CheckConstraint,
    Index,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from app.db import Base


# ==========================================
# Python 列舉型別 (業務層型別安全保障)
# ==========================================
class UserRole(str, enum.Enum):
  ADMIN = "admin"
  USER = "user"
  VIEWER = "viewer"


class EventStatus(str, enum.Enum):
  DRAFT = "draft"
  PUBLISHED = "published"


class SeatStatus(str, enum.Enum):
  AVAILABLE = "available"
  HELD = "held"
  SOLD = "sold"


class OrderStatus(str, enum.Enum):
  PENDING = "pending"
  PAID = "paid"
  CANCELLED = "cancelled"
  CANCELLED_REFUND_REQUIRED = "cancelled_refund_required"


class OrderItemStatus(str, enum.Enum):
  HELD = "held"
  PAID = "paid"
  CANCELLED = "cancelled"


class OutboxStatus(str, enum.Enum):
  PENDING = "pending"
  PUBLISHED = "published"


class ImportJobMode(str, enum.Enum):
  ALL_OR_NOTHING = "all_or_nothing"
  SKIP_INVALID = "skip_invalid"


class ImportJobStatus(str, enum.Enum):
  PENDING = "pending"
  PROCESSING = "processing"
  COMPLETED = "completed"
  FAILED = "failed"
  CANCELLED = "cancelled"


# ==========================================
# 1. 使用者模型 (User)
# ==========================================
class User(Base):
  """使用者模型

  - 支援 Admin, User, Viewer 三種角色。
  - token_version: 登出、停用或修改角色時遞增，使既有 JWT 立即作廢 (Q5.1)。
  - updated_at: 採用 DB server_onupdate 保證時鐘基準一致性。
  """

  __tablename__ = "users"

  id = Column(Integer, primary_key=True, index=True)
  email = Column(
      String(255),
      unique=True,
      nullable=False,
      index=True,
      comment="使用者帳號信箱",
  )
  hashed_password = Column(
      String(255), nullable=False, comment="Bcrypt 密碼雜湊"
  )
  role = Column(
      String(32),
      nullable=False,
      default=UserRole.USER.value,
      comment="使用者角色權限 (admin/user/viewer)",
  )
  is_active = Column(
      Boolean,
      default=True,
      nullable=False,
      comment="是否啟用 (停用時進行中的匯入將被取消 Q5.2)",
  )
  token_version = Column(
      Integer,
      default=1,
      nullable=False,
      comment="憑證版本號，登出或改角色時遞增",
  )
  created_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )
  updated_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      onupdate=func.now(),
      server_onupdate=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )

  created_events = relationship("Event", back_populates="creator")
  orders = relationship("Order", back_populates="user")

  __table_args__ = (
      CheckConstraint(
          "role IN ('admin', 'user', 'viewer')", name="ck_users_role"
      ),
  )


# ==========================================
# 2. 活動模型 (Event)
# ==========================================
class Event(Base):
  """活動主表

  - last_seq: 單一活動專屬的單調遞增事件序號 (Q4.1)，解決跨活動偽跳號問題。
  - version: 樂觀鎖版本號，防止並行編輯覆蓋。
  """

  __tablename__ = "events"

  id = Column(Integer, primary_key=True, index=True)
  creator_id = Column(
      Integer,
      ForeignKey("users.id", ondelete="RESTRICT"),
      nullable=False,
      comment="建立者 ID",
  )
  title = Column(String(255), nullable=False, comment="活動標題")
  description = Column(Text, nullable=True, comment="詳細描述")
  category = Column(
      String(100), nullable=False, index=True, comment="分類標籤"
  )
  venue = Column(String(255), nullable=False, comment="舉辦場地")
  start_time = Column(
      DateTime(timezone=True), nullable=False, comment="開賣/開始時間"
  )
  end_time = Column(
      DateTime(timezone=True), nullable=False, comment="結束時間"
  )
  status = Column(
      String(32),
      default=EventStatus.DRAFT.value,
      nullable=False,
      index=True,
      comment="活動狀態 (draft/published)",
  )
  last_seq = Column(
      BigInteger,
      default=0,
      nullable=False,
      comment="活動專屬事件單調遞增序號 (Q4.1)",
  )
  version = Column(
      Integer, default=1, nullable=False, comment="樂觀鎖版本號"
  )
  created_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )
  updated_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      onupdate=func.now(),
      server_onupdate=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )

  creator = relationship("User", back_populates="created_events")
  zones = relationship(
      "Zone", back_populates="event", cascade="all, delete-orphan"
  )
  seats = relationship(
      "Seat", back_populates="event", cascade="all, delete-orphan"
  )
  orders = relationship("Order", back_populates="event")
  outbox_events = relationship(
      "EventOutbox", back_populates="event", cascade="all, delete-orphan"
  )

  __table_args__ = (
      CheckConstraint(
          "status IN ('draft', 'published')", name="ck_events_status"
      ),
  )


# ==========================================
# 3. 票價區域模型 (Zone)
# ==========================================
class Zone(Base):
  """活動區域票價設定"""

  __tablename__ = "zones"

  id = Column(Integer, primary_key=True, index=True)
  event_id = Column(
      Integer, ForeignKey("events.id", ondelete="CASCADE"), nullable=False
  )
  zone_code = Column(String(50), nullable=False, comment="區域代碼 (如 A, VIP)")
  name = Column(String(100), nullable=False, comment="區域名稱")
  price = Column(Numeric(10, 2), nullable=False, comment="該區票價")
  created_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )

  event = relationship("Event", back_populates="zones")

  __table_args__ = (
      UniqueConstraint("event_id", "zone_code", name="uq_event_zone_code"),
  )


# ==========================================
# 4. 座位模型 (Seat)
# ==========================================
class Seat(Base):
  """座位庫存模型

  - 複合唯一鍵防止重複匯入。
  - 複合外來鍵約束保證 zone_code 必須合法存在於該活動的 zones 中。
  - ix_seats_allocation 加速自動配位 (FOR UPDATE SKIP LOCKED)。
  """

  __tablename__ = "seats"

  id = Column(BigInteger, primary_key=True, index=True)
  event_id = Column(
      Integer, ForeignKey("events.id", ondelete="CASCADE"), nullable=False
  )
  zone_code = Column(String(50), nullable=False, comment="所屬分區")
  row_label = Column(String(50), nullable=False, comment="排號")
  seat_number = Column(String(50), nullable=False, comment="座位號")
  status = Column(
      String(32),
      default=SeatStatus.AVAILABLE.value,
      nullable=False,
      comment="座位狀態 (available/held/sold)",
  )
  held_by = Column(
      Integer,
      ForeignKey("users.id", ondelete="SET NULL"),
      nullable=True,
      comment="當前鎖定該座位之 User ID",
  )
  expires_at = Column(
      DateTime(timezone=True),
      nullable=True,
      comment="鎖位逾期時戳 (依據 DB NOW())",
  )
  version = Column(
      Integer, default=1, nullable=False, comment="樂觀更新版本號"
  )
  created_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )
  updated_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      onupdate=func.now(),
      server_onupdate=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )

  event = relationship("Event", back_populates="seats")
  holder = relationship("User")

  __table_args__ = (
      # 1. 狀態約束
      CheckConstraint(
          "status IN ('available', 'held', 'sold')", name="ck_seats_status"
      ),
      # 2. 複合外鍵：保證座位的 zone_code 必須合法存在於該活動的 zones 中，且有座位時物理禁止刪除區域
      ForeignKeyConstraint(
          ["event_id", "zone_code"],
          ["zones.event_id", "zones.zone_code"],
          ondelete="RESTRICT",
          name="fk_seats_zones_event_zone",
      ),
      # 3. 防止同場活動重複匯入相同位置
      UniqueConstraint(
          "event_id",
          "zone_code",
          "row_label",
          "seat_number",
          name="uq_event_seat_location",
      ),
      # 4. 自動配位加速索引 (SELECT FOR UPDATE SKIP LOCKED)
      Index(
          "ix_seats_allocation",
          "event_id",
          "zone_code",
          "status",
          "row_label",
          "seat_number",
      ),
      # 5. 逾時釋放掃描索引 (WHERE status='held' AND expires_at < NOW())
      Index("ix_seats_held_expiry", "status", "expires_at"),
  )


# ==========================================
# 5. 訂單模型 (Order)
# ==========================================
class Order(Base):
  """購票訂單主表

  - service_fee: 5% 服務費精確四捨五入 (ROUND_HALF_UP)。
  - idempotency_key: 冪等鍵，保證重試安全性。
  - ix_orders_user_created: 加速使用者「我的訂單」頁面排序查詢。
  - ix_orders_pending_expiry: 部分索引，僅索引 pending 訂單以縮小逾時掃描範圍。
  """

  __tablename__ = "orders"

  id = Column(BigInteger, primary_key=True, index=True)
  user_id = Column(
      Integer, ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
  )
  event_id = Column(
      Integer, ForeignKey("events.id", ondelete="RESTRICT"), nullable=False
  )
  total_ticket_amount = Column(
      Numeric(12, 2), nullable=False, comment="票價總計"
  )
  service_fee = Column(
      Numeric(12, 2), nullable=False, comment="5% 服務費 (四捨五入)"
  )
  total_amount = Column(
      Numeric(12, 2), nullable=False, comment="實付金額 (票價+手續費)"
  )
  status = Column(
      String(32),
      default=OrderStatus.PENDING.value,
      nullable=False,
      index=True,
      comment="訂單狀態 (pending/paid/cancelled/refund)",
  )
  idempotency_key = Column(
      String(128),
      nullable=True,
      comment="客戶端冪等鍵",
  )
  expires_at = Column(
      DateTime(timezone=True),
      nullable=False,
      comment="訂單付款截止時間",
  )
  created_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )
  updated_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      onupdate=func.now(),
      server_onupdate=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )

  user = relationship("User", back_populates="orders")
  event = relationship("Event", back_populates="orders")
  items = relationship(
      "OrderItem", back_populates="order", cascade="all, delete-orphan"
  )

  __table_args__ = (
      CheckConstraint(
          "status IN ('pending', 'paid', 'cancelled',"
          " 'cancelled_refund_required')",
          name="ck_orders_status",
      ),
      UniqueConstraint("idempotency_key", name="uq_orders_idempotency_key"),
      # 1. 使用者歷史訂單查詢加速 (WHERE user_id = :uid ORDER BY created_at DESC)
      Index("ix_orders_user_created", "user_id", "created_at"),
      # 2. 待付款逾時掃描部分索引 (僅索引 pending 狀態，避免掃描歷史訂單)
      Index(
          "ix_orders_pending_expiry",
          "expires_at",
          postgresql_where=text("status = 'pending'"),
      ),
  )


# ==========================================
# 6. 訂單座位明細 (OrderItem) — 關鍵第二道防線
# ==========================================
class OrderItem(Base):
  """訂單明細項目

  物理約束防線：
  1. uq_order_item_seat: 同一張訂單內不可重複加入同一座位。
  2. uq_active_seat_order: 針對 status IN ('held', 'paid') 建立部分唯一索引。
     即使應用程式併發控制失效，資料庫引擎在物理上亦絕對禁止同一個座位同時存在於兩個有效訂單中！
  """

  __tablename__ = "order_items"

  id = Column(BigInteger, primary_key=True, index=True)
  order_id = Column(
      BigInteger, ForeignKey("orders.id", ondelete="CASCADE"), nullable=False
  )
  seat_id = Column(
      BigInteger, ForeignKey("seats.id", ondelete="RESTRICT"), nullable=False
  )
  price = Column(Numeric(10, 2), nullable=False, comment="購入單價")
  status = Column(
      String(32),
      default=OrderItemStatus.HELD.value,
      nullable=False,
      comment="項目狀態 (held/paid/cancelled)",
  )
  created_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )

  order = relationship("Order", back_populates="items")
  seat = relationship("Seat")

  __table_args__ = (
      CheckConstraint(
          "status IN ('held', 'paid', 'cancelled')", name="ck_order_items_status"
      ),
      # 1. 保證同一張訂單內不重複加入相同座位
      UniqueConstraint("order_id", "seat_id", name="uq_order_item_seat"),
      # 2. 跨訂單防超賣部分唯一索引 (Partial Unique Index)
      Index(
          "uq_active_seat_order",
          "seat_id",
          unique=True,
          postgresql_where=text("status IN ('held', 'paid')"),
      ),
      Index("ix_order_items_order_id", "order_id"),
  )


# ==========================================
# 7. Transactional Outbox 模型 (EventOutbox)
# ==========================================
class EventOutbox(Base):
  """Transactional Outbox 表

  - 座位變更與此表於同一個 DB 事務內提交，解決 Dual-Write 一致性中斷問題。
  - sequence_id: 單一活動專屬連續序號，由 Relay Worker 推播至 Redis Pub/Sub。
  - ix_outbox_pending_relay: 部分索引 (Partial Index)，僅索引 status = 'pending' 以維持索引精簡。
  """

  __tablename__ = "event_outbox"

  id = Column(BigInteger, primary_key=True, index=True)
  event_id = Column(
      Integer, ForeignKey("events.id", ondelete="CASCADE"), nullable=False
  )
  sequence_id = Column(
      BigInteger, nullable=False, comment="活動專屬單調遞增序號"
  )
  event_type = Column(
      String(100), nullable=False, comment="事件類型 (如 seat_held, seat_sold)"
  )
  payload = Column(JSONB, nullable=False, comment="事件負載資料 (含座標與狀態)")
  status = Column(
      String(32),
      default=OutboxStatus.PENDING.value,
      nullable=False,
      index=True,
      comment="發布狀態 (pending/published)",
  )
  created_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )

  event = relationship("Event", back_populates="outbox_events")

  __table_args__ = (
      CheckConstraint(
          "status IN ('pending', 'published')", name="ck_event_outbox_status"
      ),
      UniqueConstraint(
          "event_id", "sequence_id", name="uq_event_outbox_sequence"
      ),
      # Partial Index，僅針對待發布事件建立索引，縮小掃描範圍
      Index(
          "ix_outbox_pending_relay",
          "created_at",
          postgresql_where=text("status = 'pending'"),
      ),
  )


# ==========================================
# 8. 批量匯入任務模型 (ImportJob)
# ==========================================
class ImportJob(Base):
  """座位匯入非同步任務

  - 支援 CSV/JSON 格式，10 萬筆 < 60 秒處理。
  - uq_event_file_sha256_job: 題目要求相同檔案上傳回傳既有 Job，不得重複匯入。
  """

  __tablename__ = "import_jobs"

  id = Column(BigInteger, primary_key=True, index=True)
  event_id = Column(
      Integer, ForeignKey("events.id", ondelete="CASCADE"), nullable=False
  )
  user_id = Column(
      Integer, ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
  )
  file_sha256 = Column(
      String(64), nullable=False, comment="檔案 SHA-256 雜湊 (去重依據)"
  )
  filename = Column(String(255), nullable=False, comment="原始檔名")
  total_rows = Column(
      Integer, default=0, nullable=False, comment="檔案總列數"
  )
  imported_rows = Column(
      Integer, default=0, nullable=False, comment="成功匯入列數"
  )
  error_rows = Column(
      Integer, default=0, nullable=False, comment="異常錯誤列數"
  )
  mode = Column(
      String(32),
      default=ImportJobMode.ALL_OR_NOTHING.value,
      nullable=False,
      comment="匯入模式 (all_or_nothing / skip_invalid)",
  )
  status = Column(
      String(32),
      default=ImportJobStatus.PENDING.value,
      nullable=False,
      comment="任務進度狀態",
  )
  errors = Column(
      JSONB,
      default=list,
      nullable=False,
      comment="錯誤清單 [{row: 1, reason: 'duplicate'}]",
  )
  created_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )
  updated_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      onupdate=func.now(),
      server_onupdate=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )

  __table_args__ = (
      CheckConstraint(
          "mode IN ('all_or_nothing', 'skip_invalid')",
          name="ck_import_jobs_mode",
      ),
      CheckConstraint(
          "status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')",
          name="ck_import_jobs_status",
      ),
      # 題目硬性規範：同一活動重複上傳相同檔案回傳既有 Job，不得重複匯入
      UniqueConstraint(
          "event_id", "file_sha256", name="uq_event_file_sha256_job"
      ),
  )


# ==========================================
# 9. HTTP 存取日誌模型 (AccessLog)
# ==========================================
class AccessLog(Base):
  """全鏈路存取日誌 (依據 request_id 追蹤)"""

  __tablename__ = "access_logs"

  id = Column(BigInteger, primary_key=True, index=True)
  request_id = Column(
      String(64),
      nullable=False,
      index=True,
      comment="X-Request-ID 全鏈路追蹤 ID",
  )
  user_id = Column(
      Integer, nullable=True, comment="請求操作之 User ID (若已認證)"
  )
  method = Column(String(10), nullable=False, comment="HTTP 方法")
  path = Column(String(255), nullable=False, comment="請求路徑")
  status_code = Column(Integer, nullable=False, comment="HTTP 狀態碼")
  latency_ms = Column(Numeric(10, 2), nullable=False, comment="請求延遲毫秒數")
  client_ip = Column(String(45), nullable=True, comment="客戶端來源 IP")
  created_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      nullable=False,
      index=True,
  )


# ==========================================
# 10. 每秒系統指標模型 (SystemMetric)
# ==========================================
class SystemMetric(Base):
  """每秒系統指標彙總 (由監控 Worker 批次入庫)"""

  __tablename__ = "metrics_per_second"

  id = Column(BigInteger, primary_key=True, index=True)
  timestamp = Column(
      DateTime(timezone=True),
      nullable=False,
      comment="秒級對齊時間戳",
  )
  total_requests = Column(
      Integer, default=0, nullable=False, comment="該秒總請求量 (RPS)"
  )
  error_requests = Column(
      Integer, default=0, nullable=False, comment="該秒錯誤請求數 (5xx)"
  )
  p50_latency_ms = Column(
      Numeric(10, 2), default=0, nullable=False, comment="p50 延遲"
  )
  p95_latency_ms = Column(
      Numeric(10, 2), default=0, nullable=False, comment="p95 延遲"
  )
  p99_latency_ms = Column(
      Numeric(10, 2), default=0, nullable=False, comment="p99 延遲"
  )
  active_connections = Column(
      Integer, default=0, nullable=False, comment="該秒即時在線連線數"
  )
  created_at = Column(
      DateTime(timezone=True),
      server_default=text("CURRENT_TIMESTAMP"),
      nullable=False,
  )

  __table_args__ = (
      UniqueConstraint("timestamp", name="uq_metrics_timestamp"),
  )
