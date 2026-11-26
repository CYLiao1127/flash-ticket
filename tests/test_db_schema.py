import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import text, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from alembic.config import Config
from alembic import command
from alembic.migration import MigrationContext
from alembic.autogenerate import compare_metadata

from app.config import settings
from app.db import Base
from app.models import (
    User,
    UserRole,
    Event,
    Zone,
    Seat,
    Order,
    OrderItem,
    OrderItemStatus,
    ImportJob,
)

test_engine = create_async_engine(
    settings.DATABASE_URL,
    poolclass=NullPool,
    echo=False,
)

TestSessionLocal = async_sessionmaker(
    bind=test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


def ensure_test_database_exists():
    """確保測試資料庫存在；若不存在則自動於 PostgreSQL 建立"""
    admin_url = (
        f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@"
        f"{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/postgres"
    )
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = 'flash_ticket_test'")
            ).scalar()
            if not exists:
                conn.execute(text("CREATE DATABASE flash_ticket_test"))
    finally:
        admin_engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def setup_test_database_via_alembic():
    """使用 Alembic Migration 建立測試資料庫 Schema，嚴格驗證 Migration 腳本之正確性"""
    ensure_test_database_exists()

    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", settings.SYNC_DATABASE_URL)

    # 確保由乾淨起點執行完整 upgrade head
    command.downgrade(alembic_cfg, "base")
    command.upgrade(alembic_cfg, "head")

    yield

    # 測試結束後清理
    command.downgrade(alembic_cfg, "base")


@pytest_asyncio.fixture(scope="function")
async def db_session():
    """每次測試前清空資料表，並於由 Alembic 建立的實體 Schema 上執行測試"""
    async with test_engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE TABLE users, events, zones, seats, orders, order_items, "
                "event_outbox, import_jobs, access_logs, metrics_per_second "
                "RESTART IDENTITY CASCADE;"
            )
        )

    async with TestSessionLocal() as session:
        yield session

    async with test_engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE TABLE users, events, zones, seats, orders, order_items, "
                "event_outbox, import_jobs, access_logs, metrics_per_second "
                "RESTART IDENTITY CASCADE;"
            )
        )


def test_alembic_migrations_and_schema_consistency():
    """
    驗證 Alembic 遷移腳本與 SQLAlchemy ORM 模型完全一致 (Zero Schema Drift)
    1. 驗證 downgrade/upgrade 可重複且無錯誤執行
    2. 透過 compare_metadata 檢驗 Alembic 產生的實體資料庫與 Base.metadata 無任何結構性差異
    """
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", settings.SYNC_DATABASE_URL)

    # 測試 downgrade 到 base
    command.downgrade(alembic_cfg, "base")
    # 測試 upgrade 回 head
    command.upgrade(alembic_cfg, "head")

    sync_engine = create_engine(settings.SYNC_DATABASE_URL)
    with sync_engine.connect() as conn:
        mc = MigrationContext.configure(conn, opts={"compare_type": True})
        diff = compare_metadata(mc, Base.metadata)

        # 排除註解 (modify_comment) 差異，專注於表、欄位、型別、約束與索引
        meaningful_diff = [
            d for d in diff
            if not (isinstance(d, list) and d and d[0][0] == "modify_comment")
        ]
        assert meaningful_diff == [], f"Alembic migration 與 models.py 存在結構差異: {meaningful_diff}"


@pytest.mark.asyncio
async def test_partial_unique_index_blocks_duplicate_active_seats(db_session):
    """驗證關鍵防線：透過 Alembic 建立之 uq_active_seat_order 部分唯一索引必須阻止同一座位被重複鎖定或售出"""
    now = datetime.now(timezone.utc)
    user = User(email="buyer@test.com", hashed_password="pw", role=UserRole.USER)
    db_session.add(user)
    await db_session.flush()

    event = Event(
        creator_id=user.id,
        title="Concert",
        category="Music",
        venue="Arena",
        start_time=now + timedelta(days=1),
        end_time=now + timedelta(days=2),
    )
    db_session.add(event)
    await db_session.flush()

    zone = Zone(event_id=event.id, zone_code="A", name="VIP", price=1000.0)
    db_session.add(zone)
    await db_session.flush()

    seat = Seat(event_id=event.id, zone_code="A", row_label="1", seat_number="1")
    db_session.add(seat)
    await db_session.flush()

    order1 = Order(
        user_id=user.id,
        event_id=event.id,
        total_ticket_amount=1000.0,
        service_fee=50.0,
        total_amount=1050.0,
        expires_at=now + timedelta(minutes=10),
    )
    db_session.add(order1)
    await db_session.flush()

    order2 = Order(
        user_id=user.id,
        event_id=event.id,
        total_ticket_amount=1000.0,
        service_fee=50.0,
        total_amount=1050.0,
        expires_at=now + timedelta(minutes=10),
    )
    db_session.add(order2)
    await db_session.flush()

    # 提早取得 primary key id，避免 rollback 後觸發 lazy load 造成 MissingGreenlet
    seat_id = seat.id
    order1_id = order1.id
    order2_id = order2.id

    # 1. 第一筆訂單鎖定座位 (status = held)
    await db_session.execute(
        text("INSERT INTO order_items (order_id, seat_id, price, status) VALUES (:oid, :sid, :p, :st)"),
        {"oid": order1_id, "sid": seat_id, "p": 1000.0, "st": "held"},
    )
    await db_session.commit()

    # 2. 第二筆訂單對同一座位插入 status = held，必須觸發 uq_active_seat_order 唯一限制違規
    with pytest.raises(IntegrityError) as exc_info:
        await db_session.execute(
            text("INSERT INTO order_items (order_id, seat_id, price, status) VALUES (:oid, :sid, :p, :st)"),
            {"oid": order2_id, "sid": seat_id, "p": 1000.0, "st": "held"},
        )
        await db_session.commit()

    assert "uq_active_seat_order" in str(exc_info.value)
    await db_session.rollback()

    # 3. 若原訂單項目取消 (status = cancelled)，第二筆訂單即可成功持有該座位
    await db_session.execute(
        text("UPDATE order_items SET status = 'cancelled' WHERE order_id = :oid AND seat_id = :sid"),
        {"oid": order1_id, "sid": seat_id},
    )
    await db_session.commit()

    # 現在 order2 插入 held 必須成功
    res = await db_session.execute(
        text("INSERT INTO order_items (order_id, seat_id, price, status) VALUES (:oid, :sid, :p, :st) RETURNING id"),
        {"oid": order2_id, "sid": seat_id, "p": 1000.0, "st": "held"},
    )
    await db_session.commit()
    assert res.scalar_one() is not None


@pytest.mark.asyncio
async def test_duplicate_import_job_sha256_constraint(db_session):
    """驗證題目要求：在 Alembic 建立之 Schema 上，相同活動上傳相同 SHA-256 檔案必須觸發唯一限制"""
    now = datetime.now(timezone.utc)
    user = User(email="importer@test.com", hashed_password="pw", role=UserRole.USER)
    db_session.add(user)
    await db_session.flush()

    event = Event(
        creator_id=user.id,
        title="Concert 2",
        category="Music",
        venue="Arena",
        start_time=now + timedelta(days=1),
        end_time=now + timedelta(days=2),
    )
    db_session.add(event)
    await db_session.flush()

    job1 = ImportJob(
        event_id=event.id,
        user_id=user.id,
        file_sha256="abc123sha256fake",
        filename="seats.csv",
    )
    db_session.add(job1)
    await db_session.commit()

    job2 = ImportJob(
        event_id=event.id,
        user_id=user.id,
        file_sha256="abc123sha256fake",
        filename="seats_duplicate.csv",
    )
    db_session.add(job2)

    with pytest.raises(IntegrityError) as exc_info:
        await db_session.commit()

    assert "uq_event_file_sha256_job" in str(exc_info.value)
