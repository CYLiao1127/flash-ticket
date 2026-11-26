import pytest
import pytest_asyncio
from sqlalchemy import text, create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from alembic.config import Config
from alembic import command
from starlette.testclient import TestClient

from app.config import settings
from app.db import get_db
from app.main import app

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


async def override_get_db():
    async with TestSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


app.dependency_overrides[get_db] = override_get_db


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


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client
