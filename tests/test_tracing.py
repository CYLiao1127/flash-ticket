import logging
import pytest
from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from starlette.testclient import TestClient

from app.main import app
from app.core.logging import request_id_ctx, TracingFilter, setup_logging
from app.core.outbox import create_outbox_event
from app.workers.runner import process_outbox_event, poll_and_relay_outbox
from app.models import User, UserRole, Event, EventOutbox, OutboxStatus
from app.config import settings


def test_generated_request_id(client):
    """驗證當客戶端未提供 X-Request-ID 時，系統自動生成並回傳於回應標頭"""
    response = client.get("/health")
    assert response.status_code == 200
    assert "X-Request-ID" in response.headers
    req_id = response.headers["X-Request-ID"]
    assert len(req_id) >= 32  # UUID hex format


def test_custom_request_id_passthrough(client):
    """驗證客戶端傳入自訂 X-Request-ID 時，系統完整透傳並回傳相同 ID"""
    custom_trace_id = "trace-client-custom-987654321"
    response = client.get("/health", headers={"X-Request-ID": custom_trace_id})
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == custom_trace_id


def test_log_tracing_filter_capture():
    """驗證 TracingFilter 正確附加 instance_id 與 request_id 至 LogRecord"""
    filter_ = TracingFilter()
    token = request_id_ctx.set("test-trace-id-abc")
    try:
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="test message",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True
        assert getattr(record, "request_id") == "test-trace-id-abc"
        assert getattr(record, "instance_id") == settings.INSTANCE_ID
    finally:
        request_id_ctx.reset(token)


def test_outbox_event_creation_captures_request_id():
    """驗證建立 Outbox 領域事件時，自動將當前 HTTP 上下文之 request_id 封裝入 payload"""
    token = request_id_ctx.set("http-req-trace-999")
    try:
        outbox = create_outbox_event(
            event_id=1,
            sequence_id=42,
            event_type="seat_held",
            payload={"seat_id": 1001, "zone_code": "A"},
        )
        assert outbox.payload["request_id"] == "http-req-trace-999"
        assert outbox.payload["seat_id"] == 1001
        assert outbox.status == OutboxStatus.PENDING
    finally:
        request_id_ctx.reset(token)


@pytest.mark.asyncio
async def test_worker_process_outbox_event_inherits_request_id_in_logs():
    """驗證背景 Worker 在處理 Outbox 事件時，日誌自動延續當初 HTTP 請求之 request_id"""
    worker_logger = logging.getLogger("flash_ticket.worker")
    worker_logger.setLevel(logging.INFO)
    log_capture = []

    class MemoryHandler(logging.Handler):
        def emit(self, record):
            log_capture.append(record)

    handler = MemoryHandler()
    handler.addFilter(TracingFilter())
    worker_logger.addHandler(handler)

    event = EventOutbox(
        id=99,
        event_id=1,
        sequence_id=1,
        event_type="seat_sold",
        payload={"seat_id": 888, "request_id": "worker-trace-continuity-555"},
        status=OutboxStatus.PENDING,
    )

    try:
        await process_outbox_event(event, session=None)
        assert event.status == OutboxStatus.PUBLISHED

        # 檢驗 Worker 輸出的日誌紀錄
        assert len(log_capture) > 0
        relay_record = log_capture[-1]
        assert getattr(relay_record, "request_id") == "worker-trace-continuity-555"
        assert getattr(relay_record, "instance_id") == settings.INSTANCE_ID
        assert "Relaying outbox event id=99" in relay_record.getMessage()
    finally:
        worker_logger.removeHandler(handler)


@pytest.mark.asyncio
async def test_full_chain_http_to_outbox_to_worker_database(db_session):
    """
    ★ 核心驗證：HTTP 請求 -> DB Outbox 寫入 -> 背景 Worker 撈取推播的全鏈路追蹤閉環
    1. 模擬 HTTP 請求到達，ContextVar 綁定自訂 Trace ID
    2. 交易內寫入資料庫 EventOutbox，驗證 payload.request_id 成功持久化
    3. 背景 Worker 執行輪詢與推播處理
    4. 驗證 Worker 日誌攜帶完全相同的 Trace ID，且事件狀態更新為 published
    """
    setup_logging("INFO")
    now = datetime.now(timezone.utc)
    user = User(email="organizer@flashticket.com", hashed_password="pw", role=UserRole.ADMIN)
    db_session.add(user)
    await db_session.flush()

    event = Event(
        creator_id=user.id,
        title="Mega Concert",
        category="Music",
        venue="Dome",
        start_time=now + timedelta(days=1),
        end_time=now + timedelta(days=2),
    )
    db_session.add(event)
    await db_session.flush()

    # 1. 模擬 HTTP 請求攜帶 X-Request-ID
    trace_id = "trace-e2e-pipeline-777888999"
    token = request_id_ctx.set(trace_id)

    try:
        # 2. 於交易內寫入 Outbox 領域事件
        outbox_event = create_outbox_event(
            event_id=event.id,
            sequence_id=1,
            event_type="seat_held",
            payload={"seat_id": 500, "user_id": user.id},
        )
        db_session.add(outbox_event)
        await db_session.commit()
    finally:
        request_id_ctx.reset(token)

    # 驗證 DB 內 Outbox 紀錄之 request_id
    stmt = select(EventOutbox).where(EventOutbox.id == outbox_event.id)
    res = await db_session.execute(stmt)
    persisted_event = res.scalar_one()
    assert persisted_event.payload["request_id"] == trace_id
    assert persisted_event.status == OutboxStatus.PENDING

    # 3. 執行背景 Worker 輪詢處理，並捕捉 Worker 日誌
    worker_logger = logging.getLogger("flash_ticket.worker")
    worker_logger.setLevel(logging.INFO)
    worker_logs = []

    class MemoryHandler(logging.Handler):
        def emit(self, record):
            worker_logs.append(record)

    handler = MemoryHandler()
    handler.addFilter(TracingFilter())
    worker_logger.addHandler(handler)

    try:
        relayed_count = await poll_and_relay_outbox(db_session)
        assert relayed_count == 1

        # 4. 驗證資料庫事件狀態已切換為 PUBLISHED
        await db_session.refresh(persisted_event)
        assert persisted_event.status == OutboxStatus.PUBLISHED

        # 5. 驗證 Worker 日誌中的 request_id 與當初 HTTP 請求之 trace_id 完全吻合
        assert len(worker_logs) > 0
        log_record = worker_logs[-1]
        assert getattr(log_record, "request_id") == trace_id
        assert getattr(log_record, "instance_id") == settings.INSTANCE_ID
        assert f"Relaying outbox event id={persisted_event.id}" in log_record.getMessage()
    finally:
        worker_logger.removeHandler(handler)
