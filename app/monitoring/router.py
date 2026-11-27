"""Admin 監控指標、存取日誌、即時異常告警與全鏈路 Request-ID 查詢 API 路由 (Commit 18 & 19)"""

import json
import time
from dataclasses import asdict
from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import require_roles
from app.core.redis import get_redis
from app.db import get_db
from app.models import AccessLog, EventOutbox, SystemMetric, User, UserRole
from app.monitoring.alerts import (
    ALERT_CHANNEL,
    DEFAULT_ALERT_THRESHOLDS,
    evaluate_and_dispatch_alerts,
    get_recent_alerts,
)
from app.monitoring.collector import get_cluster_active_connections
from app.monitoring.schemas import (
    AccessLogItemResponse,
    AccessLogListResponse,
    AlertEvaluateResponse,
    AlertItemResponse,
    AlertListResponse,
    AlertThresholdsResponse,
    MetricsSummaryResponse,
    RequestTraceResponse,
    SystemMetricItemResponse,
    TraceOutboxItemResponse,
)

admin_monitoring_router = APIRouter(
    prefix="/api/v1/admin/monitoring",
    tags=["Admin - Monitoring"],
)


@admin_monitoring_router.get(
    "/metrics",
    response_model=MetricsSummaryResponse,
    summary="查詢每秒系統監控指標 (RPS、p50/p95/p99 延遲、錯誤率、即時連線數)",
)
async def get_system_metrics_endpoint(
    limit: int = Query(60, ge=1, le=3600, description="回傳最近幾秒之指標紀錄"),
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.VIEWER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限 Admin / Viewer 檢視每秒聚合之系統指標序列與摘要"""
    rows = list(
        (
            await db.execute(
                select(SystemMetric)
                .order_by(SystemMetric.timestamp.desc())
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    rows.reverse()

    total_reqs = sum(r.total_requests for r in rows)
    error_reqs = sum(r.error_requests for r in rows)
    error_rate = round(error_reqs / total_reqs, 6) if total_reqs > 0 else 0.0
    avg_rps = round(total_reqs / len(rows), 2) if rows else 0.0
    max_p99 = (
        max((Decimal(r.p99_latency_ms) for r in rows), default=Decimal("0.00"))
        if rows
        else Decimal("0.00")
    )
    active_conns = await get_cluster_active_connections()

    return MetricsSummaryResponse(
        window_seconds=len(rows),
        total_requests=total_reqs,
        error_requests=error_reqs,
        error_rate=error_rate,
        avg_rps=avg_rps,
        max_p99_latency_ms=max_p99,
        current_active_connections=active_conns,
        series=[SystemMetricItemResponse.model_validate(r) for r in rows],
    )


@admin_monitoring_router.get(
    "/access-logs",
    response_model=AccessLogListResponse,
    summary="查詢系統 HTTP 存取日誌（支援 request_id / user_id / path / status_code 篩選）",
)
async def list_access_logs_endpoint(
    request_id: Optional[str] = Query(None, max_length=64),
    user_id: Optional[int] = Query(None, ge=1),
    path: Optional[str] = Query(None, max_length=255),
    status_code: Optional[int] = Query(None, ge=100, le=599),
    limit: int = Query(50, ge=1, le=500),
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.VIEWER)),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(AccessLog)
    if request_id is not None:
        stmt = stmt.where(AccessLog.request_id == request_id)
    if user_id is not None:
        stmt = stmt.where(AccessLog.user_id == user_id)
    if path is not None:
        stmt = stmt.where(AccessLog.path == path)
    if status_code is not None:
        stmt = stmt.where(AccessLog.status_code == status_code)
    stmt = stmt.order_by(AccessLog.created_at.desc(), AccessLog.id.desc()).limit(limit)

    items = list((await db.execute(stmt)).scalars().all())
    return AccessLogListResponse(
        items=[AccessLogItemResponse.model_validate(r) for r in items],
        total=len(items),
    )


@admin_monitoring_router.get(
    "/trace/{request_id}",
    response_model=RequestTraceResponse,
    summary="依 X-Request-ID 全鏈路追蹤查詢（串聯 HTTP AccessLog 與 Outbox 領域事件）",
)
async def trace_by_request_id_endpoint(
    request_id: str,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.VIEWER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限 Admin / Viewer 依 request_id 跨服務還原單一請求之 HTTP 存取與背景 Outbox 事件"""
    access_logs = list(
        (
            await db.execute(
                select(AccessLog)
                .where(AccessLog.request_id == request_id)
                .order_by(AccessLog.created_at.asc(), AccessLog.id.asc())
            )
        )
        .scalars()
        .all()
    )
    outbox_events = list(
        (
            await db.execute(
                select(EventOutbox)
                .where(text("payload->>'request_id' = :rid"))
                .params(rid=request_id)
                .order_by(EventOutbox.created_at.asc(), EventOutbox.id.asc())
            )
        )
        .scalars()
        .all()
    )
    return RequestTraceResponse(
        request_id=request_id,
        access_logs=[AccessLogItemResponse.model_validate(r) for r in access_logs],
        outbox_events=[TraceOutboxItemResponse.model_validate(r) for r in outbox_events],
    )


@admin_monitoring_router.get(
    "/alerts",
    response_model=AlertListResponse,
    summary="查詢近期系統與業務異常告警歷史與規則門檻",
)
async def list_alerts_endpoint(
    limit: int = Query(50, ge=1, le=200),
    rule: Optional[str] = Query(None, max_length=64),
    severity: Optional[str] = Query(None, max_length=16),
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.VIEWER)),
):
    """僅限 Admin / Viewer 查詢最近觸發的異常告警與現行規則門檻"""
    items = await get_recent_alerts(limit=limit, rule=rule, severity=severity)
    return AlertListResponse(
        items=[AlertItemResponse.model_validate(a) for a in items],
        total=len(items),
        thresholds=AlertThresholdsResponse(**asdict(DEFAULT_ALERT_THRESHOLDS)),
    )


@admin_monitoring_router.post(
    "/alerts/evaluate",
    response_model=AlertEvaluateResponse,
    summary="手動觸發一次異常告警規則評估並即時推播 (Admin)",
)
async def evaluate_alerts_endpoint(
    current_user: User = Depends(require_roles(UserRole.ADMIN)),
    db: AsyncSession = Depends(get_db),
):
    """僅限 Admin 手動觸發一次四大異常規則檢查並透過即時通道派送告警"""
    dispatched = await evaluate_and_dispatch_alerts(db)
    return AlertEvaluateResponse(
        dispatched=[AlertItemResponse.model_validate(a) for a in dispatched],
        dispatched_count=len(dispatched),
    )


@admin_monitoring_router.get(
    "/alerts/stream",
    summary="SSE 即時訂閱系統與業務異常告警推播通道 (Admin / Viewer)",
)
async def stream_alerts_endpoint(
    request: Request,
    max_events: Optional[int] = Query(
        None,
        ge=0,
        le=1000,
        description="選填：接收指定筆數即時告警後自動關閉串流（0 表示僅接收初始快照）",
    ),
    timeout_seconds: Optional[float] = Query(
        None,
        gt=0,
        le=300,
        description="選填：串流最長等待秒數（未設定則持續連線）",
    ),
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.VIEWER)),
):
    """
    透過 Server-Sent Events 即時推送異常告警給在線 Admin / Viewer：
    1. 連線建立時立即推送 `event: connected` 與最近告警快照。
    2. 訂閱 Redis Pub/Sub `admin:alerts`，每當 Worker 偵測到異常即毫秒級推送 `event: alert`。
    """

    async def alert_stream():
        redis = get_redis()
        pubsub = redis.pubsub()
        await pubsub.subscribe(ALERT_CHANNEL)
        try:
            recent = await get_recent_alerts(limit=20, redis=redis)
            init_payload = json.dumps(
                {"status": "connected", "channel": ALERT_CHANNEL, "recent_alerts": recent},
                ensure_ascii=False,
            )
            yield f"event: connected\ndata: {init_payload}\n\n"
            if max_events == 0:
                return

            received = 0
            start_mono = time.monotonic()
            last_heartbeat = start_mono
            while True:
                if await request.is_disconnected():
                    break
                if timeout_seconds is not None and (time.monotonic() - start_mono) >= timeout_seconds:
                    break

                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if msg and msg.get("type") == "message":
                    data_str = msg.get("data")
                    if isinstance(data_str, bytes):
                        data_str = data_str.decode("utf-8")
                    yield f"event: alert\ndata: {data_str}\n\n"
                    received += 1
                    last_heartbeat = time.monotonic()
                    if max_events is not None and received >= max_events:
                        break
                elif time.monotonic() - last_heartbeat >= 15.0:
                    yield ": heartbeat\n\n"
                    last_heartbeat = time.monotonic()
        finally:
            try:
                await pubsub.unsubscribe(ALERT_CHANNEL)
                await pubsub.aclose()
            except Exception:
                pass

    return StreamingResponse(
        alert_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
