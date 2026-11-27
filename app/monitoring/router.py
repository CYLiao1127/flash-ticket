"""Admin 監控指標、存取日誌與全鏈路 Request-ID 查詢 API 路由 (Commit 18 / 6.6)"""

from decimal import Decimal
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import require_roles
from app.db import get_db
from app.models import AccessLog, EventOutbox, SystemMetric, User, UserRole
from app.monitoring.collector import get_cluster_active_connections
from app.monitoring.schemas import (
    AccessLogItemResponse,
    AccessLogListResponse,
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
