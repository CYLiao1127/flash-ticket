"""核心搶票與業務統計 API 路由"""

from typing import Optional
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import require_roles
from app.db import get_db
from app.models import User, UserRole
from app.tickets.metrics import get_ticket_metrics_snapshot
from app.tickets.schemas import (
    AutoAllocateSeatsRequest,
    OrderReservationResponse,
    ReserveSeatsRequest,
    TicketMetricsResponse,
)
from app.tickets.service import allocate_zone_seats, reserve_specific_seats

router = APIRouter(prefix="/api/v1/events", tags=["Tickets"])
admin_tickets_router = APIRouter(prefix="/api/v1/admin/tickets", tags=["Admin - Tickets"])


@router.post(
    "/{event_id}/tickets/reserve",
    response_model=OrderReservationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="指定座位選位預訂（限購 4 張、條件原子更新防超賣）",
)
async def reserve_seats_endpoint(
    event_id: int,
    req: ReserveSeatsRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限 Admin / User 針對已發布活動指定 1~4 個座位鎖位預訂，受單人單場限購 4 張與雙重防超賣保護"""
    return await reserve_specific_seats(db, event_id, current_user, req.seat_ids)


@router.post(
    "/{event_id}/tickets/allocate",
    response_model=OrderReservationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="區域自動配位預訂（FOR UPDATE SKIP LOCKED、限購 4 張）",
)
async def allocate_seats_endpoint(
    event_id: int,
    req: AutoAllocateSeatsRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限 Admin / User 針對已發布活動指定區域自動配位 1~4 張，受單人單場限購 4 張與 SKIP LOCKED 保護"""
    return await allocate_zone_seats(
        db, event_id, current_user, req.zone_code, req.quantity
    )


@admin_tickets_router.get(
    "/metrics",
    response_model=TicketMetricsResponse,
    summary="查詢搶票業務層統計指標（購票成功、鎖位衝突、庫存售罄、限購超額）",
)
async def get_ticket_metrics_endpoint(
    event_id: Optional[int] = Query(None, ge=1, description="選填：依指定活動篩選統計"),
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.VIEWER)),
):
    """僅限 Admin / Viewer 查詢搶票業務層統計指標"""
    return await get_ticket_metrics_snapshot(event_id=event_id)
