"""訂單查詢／取消、模擬金流與簽章 Webhook API 路由"""

from typing import Optional
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_current_user, public_endpoint, require_roles
from app.db import get_db
from app.models import OrderStatus, User, UserRole
from app.payments.schemas import (
    MockPaymentRequest,
    MockPaymentResponse,
    OrderDetailResponse,
    OrderListResponse,
    PaymentWebhookPayload,
    PaymentWebhookResponse,
)
from app.payments.service import (
    cancel_order,
    get_order_detail,
    list_orders,
    process_payment_webhook,
    trigger_mock_payment,
)
from app.payments.signature import verify_webhook_signature

orders_router = APIRouter(prefix="/api/v1/orders", tags=["Orders"])
payments_router = APIRouter(prefix="/api/v1/payments", tags=["Payments"])


@orders_router.get(
    "",
    response_model=OrderListResponse,
    summary="查詢訂單清單（User 僅看本人、Admin/Viewer 可看全部）",
)
async def list_orders_endpoint(
    status_filter: Optional[OrderStatus] = Query(
        None, alias="status", description="依訂單狀態篩選"
    ),
    event_id: Optional[int] = Query(None, ge=1, description="依活動 ID 篩選"),
    limit: int = Query(50, ge=1, le=200),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await list_orders(
        db,
        current_user,
        status_filter=status_filter,
        event_id=event_id,
        limit=limit,
    )


@orders_router.get(
    "/{order_id}",
    response_model=OrderDetailResponse,
    summary="查詢單筆訂單明細與座位狀態",
)
async def get_order_detail_endpoint(
    order_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await get_order_detail(db, order_id, current_user)


@orders_router.post(
    "/{order_id}/cancel",
    response_model=OrderDetailResponse,
    summary="自主取消待付款訂單並即時釋放座位",
)
async def cancel_order_endpoint(
    order_id: int,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    return await cancel_order(db, order_id, current_user)


@payments_router.post(
    "/mock/pay",
    response_model=MockPaymentResponse,
    summary="模擬金流付款（自動簽發 HMAC-SHA256 Webhook 並完成入帳）",
)
async def mock_pay_endpoint(
    req: MockPaymentRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    return await trigger_mock_payment(
        db,
        current_user,
        order_id=req.order_id,
        outcome=req.outcome,
        webhook_event_id=req.webhook_event_id,
    )


@payments_router.post(
    "/webhook",
    response_model=PaymentWebhookResponse,
    summary="金流簽章 Webhook 回呼端點（HMAC-SHA256 + 5 分鐘防重放 + 事件冪等）",
)
@public_endpoint("Authenticated via HMAC-SHA256 webhook signature rather than user JWT")
async def payment_webhook_endpoint(
    request: Request,
    x_webhook_timestamp: Optional[str] = Header(None, alias="X-Webhook-Timestamp"),
    x_webhook_signature: Optional[str] = Header(None, alias="X-Webhook-Signature"),
    db: AsyncSession = Depends(get_db),
):
    raw_body = await request.body()
    verify_webhook_signature(x_webhook_timestamp, x_webhook_signature, raw_body)

    try:
        payload = PaymentWebhookPayload.model_validate_json(raw_body)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=exc.errors(),
        )

    return await process_payment_webhook(db, payload)
