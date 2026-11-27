"""金流付款、Webhook 回呼與訂單管理 Pydantic 結構定義"""

from datetime import datetime
from decimal import Decimal
from typing import List, Literal, Optional
from pydantic import BaseModel, ConfigDict, Field

from app.tickets.schemas import OrderItemDetailResponse


class OrderDetailResponse(BaseModel):
    """訂單明細與座位狀態回應"""

    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: int
    user_id: int
    status: str
    idempotency_key: Optional[str] = None
    total_ticket_amount: Decimal
    service_fee: Decimal
    total_amount: Decimal
    expires_at: datetime
    created_at: datetime
    updated_at: datetime
    items: List[OrderItemDetailResponse]


class OrderListResponse(BaseModel):
    """訂單清單回應"""

    items: List[OrderDetailResponse]
    total: int


class MockPaymentRequest(BaseModel):
    """模擬金流發起付款請求"""

    order_id: int = Field(..., ge=1, description="欲付款之訂單 ID")
    outcome: Literal["succeeded", "failed"] = Field(
        default="succeeded",
        description="模擬金流結果：succeeded 或 failed",
    )
    webhook_event_id: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=128,
        description="選填：指定 Webhook 事件 ID（便於重放冪等測試）",
    )


class PaymentWebhookPayload(BaseModel):
    """金流 Webhook 回呼資料結構"""

    webhook_event_id: str = Field(..., min_length=1, max_length=128)
    order_id: int = Field(..., ge=1)
    payment_status: Literal["succeeded", "failed"]
    amount: Optional[Decimal] = Field(default=None, ge=0)


class PaymentWebhookResponse(BaseModel):
    """Webhook 處理回應"""

    webhook_event_id: str
    order_id: int
    order_status: str
    action: str
    duplicate: bool = False
    refund_triggered: bool = False
    sequence_id: Optional[int] = None


class MockPaymentResponse(BaseModel):
    """模擬金流執行回應"""

    webhook_event_id: str
    timestamp: str
    signature: str
    webhook_result: PaymentWebhookResponse
