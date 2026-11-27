"""核心搶票與訂單預訂 Pydantic 結構定義"""

from datetime import datetime
from decimal import Decimal
from typing import List
from pydantic import BaseModel, ConfigDict, Field, field_validator


class ReserveSeatsRequest(BaseModel):
    """指定選位購買請求（單次最多 4 張）"""

    seat_ids: List[int] = Field(
        ...,
        min_length=1,
        max_length=4,
        description="欲預訂的座位 ID 清單（1~4 張，不可重複）",
    )

    @field_validator("seat_ids")
    @classmethod
    def validate_unique_positive_ids(cls, value: List[int]) -> List[int]:
        if any(sid <= 0 for sid in value):
            raise ValueError("All seat_ids must be positive integers")
        if len(set(value)) != len(value):
            raise ValueError("Duplicate seat_ids are not allowed in a single reservation")
        return value


class AutoAllocateSeatsRequest(BaseModel):
    """區域自動配位購買請求（單次 1~4 張）"""

    zone_code: str = Field(..., min_length=1, max_length=50, description="目標區域代碼")
    quantity: int = Field(..., ge=1, le=4, description="購買張數（1~4 張）")

    @field_validator("zone_code")
    @classmethod
    def strip_zone_code(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("zone_code must not be blank")
        return cleaned


class OrderItemDetailResponse(BaseModel):
    """訂單座位明細回應"""

    model_config = ConfigDict(from_attributes=True)

    id: int
    seat_id: int
    zone_code: str
    row_label: str
    seat_number: str
    price: Decimal
    status: str


class OrderReservationResponse(BaseModel):
    """搶票預訂成功訂單回應"""

    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: int
    user_id: int
    status: str
    total_ticket_amount: Decimal
    service_fee: Decimal
    total_amount: Decimal
    expires_at: datetime
    created_at: datetime
    items: List[OrderItemDetailResponse]


class TicketMetricsResponse(BaseModel):
    """業務層搶票統計指標回應"""

    reservation_success: int
    seat_conflict: int
    zone_sold_out: int
    limit_exceeded: int
