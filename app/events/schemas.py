from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _require_non_empty(value: str, field_name: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} cannot be blank")
    return stripped


def _require_tz_aware_utc(dt: datetime, field_name: str) -> datetime:
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError(f"{field_name} must include timezone information")
    return dt.astimezone(timezone.utc)


class ZoneCreateRequest(BaseModel):
    zone_code: str = Field(..., min_length=1, max_length=50, description="區域代碼 (如 VIP, A)")
    name: str = Field(..., min_length=1, max_length=100, description="區域名稱")
    price: Decimal = Field(..., ge=0, max_digits=10, decimal_places=2, description="區域票價")

    @field_validator("zone_code")
    @classmethod
    def validate_zone_code(cls, v: str) -> str:
        return _require_non_empty(v, "zone_code")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        return _require_non_empty(v, "name")


class ZoneResponse(BaseModel):
    id: int
    event_id: int
    zone_code: str
    name: str
    price: Decimal
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class EventCreateRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=255, description="活動標題")
    description: Optional[str] = Field(None, description="活動詳細描述")
    category: str = Field(..., min_length=1, max_length=100, description="活動分類")
    venue: str = Field(..., min_length=1, max_length=255, description="舉辦場地")
    start_time: datetime = Field(..., description="開賣/開始時間")
    end_time: datetime = Field(..., description="結束時間")
    zones: List[ZoneCreateRequest] = Field(..., min_length=1, description="區域與票價設定")

    @field_validator("title", "category", "venue")
    @classmethod
    def validate_non_blank(cls, v: str, info) -> str:
        return _require_non_empty(v, info.field_name)

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_tz_aware_datetimes(cls, v: datetime, info) -> datetime:
        return _require_tz_aware_utc(v, info.field_name)

    @model_validator(mode="after")
    def validate_event_constraints(self) -> "EventCreateRequest":
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        seen_codes = set()
        for zone in self.zones:
            if zone.zone_code in seen_codes:
                raise ValueError(f"Duplicate zone_code in request: {zone.zone_code}")
            seen_codes.add(zone.zone_code)
        return self


class EventUpdateRequest(BaseModel):
    version: int = Field(..., ge=1, description="目前資料版本號，用於樂觀鎖防並行覆蓋")
    title: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    category: Optional[str] = Field(None, min_length=1, max_length=100)
    venue: Optional[str] = Field(None, min_length=1, max_length=255)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    zones: Optional[List[ZoneCreateRequest]] = None

    @field_validator("title", "category", "venue")
    @classmethod
    def validate_optional_non_blank(cls, v: Optional[str], info) -> Optional[str]:
        if v is None:
            return None
        return _require_non_empty(v, info.field_name)

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_optional_tz_aware_datetimes(
        cls, v: Optional[datetime], info
    ) -> Optional[datetime]:
        if v is None:
            return None
        return _require_tz_aware_utc(v, info.field_name)

    @model_validator(mode="after")
    def validate_update_constraints(self) -> "EventUpdateRequest":
        if self.start_time is not None and self.end_time is not None:
            if self.end_time <= self.start_time:
                raise ValueError("end_time must be after start_time")
        if self.zones is not None:
            if len(self.zones) == 0:
                raise ValueError("zones must contain at least one zone when provided")
            seen_codes = set()
            for zone in self.zones:
                if zone.zone_code in seen_codes:
                    raise ValueError(f"Duplicate zone_code in request: {zone.zone_code}")
                seen_codes.add(zone.zone_code)
        return self


class EventResponse(BaseModel):
    id: int
    creator_id: int
    title: str
    description: Optional[str]
    category: str
    venue: str
    start_time: datetime
    end_time: datetime
    status: str
    last_seq: int
    version: int
    created_at: datetime
    updated_at: datetime
    zones: List[ZoneResponse]

    model_config = ConfigDict(from_attributes=True)


class EventListResponse(BaseModel):
    items: List[EventResponse]
    next_cursor: Optional[str] = None
    has_more: bool


class ZoneSeatSummary(BaseModel):
    zone_code: str
    name: str
    price: Decimal
    total_seats: int
    available_seats: int
    held_seats: int
    sold_seats: int


class SeatSnapshotItem(BaseModel):
    id: int
    zone_code: str
    row_label: str
    seat_number: str
    status: str
    version: int

    model_config = ConfigDict(from_attributes=True)


class EventSeatsSnapshotResponse(BaseModel):
    event_id: int
    status: str
    last_seq: int
    total_seats: int
    zones: List[ZoneSeatSummary]
    seats: List[SeatSnapshotItem]
    next_after_id: Optional[int] = None
    has_more: bool


class ImportErrorItem(BaseModel):
    row: int = Field(..., description="原始列號 (CSV 由第 2 列起算，JSON 由第 1 項起算)")
    reason: str = Field(..., description="錯誤原因")


class ImportJobResponse(BaseModel):
    id: int
    event_id: int
    user_id: int
    file_sha256: str
    filename: str
    total_rows: int
    imported_rows: int
    error_rows: int
    mode: str
    status: str
    errors: List[ImportErrorItem]
    is_duplicate: bool = False
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
