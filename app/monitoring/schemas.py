"""監控指標、存取日誌與全鏈路 Request Trace Pydantic 回應結構"""

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict


class SystemMetricItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    timestamp: datetime
    total_requests: int
    error_requests: int
    p50_latency_ms: Decimal
    p95_latency_ms: Decimal
    p99_latency_ms: Decimal
    active_connections: int


class MetricsSummaryResponse(BaseModel):
    window_seconds: int
    total_requests: int
    error_requests: int
    error_rate: float
    avg_rps: float
    max_p99_latency_ms: Decimal
    current_active_connections: int
    series: List[SystemMetricItemResponse]


class AccessLogItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    request_id: str
    user_id: Optional[int]
    method: str
    path: str
    status_code: int
    latency_ms: Decimal
    client_ip: Optional[str]
    created_at: datetime


class AccessLogListResponse(BaseModel):
    items: List[AccessLogItemResponse]
    total: int


class TraceOutboxItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: int
    sequence_id: int
    event_type: str
    status: str
    payload: Dict[str, Any]
    created_at: datetime


class RequestTraceResponse(BaseModel):
    request_id: str
    access_logs: List[AccessLogItemResponse]
    outbox_events: List[TraceOutboxItemResponse]
