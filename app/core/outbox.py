from typing import Any, Dict
from app.core.logging import get_request_id
from app.models import EventOutbox, OutboxStatus


def create_outbox_event(
    event_id: int,
    sequence_id: int,
    event_type: str,
    payload: Dict[str, Any],
) -> EventOutbox:
    """
    建立 Transactional Outbox 事件實例
    自動將當前 HTTP / 協程上下文之 request_id 注入 payload，
    確保領域事件於非同步 Relay 與推播至 WebSocket 時具備端到端全鏈路追蹤能力。
    """
    event_payload = dict(payload)
    if "request_id" not in event_payload:
        event_payload["request_id"] = get_request_id()

    return EventOutbox(
        event_id=event_id,
        sequence_id=sequence_id,
        event_type=event_type,
        payload=event_payload,
        status=OutboxStatus.PENDING,
    )
