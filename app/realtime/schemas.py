"""即時座位推播與斷線補償 Pydantic 結構"""

from datetime import datetime
from typing import Any, Dict, List
from pydantic import BaseModel, ConfigDict


class EventOutboxDeltaItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: int
    sequence_id: int
    event_type: str
    payload: Dict[str, Any]
    created_at: datetime


class EventUpdatesRecoveryResponse(BaseModel):
    event_id: int
    current_last_seq: int
    requested_after_seq: int
    resync_required: bool
    events: List[EventOutboxDeltaItem]
