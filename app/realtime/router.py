"""即時座位廣播 WebSocket 端點與斷線補償 API 路由 (Commit 20 / Q4)"""

import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect, status
import jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_current_user
from app.auth.security import decode_access_token
from app.auth.service import get_cached_token_version, set_cached_token_version
from app.config import settings
from app.core.redis import get_redis
from app.db import get_db
from app.models import Event, EventOutbox, User
from app.realtime.hub import WS_CLIENT_MAX_QUEUE_SIZE, event_hub
from app.realtime.schemas import EventOutboxDeltaItem, EventUpdatesRecoveryResponse

logger = logging.getLogger("flash_ticket.realtime.router")

realtime_router = APIRouter(prefix="/api/v1/events", tags=["Realtime"])
ws_router = APIRouter(tags=["Realtime WebSocket"])

# 補發上限嚴格小於客戶端佇列容量 (WS_CLIENT_MAX_QUEUE_SIZE=64)，預留握手與即時緩衝空間，超限引導拉快照
WS_MAX_CATCHUP_EVENTS = max(1, WS_CLIENT_MAX_QUEUE_SIZE - 16)
WS_CLOSE_UNAUTHORIZED = 4401
WS_CLOSE_NOT_FOUND = 4404


async def _authenticate_ws_token(
    token: Optional[str],
    db: AsyncSession,
) -> Optional[User]:
    """比照 HTTP get_current_user 驗證 WebSocket JWT 簽章、token_version 與 is_active (Q5.1 / Q5.2)"""
    if not token:
        return None
    try:
        payload = decode_access_token(token)
    except jwt.PyJWTError:
        return None

    sub = payload.get("sub")
    token_version = payload.get("token_version")
    if not sub or token_version is None:
        return None
    try:
        user_id = int(sub)
    except (ValueError, TypeError):
        return None

    redis = get_redis()
    cached_version: Optional[int] = None
    try:
        cached_version = await get_cached_token_version(redis, user_id)
    except Exception:
        cached_version = None

    if cached_version is not None and cached_version != token_version:
        return None

    user = (
        await db.execute(select(User).where(User.id == user_id))
    ).scalar_one_or_none()
    if not user or not user.is_active:
        return None
    if user.token_version != token_version:
        try:
            await set_cached_token_version(redis, user.id, user.token_version)
        except Exception:
            pass
        return None

    if cached_version is None:
        try:
            await set_cached_token_version(redis, user.id, user.token_version)
        except Exception:
            pass
    return user


@realtime_router.get(
    "/{event_id}/updates",
    response_model=EventUpdatesRecoveryResponse,
    summary="斷線補償查詢：拉取指定 sequence_id 之後的活動座位變更事件 (Q4)",
)
async def get_event_updates_recovery_endpoint(
    event_id: int,
    after_seq: int = Query(0, ge=0, description="客戶端最後已套用之活動序號"),
    limit: int = Query(200, ge=1, le=500, description="單次最多補償筆數"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    當前端偵測到 WebSocket 跳號 (`seq != last_seq + 1`) 或斷線重連時呼叫：
    - 若缺失事件量 <= limit 且 `after_seq <= current_last_seq`，回傳依 `sequence_id ASC` 排序的增量事件。
    - 若缺口過大（超過 limit）或 `after_seq > current_last_seq`，回傳 `resync_required=True`，
      引導客戶端重新呼叫 `GET /api/v1/events/{event_id}/seats` 拉取完整快照。
    """
    event = (
        await db.execute(select(Event).where(Event.id == event_id))
    ).scalar_one_or_none()
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event not found",
        )

    current_last_seq = int(event.last_seq or 0)
    if after_seq > current_last_seq:
        return EventUpdatesRecoveryResponse(
            event_id=event_id,
            current_last_seq=current_last_seq,
            requested_after_seq=after_seq,
            resync_required=True,
            events=[],
        )

    rows = list(
        (
            await db.execute(
                select(EventOutbox)
                .where(
                    EventOutbox.event_id == event_id,
                    EventOutbox.sequence_id > after_seq,
                )
                .order_by(EventOutbox.sequence_id.asc(), EventOutbox.id.asc())
                .limit(limit + 1)
            )
        )
        .scalars()
        .all()
    )

    if len(rows) > limit:
        return EventUpdatesRecoveryResponse(
            event_id=event_id,
            current_last_seq=current_last_seq,
            requested_after_seq=after_seq,
            resync_required=True,
            events=[],
        )

    return EventUpdatesRecoveryResponse(
        event_id=event_id,
        current_last_seq=current_last_seq,
        requested_after_seq=after_seq,
        resync_required=False,
        events=[EventOutboxDeltaItem.model_validate(r) for r in rows],
    )


@ws_router.websocket("/ws/events/{event_id}")
async def event_seats_websocket_endpoint(
    websocket: WebSocket,
    event_id: int,
    token: Optional[str] = Query(None, description="Bearer JWT Token"),
    last_seq: Optional[int] = Query(
        None, ge=0, description="斷線重連時攜帶最後收到的 sequence_id 以自動補償"
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    即時座位狀態 WebSocket 訂閱端點 (Q4)：
    - 支援透過 `?token=<jwt>` 或 `Authorization: Bearer <jwt>` 認證並檢查 token_version。
    - 連線後立即推送 `connected` 握手幀（含當前 `last_seq`）。
    - 若攜帶 `last_seq` 且落後於最新序號，自動補發缺失增量事件或提示 `resync_required` 重拉快照。
    - 透過 EventBroadcastHub 訂閱 Redis Pub/Sub 跨節點廣播，並保護慢客戶端不造成 OOM。
    """
    raw_token = token
    if not raw_token:
        auth_header = websocket.headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            raw_token = auth_header[7:].strip()

    user = await _authenticate_ws_token(raw_token, db)
    if not user:
        await websocket.close(code=WS_CLOSE_UNAUTHORIZED, reason="Unauthorized or revoked token")
        return

    event_exists = (
        await db.execute(select(Event.id).where(Event.id == event_id))
    ).scalar_one_or_none()
    if event_exists is None:
        await websocket.close(code=WS_CLOSE_NOT_FOUND, reason="Event not found")
        return

    # 先完成 accept 與 Redis Pub/Sub 訂閱就緒（Subscribe-Before-Snapshot），
    # 並啟用 `buffer_live_until_ready=True` 暫存期間湧入的即時事件，
    # 確保客戶端必先收到 `connected` -> `catchup`，再依序收到去重後的即時事件
    await websocket.accept()
    try:
        client = await event_hub.connect(
            event_id=event_id,
            websocket=websocket,
            max_queue_size=WS_CLIENT_MAX_QUEUE_SIZE,
            buffer_live_until_ready=True,
        )
    except TimeoutError:
        await websocket.close(
            code=1013, reason="Realtime subscription temporarily unavailable"
        )
        return

    try:
        current_last_seq = int(
            (
                await db.execute(
                    select(Event.last_seq)
                    .where(Event.id == event_id)
                    .execution_options(populate_existing=True)
                )
            ).scalar_one()
            or 0
        )

        # 1. 送出連線握手幀
        welcome = json.dumps(
            {
                "type": "connected",
                "event_id": event_id,
                "last_seq": current_last_seq,
                "server_instance": settings.INSTANCE_ID,
            }
        )
        client.try_enqueue(welcome)

        high_watermark = current_last_seq
        # 2. 若客戶端攜帶 last_seq（斷線重連），自動執行增量補償或通知重拉快照
        if last_seq is not None and last_seq != current_last_seq:
            if last_seq > current_last_seq or (current_last_seq - last_seq) > WS_MAX_CATCHUP_EVENTS:
                client.try_enqueue(
                    json.dumps(
                        {
                            "type": "resync_required",
                            "event_id": event_id,
                            "requested_after_seq": last_seq,
                            "current_last_seq": current_last_seq,
                            "snapshot_url": f"/api/v1/events/{event_id}/seats",
                        }
                    )
                )
            else:
                missed_rows = list(
                    (
                        await db.execute(
                            select(EventOutbox)
                            .where(
                                EventOutbox.event_id == event_id,
                                EventOutbox.sequence_id > last_seq,
                            )
                            .order_by(EventOutbox.sequence_id.asc(), EventOutbox.id.asc())
                            .limit(WS_MAX_CATCHUP_EVENTS + 1)
                        )
                    )
                    .scalars()
                    .all()
                )
                if len(missed_rows) > WS_MAX_CATCHUP_EVENTS:
                    client.try_enqueue(
                        json.dumps(
                            {
                                "type": "resync_required",
                                "event_id": event_id,
                                "requested_after_seq": last_seq,
                                "current_last_seq": current_last_seq,
                                "snapshot_url": f"/api/v1/events/{event_id}/seats",
                            }
                        )
                    )
                else:
                    for row in missed_rows:
                        delta_msg = json.dumps(
                            {
                                "type": "catchup",
                                "outbox_id": row.id,
                                "event_id": row.event_id,
                                "sequence_id": row.sequence_id,
                                "event_type": row.event_type,
                                "payload": row.payload or {},
                            }
                        )
                        if not client.try_enqueue(delta_msg):
                            await client.close(code=1008, reason="Slow client buffer overflow")
                            return
                        high_watermark = max(high_watermark, int(row.sequence_id))

        # 3. 依 sequence_id 去重並排序沖刷快照／補發期間暫存的即時事件，切換為即時串流模式
        if not client.flush_live_buffer(min_sequence_exclusive=high_watermark):
            await client.close(code=1008, reason="Slow client buffer overflow")
            return

        # 3. 維持連線並處理心跳 ping
        while not client.closed:
            incoming = await websocket.receive_text()
            if incoming.strip().lower() == "ping":
                client.try_enqueue(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.debug(f"WebSocket closed on event_id={event_id}: {exc}")
    finally:
        await client.stop_sender()
        await event_hub.disconnect(client)
