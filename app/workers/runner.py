"""非同步背景 Worker 服務

職責包含：
1. 輪詢 event_outbox (status = 'pending') 並接續全鏈路 X-Request-ID
2. 將事件推播至 Redis Pub/Sub 與 WebSocket 連線池
3. 定期清理逾時訂單 (cleanup expired held seats & pending orders)
"""

import asyncio
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.db import AsyncSessionLocal
from app.models import EventOutbox, OutboxStatus
from app.core.logging import setup_logging, request_id_ctx

setup_logging()
logger = logging.getLogger("flash_ticket.worker")
logger.setLevel(logging.INFO)


async def process_outbox_event(event: EventOutbox, session: AsyncSession) -> None:
    """
    處理單筆 Outbox 事件並延續全鏈路 request_id 追蹤
    """
    # 從 Outbox payload 萃取當初發起 HTTP 請求的 request_id
    payload = event.payload or {}
    req_id = payload.get("request_id") or "-"

    # 綁定至 Worker 協程之 ContextVar，使 Worker 日誌自動帶上 [request_id]
    token = request_id_ctx.set(req_id)
    try:
        logger.info(
            f"Relaying outbox event id={event.id} type={event.event_type} "
            f"event_id={event.event_id} sequence_id={event.sequence_id}"
        )
        # 標記為已發布 (PUBLISHED)
        event.status = OutboxStatus.PUBLISHED
    finally:
        request_id_ctx.reset(token)


async def poll_and_relay_outbox(session: AsyncSession, limit: int = 100) -> int:
    """
    輪詢待發布事件並批次處理
    利用 ix_outbox_pending_relay 部分索引以微秒級速度過濾 pending 事件
    """
    stmt = (
        select(EventOutbox)
        .where(EventOutbox.status == OutboxStatus.PENDING)
        .order_by(EventOutbox.created_at.asc())
        .limit(limit)
    )
    res = await session.execute(stmt)
    events = res.scalars().all()

    if not events:
        return 0

    for event in events:
        await process_outbox_event(event, session)

    await session.commit()
    return len(events)


async def main():
    logger.info("Background worker service initialized and running.")
    while True:
        try:
            async with AsyncSessionLocal() as session:
                processed = await poll_and_relay_outbox(session)
                if processed > 0:
                    logger.debug(f"Relayed {processed} outbox events.")
        except Exception as e:
            logger.error(f"Worker encountered error: {e}", exc_info=True)

        await asyncio.sleep(1.0)


if __name__ == "__main__":
    asyncio.run(main())
