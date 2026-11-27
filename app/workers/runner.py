"""非同步背景 Worker 服務

職責包含：
1. 以 FOR UPDATE SKIP LOCKED 輪詢 event_outbox (status = 'pending') 並延續 X-Request-ID
2. 將事件發布至 Redis Pub/Sub 頻道，發布成功後才標記為 published
"""

import asyncio
import json
import logging
from typing import Optional

import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import request_id_ctx, setup_logging
from app.core.redis import close_redis, get_redis
from app.events.import_service import recover_stale_import_jobs
from app.payments.service import release_expired_orders
from app.models import EventOutbox, OutboxStatus
from app.db import AsyncSessionLocal

setup_logging()
logger = logging.getLogger("flash_ticket.worker")
logger.setLevel(logging.INFO)


async def process_outbox_event(
    event: EventOutbox,
    session: Optional[AsyncSession] = None,
    redis: Optional[aioredis.Redis] = None,
) -> bool:
    """
    處理單筆 Outbox 事件：還原 request_id、發布至 Redis Pub/Sub，成功後才標記為 PUBLISHED。
    若發布失敗則保留 PENDING 狀態待下輪重試。
    註：Redis Pub/Sub 僅會投遞給當下在線訂閱者，不保留離線訊息。
    """
    payload = event.payload or {}
    req_id = payload.get("request_id") or "-"
    redis_client = redis or get_redis()

    token = request_id_ctx.set(req_id)
    try:
        channel = f"event:{event.event_id}:updates"
        message = json.dumps(
            {
                "outbox_id": event.id,
                "event_id": event.event_id,
                "sequence_id": event.sequence_id,
                "event_type": event.event_type,
                "payload": payload,
            }
        )
        await redis_client.publish(channel, message)
        logger.info(
            f"Relaying outbox event id={event.id} type={event.event_type} "
            f"event_id={event.event_id} sequence_id={event.sequence_id}"
        )
        event.status = OutboxStatus.PUBLISHED
        return True
    except Exception as e:
        logger.error(
            f"Failed to publish outbox event id={event.id} event_id={event.event_id}: {e}",
            exc_info=True,
        )
        return False
    finally:
        request_id_ctx.reset(token)


async def poll_and_relay_outbox(
    session: AsyncSession,
    limit: int = 100,
    redis: Optional[aioredis.Redis] = None,
) -> int:
    """
    輪詢待發布事件並批次發布；使用 FOR UPDATE SKIP LOCKED 避免多 Worker 重複認領同一事件。
    """
    stmt = (
        select(EventOutbox)
        .where(EventOutbox.status == OutboxStatus.PENDING)
        .order_by(EventOutbox.created_at.asc(), EventOutbox.id.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    res = await session.execute(stmt)
    events = res.scalars().all()

    if not events:
        return 0

    published_count = 0
    for event in events:
        if await process_outbox_event(event, session, redis=redis):
            published_count += 1

    await session.commit()
    return published_count


async def main():
    logger.info("Background worker service initialized and running.")
    try:
        while True:
            try:
                async with AsyncSessionLocal() as session:
                    processed = await poll_and_relay_outbox(session)
                    if processed > 0:
                        logger.debug(f"Relayed {processed} outbox events.")
                    recovered = await recover_stale_import_jobs(session)
                    if recovered > 0:
                        logger.warning(f"Recovered {recovered} stale import jobs as failed.")
                    released = await release_expired_orders(session)
                    if released > 0:
                        logger.info(f"Released {released} expired unpaid orders via DB clock.")
            except Exception as e:
                logger.error(f"Worker encountered error: {e}", exc_info=True)

            await asyncio.sleep(1.0)
    finally:
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
