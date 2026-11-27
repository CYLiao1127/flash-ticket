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
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import request_id_ctx, setup_logging
from app.core.redis import close_redis, get_redis
from app.events.import_service import recover_stale_import_jobs
from app.monitoring.alerts import evaluate_and_dispatch_alerts
from app.monitoring.collector import (
    flush_access_logs_batch,
    flush_per_second_metrics,
)
from app.payments.service import execute_mock_refund, release_expired_orders
from app.models import EventOutbox, OutboxStatus
from app.db import AsyncSessionLocal

setup_logging()
logger = logging.getLogger("flash_ticket.worker")
logger.setLevel(logging.INFO)

# Outbox Relay 單一 Leader 選主鎖鍵（透過 PostgreSQL 交易級諮詢鎖避免多 Worker 同時撈取導致亂序，Q4）
OUTBOX_RELAY_LEADER_LOCK_KEY = 420001


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
        if event.event_type == "order_refund_required":
            await execute_mock_refund(
                order_id=int(payload["order_id"]),
                amount=str(payload.get("amount", "0.00")),
                webhook_event_id=payload.get("webhook_event_id"),
                redis=redis_client,
            )

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
    use_leader_election: bool = True,
    on_leader_acquired=None,
) -> int:
    """
    Leader-Elected Outbox Relay (Q4)：
    1. 透過 `pg_try_advisory_xact_lock` 選出單一 Leader Worker，非 Leader 立即跳過避免多實例併發發送造成亂序。
    2. 嚴格依 `(event_id ASC, sequence_id ASC, id ASC)` 撈取 `status = 'pending'` 事件並依序發布至 Redis Pub/Sub。
    3. 同一 `event_id` 若前序事件發布失敗，立即停止該活動後續序號的發布，防止跳號亂序。
    """
    if use_leader_election:
        acquired = (
            await session.execute(
                text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
                {"lock_key": OUTBOX_RELAY_LEADER_LOCK_KEY},
            )
        ).scalar_one()
        if not acquired:
            return 0
        if on_leader_acquired is not None:
            await on_leader_acquired()

    stmt = (
        select(EventOutbox)
        .where(EventOutbox.status == OutboxStatus.PENDING)
        .order_by(
            EventOutbox.event_id.asc(),
            EventOutbox.sequence_id.asc(),
            EventOutbox.id.asc(),
        )
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    res = await session.execute(stmt)
    events = res.scalars().all()

    if not events:
        if use_leader_election:
            await session.commit()
        return 0

    published_count = 0
    blocked_event_ids = set()
    for event in events:
        if event.event_id in blocked_event_ids:
            continue
        if await process_outbox_event(event, session, redis=redis):
            published_count += 1
        else:
            # 嚴格保持單一活動序號順序：前序失敗時暫停同活動後續事件
            blocked_event_ids.add(event.event_id)

    await session.commit()
    return published_count


async def run_worker_cycle(session: AsyncSession) -> dict:
    """執行單一輪背景 Worker 週期任務（Outbox 派發、逾時回收、監控入庫、即時異常告警）"""
    processed = await poll_and_relay_outbox(session)
    if processed > 0:
        logger.debug(f"Relayed {processed} outbox events.")
    recovered = await recover_stale_import_jobs(session)
    if recovered > 0:
        logger.warning(f"Recovered {recovered} stale import jobs as failed.")
    released = await release_expired_orders(session)
    if released > 0:
        logger.info(f"Released {released} expired unpaid orders via DB clock.")
    flushed_logs = await flush_access_logs_batch(session)
    if flushed_logs > 0:
        logger.debug(f"Flushed {flushed_logs} access logs to PostgreSQL.")
    flushed_metrics = await flush_per_second_metrics(session)
    if flushed_metrics > 0:
        logger.debug(f"Flushed {flushed_metrics} per-second metric buckets.")
    alerts = await evaluate_and_dispatch_alerts(session)
    if alerts:
        logger.warning(f"Dispatched {len(alerts)} realtime monitoring alerts.")
    return {
        "outbox_relayed": processed,
        "stale_imports_recovered": recovered,
        "expired_orders_released": released,
        "access_logs_flushed": flushed_logs,
        "metric_buckets_flushed": flushed_metrics,
        "alerts_dispatched": len(alerts),
    }


async def main():
    logger.info("Background worker service initialized and running.")
    try:
        while True:
            try:
                async with AsyncSessionLocal() as session:
                    await run_worker_cycle(session)
            except Exception as e:
                logger.error(f"Worker encountered error: {e}", exc_info=True)

            await asyncio.sleep(1.0)
    finally:
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
