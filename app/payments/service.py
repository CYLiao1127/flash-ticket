"""金流付款處理、Webhook 冪等狀態轉換、訂單查詢／自主取消與 DB-Clock 逾時釋放服務 (Commit 15)"""

import json
import logging
import time
import uuid
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from fastapi import HTTPException, status
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.outbox import create_outbox_event
from app.core.redis import get_redis
from app.models import (
    Order,
    OrderItem,
    OrderItemStatus,
    OrderStatus,
    Seat,
    SeatStatus,
    User,
    UserRole,
)
from app.payments.schemas import (
    MockPaymentResponse,
    OrderDetailResponse,
    OrderListResponse,
    PaymentWebhookPayload,
    PaymentWebhookResponse,
)
from app.payments.signature import compute_webhook_signature
from app.tickets.schemas import OrderItemDetailResponse

logger = logging.getLogger("flash_ticket.payments")

WEBHOOK_PROCESSING_LOCK_TTL_SECONDS = 60  # 處理中鎖短 TTL，避免程序異常中斷後長時間卡住 409
WEBHOOK_COMPLETED_CACHE_TTL_SECONDS = 86400  # 已完成結果快取 24 小時
MOCK_REFUND_RECEIPT_TTL_SECONDS = 86400 * 7  # 模擬退款收據保留 7 天（冪等防重複退款）


async def execute_mock_refund(
    order_id: int,
    amount: str,
    webhook_event_id: Optional[str] = None,
    redis=None,
) -> Dict[str, object]:
    """
    模擬金流自動退款閘道（依 order_id 冪等執行，防止 Outbox 至少一次投遞造成重複退款）。
    回傳退款收據（首次執行 refunded=True, duplicate=False；重複呼叫回傳既有收據 duplicate=True）。
    """
    client = redis or get_redis()
    receipt_key = f"payment:refund:order:{order_id}"
    receipt = {
        "refund_id": f"ref_{uuid.uuid4().hex[:16]}",
        "order_id": order_id,
        "amount": str(amount),
        "webhook_event_id": webhook_event_id,
        "status": "refunded",
        "executed_at": int(time.time()),
    }
    created = bool(
        await client.set(
            receipt_key,
            json.dumps(receipt),
            nx=True,
            ex=MOCK_REFUND_RECEIPT_TTL_SECONDS,
        )
    )
    if created:
        logger.info(
            f"[MockGateway] Executed automatic refund refund_id={receipt['refund_id']} "
            f"order_id={order_id} amount={amount}"
        )
        return {**receipt, "duplicate": False}

    existing_raw = await client.get(receipt_key)
    existing = json.loads(existing_raw) if existing_raw else receipt
    return {**existing, "duplicate": True}

_RELEASE_WEBHOOK_PROCESSING_LUA = """
local val = redis.call('GET', KEYS[1])
if not val then
    return 0
end
if string.find(val, '"state": "processing"', 1, true) then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


async def _allocate_event_sequence(db: AsyncSession, event_id: int) -> int:
    """在交易最後一步原子遞增活動專屬序號 (Q4.1)"""
    res = await db.execute(
        text(
            "UPDATE events "
            "SET last_seq = last_seq + 1 "
            "WHERE id = :event_id "
            "RETURNING last_seq"
        ),
        {"event_id": event_id},
    )
    return int(res.scalar_one())


async def _load_order_items_with_seats(
    db: AsyncSession,
    order_ids: List[int],
) -> Dict[int, List[Tuple[OrderItem, Seat]]]:
    """批次載入訂單對應的 OrderItem 與 Seat，避免 N+1 查詢"""
    if not order_ids:
        return {}
    rows = (
        await db.execute(
            select(OrderItem, Seat)
            .join(Seat, Seat.id == OrderItem.seat_id)
            .where(OrderItem.order_id.in_(order_ids))
            .order_by(OrderItem.id.asc())
        )
    ).all()
    grouped: Dict[int, List[Tuple[OrderItem, Seat]]] = {oid: [] for oid in order_ids}
    for item, seat in rows:
        grouped[item.order_id].append((item, seat))
    return grouped


def _serialize_order(
    order: Order,
    pairs: List[Tuple[OrderItem, Seat]],
) -> OrderDetailResponse:
    return OrderDetailResponse(
        id=order.id,
        event_id=order.event_id,
        user_id=order.user_id,
        status=order.status,
        idempotency_key=order.idempotency_key,
        total_ticket_amount=Decimal(order.total_ticket_amount),
        service_fee=Decimal(order.service_fee),
        total_amount=Decimal(order.total_amount),
        expires_at=order.expires_at,
        created_at=order.created_at,
        updated_at=order.updated_at,
        items=[
            OrderItemDetailResponse(
                id=item.id,
                seat_id=item.seat_id,
                zone_code=seat.zone_code,
                row_label=seat.row_label,
                seat_number=seat.seat_number,
                price=Decimal(item.price),
                status=item.status,
            )
            for item, seat in pairs
        ],
    )


async def list_orders(
    db: AsyncSession,
    current_user: User,
    status_filter: Optional[OrderStatus] = None,
    event_id: Optional[int] = None,
    limit: int = 50,
) -> OrderListResponse:
    """
    查詢訂單清單：
    - User 僅能查詢自己的訂單（走 ix_orders_user_created 索引）
    - Admin / Viewer 可查詢全站訂單
    """
    stmt = select(Order)
    if current_user.role == UserRole.USER.value:
        stmt = stmt.where(Order.user_id == current_user.id)
    if status_filter is not None:
        stmt = stmt.where(Order.status == status_filter.value)
    if event_id is not None:
        stmt = stmt.where(Order.event_id == event_id)
    stmt = stmt.order_by(Order.created_at.desc(), Order.id.desc()).limit(limit)

    orders = list((await db.execute(stmt)).scalars().all())
    grouped = await _load_order_items_with_seats(db, [o.id for o in orders])
    serialized = [_serialize_order(o, grouped.get(o.id, [])) for o in orders]
    return OrderListResponse(items=serialized, total=len(serialized))


async def get_order_detail(
    db: AsyncSession,
    order_id: int,
    current_user: User,
) -> OrderDetailResponse:
    """查詢單筆訂單明細（User 僅限本人訂單；Admin / Viewer 可檢視全部）"""
    order = (
        await db.execute(select(Order).where(Order.id == order_id))
    ).scalar_one_or_none()
    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found",
        )
    if (
        current_user.role == UserRole.USER.value
        and order.user_id != current_user.id
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view this order",
        )

    grouped = await _load_order_items_with_seats(db, [order.id])
    return _serialize_order(order, grouped.get(order.id, []))


async def cancel_order(
    db: AsyncSession,
    order_id: int,
    current_user: User,
) -> OrderDetailResponse:
    """
    使用者自主取消待付款訂單（僅限訂單本人或 Admin）：
    於同一 DB 事務內鎖定訂單，同步將 order、order_items 標記為 cancelled、釋放 seats 為 available，
    並遞增 events.last_seq 寫入 seat_released Outbox 事件。
    """
    order = (
        await db.execute(
            select(Order).where(Order.id == order_id).with_for_update()
        )
    ).scalar_one_or_none()
    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found",
        )
    if (
        current_user.role != UserRole.ADMIN.value
        and order.user_id != current_user.id
    ):
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to cancel this order",
        )
    if order.status != OrderStatus.PENDING.value:
        current_status = order.status
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Only pending orders can be cancelled (current status={current_status})",
        )

    pairs = (await _load_order_items_with_seats(db, [order.id])).get(order.id, [])
    seat_ids = sorted(seat.id for _, seat in pairs)

    # 依固定順序鎖定座位並釋放回 available
    released_seats_payload = []
    if seat_ids:
        seats_locked = list(
            (
                await db.execute(
                    select(Seat)
                    .where(Seat.id.in_(seat_ids))
                    .order_by(Seat.id.asc())
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            )
            .scalars()
            .all()
        )
        for seat in seats_locked:
            if seat.status == SeatStatus.HELD.value and seat.held_by == order.user_id:
                seat.status = SeatStatus.AVAILABLE.value
                seat.held_by = None
                seat.expires_at = None
                seat.version += 1
                seat.updated_at = func.now()
                released_seats_payload.append(
                    {
                        "seat_id": seat.id,
                        "zone_code": seat.zone_code,
                        "row_label": seat.row_label,
                        "seat_number": seat.seat_number,
                        "status": SeatStatus.AVAILABLE.value,
                    }
                )

    for item, _ in pairs:
        if item.status == OrderItemStatus.HELD.value:
            item.status = OrderItemStatus.CANCELLED.value

    order.status = OrderStatus.CANCELLED.value
    order.updated_at = func.now()

    seq_id = await _allocate_event_sequence(db, order.event_id)
    db.add(
        create_outbox_event(
            event_id=order.event_id,
            sequence_id=seq_id,
            event_type="seat_released",
            payload={
                "order_id": order.id,
                "user_id": order.user_id,
                "event_id": order.event_id,
                "reason": "user_cancelled",
                "seats": released_seats_payload,
            },
        )
    )

    await db.commit()
    await db.refresh(order)
    refreshed_pairs = (await _load_order_items_with_seats(db, [order.id])).get(
        order.id, []
    )
    return _serialize_order(order, refreshed_pairs)


async def process_payment_webhook(
    db: AsyncSession,
    payload: PaymentWebhookPayload,
) -> PaymentWebhookResponse:
    """
    處理已通過 HMAC-SHA256 驗證之金流 Webhook 回呼：
    1. 透過 Redis SET NX 依 webhook_event_id 實現冪等處理（相同事件重送直接回傳快取結果）。
    2. 交易內 FOR UPDATE 鎖定訂單，若為 pending + succeeded 則原子轉為 paid、座位轉 sold 並寫入 Outbox。
    """
    redis = get_redis()
    idem_key = f"webhook:payment:event:{payload.webhook_event_id}"
    acquired_key = False

    try:
        acquired_key = bool(
            await redis.set(
                idem_key,
                json.dumps({"state": "processing"}),
                nx=True,
                ex=WEBHOOK_PROCESSING_LOCK_TTL_SECONDS,
            )
        )
        if not acquired_key:
            cached_raw = await redis.get(idem_key)
            if cached_raw:
                cached = json.loads(cached_raw)
                if cached.get("state") == "completed" and "result" in cached:
                    resp = PaymentWebhookResponse.model_validate(cached["result"])
                    resp.duplicate = True
                    return resp
            # 仍在處理中
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Webhook event is currently being processed",
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(f"Redis webhook idempotency unavailable: {exc}")

    try:
        order = (
            await db.execute(
                select(Order)
                .where(Order.id == payload.order_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if order is None:
            await db.rollback()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Order not found",
            )

        order_id_val = order.id
        order_status_val = order.status
        order_total_val = Decimal(order.total_amount)

        if payload.amount is not None and Decimal(payload.amount) != order_total_val:
            await db.rollback()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Payment amount mismatch: expected {order_total_val}, "
                    f"got {payload.amount}"
                ),
            )

        seq_id: Optional[int] = None
        action = "ignored"
        refund_triggered = False

        if payload.payment_status == "failed":
            action = "payment_failed_recorded"
            await db.rollback()
        elif order_status_val == OrderStatus.PAID.value:
            action = "already_paid"
            await db.rollback()
        elif order_status_val == OrderStatus.CANCELLED_REFUND_REQUIRED.value:
            action = "already_refund_required"
            refund_triggered = True
            await db.rollback()
        elif order_status_val == OrderStatus.PENDING.value:
            pairs = (await _load_order_items_with_seats(db, [order_id_val])).get(
                order_id_val, []
            )
            seat_ids = sorted(seat.id for _, seat in pairs)
            sold_seats_payload = []

            if seat_ids:
                seats_locked = list(
                    (
                        await db.execute(
                            select(Seat)
                            .where(Seat.id.in_(seat_ids))
                            .order_by(Seat.id.asc())
                            .with_for_update()
                            .execution_options(populate_existing=True)
                        )
                    )
                    .scalars()
                    .all()
                )
                for seat in seats_locked:
                    seat.status = SeatStatus.SOLD.value
                    seat.held_by = order.user_id
                    seat.expires_at = None
                    seat.version += 1
                    seat.updated_at = func.now()
                    sold_seats_payload.append(
                        {
                            "seat_id": seat.id,
                            "zone_code": seat.zone_code,
                            "row_label": seat.row_label,
                            "seat_number": seat.seat_number,
                            "status": SeatStatus.SOLD.value,
                        }
                    )

            for item, _ in pairs:
                item.status = OrderItemStatus.PAID.value

            order.status = OrderStatus.PAID.value
            order.updated_at = func.now()

            seq_id = await _allocate_event_sequence(db, order.event_id)
            db.add(
                create_outbox_event(
                    event_id=order.event_id,
                    sequence_id=seq_id,
                    event_type="seat_sold",
                    payload={
                        "order_id": order_id_val,
                        "user_id": order.user_id,
                        "event_id": order.event_id,
                        "webhook_event_id": payload.webhook_event_id,
                        "seats": sold_seats_payload,
                    },
                )
            )
            await db.commit()
            order_status_val = OrderStatus.PAID.value
            action = "paid"
        elif order_status_val == OrderStatus.CANCELLED.value:
            # ★ Q2.1 逾時取消後遲到 Webhook 的「復活 or 退款」單一交易決策：
            # 1. 鎖定原訂單所有座位 (ORDER BY id ASC FOR UPDATE 防止死結)。
            # 2. 若全部原座位仍為 available -> 重新佔位轉 sold、訂單復活為 paid。
            # 3. 若任一原座位已被他人鎖定或售出 -> 整筆轉 cancelled_refund_required 觸發退款，絕不部分復活或覆寫他人座位。
            pairs = (await _load_order_items_with_seats(db, [order_id_val])).get(
                order_id_val, []
            )
            seat_ids = sorted(seat.id for _, seat in pairs)
            seats_locked: List[Seat] = []
            if seat_ids:
                seats_locked = list(
                    (
                        await db.execute(
                            select(Seat)
                            .where(Seat.id.in_(seat_ids))
                            .order_by(Seat.id.asc())
                            .with_for_update()
                            .execution_options(populate_existing=True)
                        )
                    )
                    .scalars()
                    .all()
                )

            all_available = (
                len(seat_ids) > 0
                and len(seats_locked) == len(seat_ids)
                and all(s.status == SeatStatus.AVAILABLE.value for s in seats_locked)
            )

            if all_available:
                sold_seats_payload = []
                for seat in seats_locked:
                    seat.status = SeatStatus.SOLD.value
                    seat.held_by = order.user_id
                    seat.expires_at = None
                    seat.version += 1
                    seat.updated_at = func.now()
                    sold_seats_payload.append(
                        {
                            "seat_id": seat.id,
                            "zone_code": seat.zone_code,
                            "row_label": seat.row_label,
                            "seat_number": seat.seat_number,
                            "status": SeatStatus.SOLD.value,
                        }
                    )

                for item, _ in pairs:
                    item.status = OrderItemStatus.PAID.value

                order.status = OrderStatus.PAID.value
                order.updated_at = func.now()

                seq_id = await _allocate_event_sequence(db, order.event_id)
                db.add(
                    create_outbox_event(
                        event_id=order.event_id,
                        sequence_id=seq_id,
                        event_type="seat_sold",
                        payload={
                            "order_id": order_id_val,
                            "user_id": order.user_id,
                            "event_id": order.event_id,
                            "webhook_event_id": payload.webhook_event_id,
                            "revived": True,
                            "seats": sold_seats_payload,
                        },
                    )
                )
                await db.commit()
                order_status_val = OrderStatus.PAID.value
                action = "revived"
                logger.info(
                    f"Late webhook revived cancelled order_id={order_id_val} "
                    f"(seats={seat_ids}, sequence_id={seq_id})"
                )
            else:
                conflicting_seat_ids = [
                    s.id
                    for s in seats_locked
                    if s.status != SeatStatus.AVAILABLE.value
                ]
                order.status = OrderStatus.CANCELLED_REFUND_REQUIRED.value
                order.updated_at = func.now()

                seq_id = await _allocate_event_sequence(db, order.event_id)
                db.add(
                    create_outbox_event(
                        event_id=order.event_id,
                        sequence_id=seq_id,
                        event_type="order_refund_required",
                        payload={
                            "order_id": order_id_val,
                            "user_id": order.user_id,
                            "event_id": order.event_id,
                            "amount": str(order_total_val),
                            "webhook_event_id": payload.webhook_event_id,
                            "conflicting_seat_ids": conflicting_seat_ids,
                        },
                    )
                )
                await db.commit()
                order_status_val = OrderStatus.CANCELLED_REFUND_REQUIRED.value
                action = "refund_required"
                refund_triggered = True
                logger.warning(
                    f"Late webhook for cancelled order_id={order_id_val} could not revive "
                    f"(conflicting_seats={conflicting_seat_ids}); triggered refund amount={order_total_val}"
                )

        result = PaymentWebhookResponse(
            webhook_event_id=payload.webhook_event_id,
            order_id=order_id_val,
            order_status=order_status_val,
            action=action,
            duplicate=False,
            refund_triggered=refund_triggered,
            sequence_id=seq_id,
        )
        try:
            await redis.set(
                idem_key,
                json.dumps(
                    {
                        "state": "completed",
                        "result": result.model_dump(mode="json"),
                    }
                ),
                ex=WEBHOOK_COMPLETED_CACHE_TTL_SECONDS,
            )
        except Exception as exc:
            logger.warning(f"Failed to cache webhook result: {exc}")

        return result
    except Exception:
        if acquired_key:
            try:
                await redis.eval(_RELEASE_WEBHOOK_PROCESSING_LUA, 1, idem_key)
            except Exception:
                pass
        raise


async def trigger_mock_payment(
    db: AsyncSession,
    current_user: User,
    order_id: int,
    outcome: str = "succeeded",
    webhook_event_id: Optional[str] = None,
) -> MockPaymentResponse:
    """模擬金流閘道：驗證訂單歸屬、簽發 HMAC-SHA256 Webhook 並直接回呼處理"""
    order = (
        await db.execute(select(Order).where(Order.id == order_id))
    ).scalar_one_or_none()
    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found",
        )
    if (
        current_user.role != UserRole.ADMIN.value
        and order.user_id != current_user.id
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to pay for this order",
        )
    if order.status != OrderStatus.PENDING.value:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Order is not in pending status (current={order.status})",
        )

    event_id_str = webhook_event_id or f"wh_{uuid.uuid4().hex}"
    payload = PaymentWebhookPayload(
        webhook_event_id=event_id_str,
        order_id=order.id,
        payment_status=outcome,
        amount=Decimal(order.total_amount),
    )
    raw_body = payload.model_dump_json().encode("utf-8")
    ts_str = str(int(time.time()))
    sig = compute_webhook_signature(ts_str, raw_body)

    webhook_result = await process_payment_webhook(db, payload)
    return MockPaymentResponse(
        webhook_event_id=event_id_str,
        timestamp=ts_str,
        signature=sig,
        webhook_result=webhook_result,
    )


async def release_expired_orders(
    session: AsyncSession,
    limit: int = 50,
) -> int:
    """
    DB-Clock 逾時訂單自動釋放 Worker (Q2.3, Q2.4)：
    - 完全依據 PostgreSQL 伺服器時鐘 NOW() 判定過期 (`WHERE status = 'pending' AND expires_at < NOW()`)，
      不依賴 Python 本地時鐘，避免多副本時鐘偏差。
    - 使用 FOR UPDATE SKIP LOCKED 保證多 Worker 同時掃描時完全冪等不重複釋放。
    - 同事務內更新 orders='cancelled'、order_items='cancelled'、seats='available' 並寫入 Outbox。
    """
    stmt = (
        select(Order)
        .where(
            Order.status == OrderStatus.PENDING.value,
            Order.expires_at < func.now(),
        )
        .order_by(Order.expires_at.asc(), Order.id.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
        .execution_options(populate_existing=True)
    )
    expired_orders = list((await session.execute(stmt)).scalars().all())
    if not expired_orders:
        return 0

    grouped = await _load_order_items_with_seats(
        session, [o.id for o in expired_orders]
    )

    for order in expired_orders:
        pairs = grouped.get(order.id, [])
        seat_ids = sorted(seat.id for _, seat in pairs)
        released_seats_payload = []

        if seat_ids:
            seats_locked = list(
                (
                    await session.execute(
                        select(Seat)
                        .where(Seat.id.in_(seat_ids))
                        .order_by(Seat.id.asc())
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                )
                .scalars()
                .all()
            )
            for seat in seats_locked:
                if (
                    seat.status == SeatStatus.HELD.value
                    and seat.held_by == order.user_id
                ):
                    seat.status = SeatStatus.AVAILABLE.value
                    seat.held_by = None
                    seat.expires_at = None
                    seat.version += 1
                    seat.updated_at = func.now()
                    released_seats_payload.append(
                        {
                            "seat_id": seat.id,
                            "zone_code": seat.zone_code,
                            "row_label": seat.row_label,
                            "seat_number": seat.seat_number,
                            "status": SeatStatus.AVAILABLE.value,
                        }
                    )

        for item, _ in pairs:
            if item.status == OrderItemStatus.HELD.value:
                item.status = OrderItemStatus.CANCELLED.value

        order.status = OrderStatus.CANCELLED.value
        order.updated_at = func.now()

        seq_id = await _allocate_event_sequence(session, order.event_id)
        session.add(
            create_outbox_event(
                event_id=order.event_id,
                sequence_id=seq_id,
                event_type="seat_released",
                payload={
                    "order_id": order.id,
                    "user_id": order.user_id,
                    "event_id": order.event_id,
                    "reason": "payment_timeout",
                    "seats": released_seats_payload,
                },
            )
        )

    await session.commit()
    return len(expired_orders)
