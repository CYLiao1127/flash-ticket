"""核心搶票與防超賣預訂服務 (Commit 10)

核心並發與一致性設計：
1. 鎖順序嚴格固定以杜絕死結：
   - Step 1: pg_advisory_xact_lock(hashtext(user_id || '_' || event_id))（使用者單場限購 4 張原子序列化）
   - Step 2: seats 行級鎖（指定選位依 seat_id ASC 條件更新；自動配位以 FOR UPDATE SKIP LOCKED）
2. 雙重防超賣防線：
   - 第一道：條件原子更新 (`WHERE id = :sid AND status = 'available'`) / `FOR UPDATE SKIP LOCKED`
   - 第二道：order_items 部分唯一索引 `uq_active_seat_order (seat_id) WHERE status IN ('held', 'paid')`
3. 5% 服務費採嚴格四捨五入 (`ROUND_HALF_UP`) 至整數元，禁用 Python 銀行家捨入 `round()`。
4. 鎖位逾時時間統一由 PostgreSQL 伺服器時鐘 `NOW() + INTERVAL '10 minutes'` 產生。
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Tuple

from fastapi import HTTPException, status
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Event,
    EventStatus,
    Order,
    OrderItem,
    OrderItemStatus,
    OrderStatus,
    Seat,
    SeatStatus,
    User,
    Zone,
)
from app.tickets.metrics import record_ticket_metric
from app.tickets.schemas import OrderItemDetailResponse, OrderReservationResponse

MAX_TICKETS_PER_USER_PER_EVENT = 4
SEAT_HOLD_MINUTES = 10


def calculate_service_fee(subtotal: Decimal) -> Decimal:
    """
    計算 5% 服務費並嚴格四捨五入至整數元 (ROUND_HALF_UP)。
    禁用 Python 內建 round() 以避免銀行家捨入誤差（例如 2.5 必須進位為 3 而非 2）。
    """
    return (Decimal(subtotal) * Decimal("0.05")).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )


async def _acquire_user_event_advisory_lock(
    db: AsyncSession,
    user_id: int,
    event_id: int,
) -> None:
    """於當前交易內取得使用者-活動層級交易排他鎖，保證單人單場限購 4 張原子性 (Q3.2)"""
    lock_key = f"{user_id}_{event_id}"
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
        {"lock_key": lock_key},
    )


async def _assert_event_purchasable(db: AsyncSession, event_id: int) -> Event:
    """確認活動存在、已發布 (published) 且位於資料庫時鐘 NOW() 判定之開賣期間內 [start_time, end_time)"""
    row = (
        await db.execute(select(Event, func.now()).where(Event.id == event_id))
    ).first()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event not found",
        )
    event, db_now = row[0], row[1]
    if event.status != EventStatus.PUBLISHED.value:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Tickets can only be reserved for published events",
        )
    if db_now < event.start_time:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ticket sales have not started yet for this event",
        )
    if db_now >= event.end_time:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ticket sales have ended for this event",
        )
    return event


async def _count_user_active_tickets_in_event(
    db: AsyncSession,
    user_id: int,
    event_id: int,
) -> int:
    """查詢該使用者於指定活動中所有有效（held 或 paid）票券張數"""
    stmt = (
        select(func.count(OrderItem.id))
        .select_from(OrderItem)
        .join(Order, Order.id == OrderItem.order_id)
        .where(
            Order.user_id == user_id,
            Order.event_id == event_id,
            OrderItem.status.in_(
                [OrderItemStatus.HELD.value, OrderItemStatus.PAID.value]
            ),
        )
    )
    return int((await db.execute(stmt)).scalar_one())


async def _assert_within_per_user_limit(
    db: AsyncSession,
    user_id: int,
    event_id: int,
    requested_qty: int,
) -> None:
    """在 advisory lock 保護下驗證單人單場限購 4 張"""
    existing_qty = await _count_user_active_tickets_in_event(db, user_id, event_id)
    if existing_qty + requested_qty > MAX_TICKETS_PER_USER_PER_EVENT:
        await db.rollback()
        await record_ticket_metric("limit_exceeded", event_id=event_id)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Per-user ticket limit ({MAX_TICKETS_PER_USER_PER_EVENT}) exceeded "
                f"for this event (current={existing_qty}, requested={requested_qty})"
            ),
        )


def _build_reservation_response(
    order: Order,
    item_seat_pairs: List[Tuple[OrderItem, str, str, str]],
) -> OrderReservationResponse:
    return OrderReservationResponse(
        id=order.id,
        event_id=order.event_id,
        user_id=order.user_id,
        status=order.status,
        total_ticket_amount=Decimal(order.total_ticket_amount),
        service_fee=Decimal(order.service_fee),
        total_amount=Decimal(order.total_amount),
        expires_at=order.expires_at,
        created_at=order.created_at,
        items=[
            OrderItemDetailResponse(
                id=item.id,
                seat_id=item.seat_id,
                zone_code=zone_code,
                row_label=row_label,
                seat_number=seat_number,
                price=Decimal(item.price),
                status=item.status,
            )
            for item, zone_code, row_label, seat_number in item_seat_pairs
        ],
    )


async def reserve_specific_seats(
    db: AsyncSession,
    event_id: int,
    current_user: User,
    seat_ids: List[int],
) -> OrderReservationResponse:
    """
    指定選位購買（條件原子更新 + 單人單場限購 4 張 advisory lock）。
    依 seat_id 升冪排序鎖定座位，避免多座位並發指定選位發生死結。
    """
    sorted_seat_ids = sorted(seat_ids)
    await _assert_event_purchasable(db, event_id)

    # 1. 使用者-活動層級交易鎖，先序列化限購檢查
    await _acquire_user_event_advisory_lock(db, current_user.id, event_id)
    await _assert_within_per_user_limit(
        db, current_user.id, event_id, len(sorted_seat_ids)
    )

    # 2. 確認指定座位皆隸屬此活動
    existing_ids_rows = await db.execute(
        select(Seat.id).where(
            Seat.event_id == event_id,
            Seat.id.in_(sorted_seat_ids),
        )
    )
    existing_ids = {r[0] for r in existing_ids_rows.all()}
    if len(existing_ids) != len(sorted_seat_ids):
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="One or more requested seats do not exist in this event",
        )

    # 3. 依固定升冪順序逐一執行條件原子更新 (第一道防線)
    held_rows: List[Tuple[int, str, str, str, object]] = []
    update_sql = text(
        "UPDATE seats "
        "SET status = :held_status, "
        "    held_by = :user_id, "
        "    expires_at = NOW() + make_interval(mins => :hold_mins), "
        "    version = version + 1, "
        "    updated_at = NOW() "
        "WHERE id = :seat_id "
        "  AND event_id = :event_id "
        "  AND status = :avail_status "
        "RETURNING id, zone_code, row_label, seat_number, expires_at"
    )
    for sid in sorted_seat_ids:
        res = await db.execute(
            update_sql,
            {
                "held_status": SeatStatus.HELD.value,
                "user_id": current_user.id,
                "hold_mins": SEAT_HOLD_MINUTES,
                "seat_id": sid,
                "event_id": event_id,
                "avail_status": SeatStatus.AVAILABLE.value,
            },
        )
        row = res.first()
        if row is None:
            await db.rollback()
            await record_ticket_metric("seat_conflict", event_id=event_id)
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="One or more requested seats are no longer available",
            )
        held_rows.append((row[0], row[1], row[2], row[3], row[4]))

    # 4. 查詢分區票價並建立 Order / OrderItem (第二道物理防線 uq_active_seat_order)
    zone_codes = {r[1] for r in held_rows}
    zone_rows = await db.execute(
        select(Zone.zone_code, Zone.price).where(
            Zone.event_id == event_id,
            Zone.zone_code.in_(zone_codes),
        )
    )
    zone_prices: Dict[str, Decimal] = {
        r[0]: Decimal(r[1]) for r in zone_rows.all()
    }

    subtotal = sum((zone_prices[r[1]] for r in held_rows), Decimal("0.00"))
    service_fee = calculate_service_fee(subtotal)
    total_amount = subtotal + service_fee
    order_expires_at = held_rows[0][4]

    order = Order(
        user_id=current_user.id,
        event_id=event_id,
        total_ticket_amount=subtotal,
        service_fee=service_fee,
        total_amount=total_amount,
        status=OrderStatus.PENDING.value,
        expires_at=order_expires_at,
    )
    db.add(order)
    await db.flush()

    item_seat_pairs: List[Tuple[OrderItem, str, str, str]] = []
    for sid, z_code, r_label, s_num, _ in held_rows:
        item = OrderItem(
            order_id=order.id,
            seat_id=sid,
            price=zone_prices[z_code],
            status=OrderItemStatus.HELD.value,
        )
        db.add(item)
        item_seat_pairs.append((item, z_code, r_label, s_num))

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        await record_ticket_metric("seat_conflict", event_id=event_id)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="One or more requested seats are already held or sold",
        )

    await db.refresh(order)
    for item, _, _, _ in item_seat_pairs:
        await db.refresh(item)

    await record_ticket_metric("reservation_success", event_id=event_id)
    return _build_reservation_response(order, item_seat_pairs)


async def allocate_zone_seats(
    db: AsyncSession,
    event_id: int,
    current_user: User,
    zone_code: str,
    quantity: int,
) -> OrderReservationResponse:
    """
    區域自動配位購買（pg_advisory_xact_lock + SELECT ... FOR UPDATE SKIP LOCKED）。
    僅在區域可鎖定數量滿足 quantity 時一次鎖定並建立訂單，否則完整回滾不殘留部分鎖位。
    """
    await _assert_event_purchasable(db, event_id)

    zone = (
        await db.execute(
            select(Zone).where(
                Zone.event_id == event_id,
                Zone.zone_code == zone_code,
            )
        )
    ).scalar_one_or_none()
    if zone is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Zone '{zone_code}' not found in event",
        )

    # 1. 使用者-活動層級交易鎖，先序列化限購檢查
    await _acquire_user_event_advisory_lock(db, current_user.id, event_id)
    await _assert_within_per_user_limit(db, current_user.id, event_id, quantity)

    # 2. 自動配位：FOR UPDATE SKIP LOCKED 跳過已被其他交易鎖定中的座位列
    alloc_stmt = (
        select(Seat)
        .where(
            Seat.event_id == event_id,
            Seat.zone_code == zone_code,
            Seat.status == SeatStatus.AVAILABLE.value,
        )
        .order_by(Seat.row_label.asc(), Seat.seat_number.asc(), Seat.id.asc())
        .limit(quantity)
        .with_for_update(skip_locked=True)
    )
    allocated_seats = list((await db.execute(alloc_stmt)).scalars().all())
    if len(allocated_seats) < quantity:
        await db.rollback()
        await record_ticket_metric("zone_sold_out", event_id=event_id)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Insufficient available seats in zone '{zone_code}'",
        )

    # 3. 使用資料庫伺服器時鐘計算鎖位逾時時間並更新已鎖定之座位
    order_expires_at = (
        await db.execute(
            text("SELECT NOW() + make_interval(mins => :hold_mins)"),
            {"hold_mins": SEAT_HOLD_MINUTES},
        )
    ).scalar_one()

    for seat in allocated_seats:
        seat.status = SeatStatus.HELD.value
        seat.held_by = current_user.id
        seat.expires_at = order_expires_at
        seat.version += 1
        seat.updated_at = func.now()

    unit_price = Decimal(zone.price)
    subtotal = unit_price * quantity
    service_fee = calculate_service_fee(subtotal)
    total_amount = subtotal + service_fee

    order = Order(
        user_id=current_user.id,
        event_id=event_id,
        total_ticket_amount=subtotal,
        service_fee=service_fee,
        total_amount=total_amount,
        status=OrderStatus.PENDING.value,
        expires_at=order_expires_at,
    )
    db.add(order)
    await db.flush()

    item_seat_pairs: List[Tuple[OrderItem, str, str, str]] = []
    for seat in allocated_seats:
        item = OrderItem(
            order_id=order.id,
            seat_id=seat.id,
            price=unit_price,
            status=OrderItemStatus.HELD.value,
        )
        db.add(item)
        item_seat_pairs.append((item, seat.zone_code, seat.row_label, seat.seat_number))

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        await record_ticket_metric("seat_conflict", event_id=event_id)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="One or more allocated seats are already held or sold",
        )

    await db.refresh(order)
    for item, _, _, _ in item_seat_pairs:
        await db.refresh(item)

    await record_ticket_metric("reservation_success", event_id=event_id)
    return _build_reservation_response(order, item_seat_pairs)
