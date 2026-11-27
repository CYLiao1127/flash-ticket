"""高併發搶位與雙 Idempotency-Key 競態整合測試 (Commit 12 / Q1, Q3.2, Q3.3, Q4.1)

驗證項目：
1. 高併發搶同一熱門座位（30 人搶 1 座）與區域自動配位（25 人搶 12 座）之零超賣與資料一致性。
2. 多座位反序指定選位（[A, B] vs [B, A]）固定鎖順序防死結驗證。
3. 同一使用者以兩個不同 Idempotency-Key 同時配位（各 3 張 > 上限 4 張）之雙鍵併發競態（Q3.2, Q3.3）：
   - pg_advisory_xact_lock 保證僅 1 筆成功、總張數不超過 4 張；
   - 成功 Key 重送命中冪等快取，失敗 Key 正確釋放 processing 鎖並可供後續合法配額使用。
4. 同一使用者以相同 Idempotency-Key 併發風暴（10 併發）僅建立 1 筆訂單與 1 筆 Outbox。
"""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import pytest
from fastapi import HTTPException
from sqlalchemy import func, select

from app.core.redis import get_redis
from app.models import (
    Event,
    EventOutbox,
    EventStatus,
    Order,
    OrderItem,
    OrderItemStatus,
    Seat,
    SeatStatus,
    User,
    UserRole,
    Zone,
)
from app.tickets.service import allocate_zone_seats, reserve_specific_seats
from tests.conftest import TestSessionLocal


async def _setup_event_and_buyers(
    db_session,
    *,
    title: str,
    zones_spec: list[tuple[str, str, Decimal, int]],
    buyer_count: int,
    email_prefix: str,
) -> tuple[Event, list[Seat], list[User]]:
    """建立開賣中活動、分區座位與多位獨立買家帳號"""
    creator = User(
        email=f"{email_prefix}_creator@flashticket.com",
        hashed_password="unused_hash_for_direct_service_test",
        role=UserRole.ADMIN.value,
        is_active=True,
    )
    db_session.add(creator)
    await db_session.flush()

    now = datetime.now(timezone.utc)
    event = Event(
        creator_id=creator.id,
        title=title,
        description="Concurrency test event",
        category="concert",
        venue="Taipei Dome",
        start_time=now - timedelta(hours=1),
        end_time=now + timedelta(days=1),
        status=EventStatus.PUBLISHED.value,
    )
    db_session.add(event)
    await db_session.flush()

    seats: list[Seat] = []
    for z_code, z_name, z_price, seat_cnt in zones_spec:
        db_session.add(
            Zone(event_id=event.id, zone_code=z_code, name=z_name, price=z_price)
        )
        await db_session.flush()
        for idx in range(1, seat_cnt + 1):
            seat = Seat(
                event_id=event.id,
                zone_code=z_code,
                row_label="1",
                seat_number=str(idx),
                status=SeatStatus.AVAILABLE.value,
            )
            db_session.add(seat)
            seats.append(seat)

    buyers: list[User] = []
    for i in range(buyer_count):
        u = User(
            email=f"{email_prefix}_buyer_{i}@flashticket.com",
            hashed_password="unused_hash_for_direct_service_test",
            role=UserRole.USER.value,
            is_active=True,
        )
        db_session.add(u)
        buyers.append(u)

    await db_session.commit()
    await db_session.refresh(event)
    for s in seats:
        await db_session.refresh(s)
    for u in buyers:
        await db_session.refresh(u)

    return event, seats, buyers


@pytest.mark.asyncio
async def test_high_concurrency_anti_overselling_reserve_and_allocate_consistency(
    db_session,
):
    """
    高併發搶位整合測試：
    1. 30 位買家同步起跑搶 1 個熱門指定座位 -> 僅 1 人成功、29 人 409。
    2. 25 位買家同步起跑向僅有 12 座之區域各自動配位 2 張 -> 精準 6 人成功（12 座零重複）、19 人 409。
    3. 驗證 seats / order_items / events.last_seq / event_outbox 嚴格一致且序號無空洞。
    """
    event, seats, buyers = await _setup_event_and_buyers(
        db_session,
        title="High Concurrency Anti-Oversell",
        zones_spec=[
            ("VIP", "VIP 單座", Decimal("5800.00"), 1),
            ("A", "A 區十二座", Decimal("2000.00"), 12),
        ],
        buyer_count=30,
        email_prefix="hc_anti_oversell",
    )

    hot_seat = next(s for s in seats if s.zone_code == "VIP")
    start_gun_1 = asyncio.Event()

    async def _race_hot_seat(user: User):
        await start_gun_1.wait()
        async with TestSessionLocal() as sess:
            try:
                return await reserve_specific_seats(sess, event.id, user, [hot_seat.id])
            except HTTPException as exc:
                assert exc.status_code == 409
                return None

    tasks_1 = [asyncio.create_task(_race_hot_seat(u)) for u in buyers]
    await asyncio.sleep(0.05)
    start_gun_1.set()
    results_1 = await asyncio.gather(*tasks_1)

    winners_1 = [r for r in results_1 if r is not None]
    assert len(winners_1) == 1
    assert results_1.count(None) == 29

    # 25 人同步起跑搶 12 張 A 區座位（每人 2 張）
    start_gun_2 = asyncio.Event()

    async def _race_zone_a(user: User):
        await start_gun_2.wait()
        async with TestSessionLocal() as sess:
            try:
                return await allocate_zone_seats(sess, event.id, user, "A", 2)
            except HTTPException as exc:
                assert exc.status_code == 409
                return None

    tasks_2 = [asyncio.create_task(_race_zone_a(u)) for u in buyers[:25]]
    await asyncio.sleep(0.05)
    start_gun_2.set()
    results_2 = await asyncio.gather(*tasks_2)

    winners_2 = [r for r in results_2 if r is not None]
    assert len(winners_2) == 6
    assert results_2.count(None) == 19

    # 驗證跨表一致性：held 座位數 == 有效訂單明細數 == 13，且無重複座位
    allocated_seat_ids = [
        item.seat_id for order_resp in winners_1 + winners_2 for item in order_resp.items
    ]
    assert len(allocated_seat_ids) == 13
    assert len(set(allocated_seat_ids)) == 13

    db_held_seats = (
        await db_session.execute(
            select(func.count(Seat.id)).where(
                Seat.event_id == event.id,
                Seat.status == SeatStatus.HELD.value,
            )
        )
    ).scalar_one()
    db_active_items = (
        await db_session.execute(
            select(func.count(OrderItem.id))
            .select_from(OrderItem)
            .join(Order, Order.id == OrderItem.order_id)
            .where(
                Order.event_id == event.id,
                OrderItem.status.in_(
                    [OrderItemStatus.HELD.value, OrderItemStatus.PAID.value]
                ),
            )
        )
    ).scalar_one()
    assert db_held_seats == 13
    assert db_active_items == 13

    # 驗證活動專屬單調序號嚴格連續 1..7（1 筆 VIP + 6 筆 A 區），無跳號無重複
    await db_session.refresh(event)
    assert event.last_seq == 7
    outbox_seqs = list(
        (
            await db_session.execute(
                select(EventOutbox.sequence_id)
                .where(EventOutbox.event_id == event.id)
                .order_by(EventOutbox.sequence_id.asc())
            )
        )
        .scalars()
        .all()
    )
    assert outbox_seqs == [1, 2, 3, 4, 5, 6, 7]


@pytest.mark.asyncio
async def test_reverse_order_multi_seat_reservation_deadlock_free(db_session):
    """
    反序多座位併發選位無死結測試：
    多組買家同時以相反座位 ID 順序 ([A, B] vs [B, A]) 搶重疊座位，
    驗證固定升冪鎖順序杜絕 PostgreSQL DeadlockDetectedError，且每組精準 1 勝 1 負。
    """
    event, seats, buyers = await _setup_event_and_buyers(
        db_session,
        title="Reverse Order Deadlock Prevention",
        zones_spec=[("A", "A 區", Decimal("1500.00"), 10)],
        buyer_count=10,
        email_prefix="deadlock_free",
    )

    start_gun = asyncio.Event()

    async def _attempt(user: User, seat_pair: list[int]) -> str:
        await start_gun.wait()
        async with TestSessionLocal() as sess:
            try:
                await reserve_specific_seats(sess, event.id, user, seat_pair)
                return "ok"
            except HTTPException as exc:
                assert exc.status_code == 409
                return "conflict"

    tasks = []
    for pair_idx in range(5):
        s1 = seats[pair_idx * 2]
        s2 = seats[pair_idx * 2 + 1]
        u_forward = buyers[pair_idx * 2]
        u_reverse = buyers[pair_idx * 2 + 1]
        tasks.append(asyncio.create_task(_attempt(u_forward, [s1.id, s2.id])))
        tasks.append(asyncio.create_task(_attempt(u_reverse, [s2.id, s1.id])))

    await asyncio.sleep(0.05)
    start_gun.set()
    outcomes = await asyncio.gather(*tasks)

    assert outcomes.count("ok") == 5
    assert outcomes.count("conflict") == 5

    # 每組都必須正好 1 勝 1 負（沒有任何一組兩敗或雙贏）
    for pair_idx in range(5):
        pair_outcomes = outcomes[pair_idx * 2 : pair_idx * 2 + 2]
        assert sorted(pair_outcomes) == ["conflict", "ok"]


@pytest.mark.asyncio
async def test_dual_idempotency_key_concurrent_allocation_race_q32_q33(db_session):
    """
    ★ 關鍵 Q3.2 / Q3.3 雙鍵併發競態測試：
    同一使用者以兩個不同 Idempotency-Key (dual-key-A 與 dual-key-B) 同時發起自動配位（各 3 張，合計 6 張 > 上限 4 張）：
    1. pg_advisory_xact_lock 必須嚴格序列化同一使用者的兩筆請求，精準 1 筆成功 (3 張)、1 筆 409 拒單。
    2. 勝出 Key 重送相同請求回傳冪等快取 (is_idempotent_replay=True)，不增加座位或序號。
    3. 敗出 Key 已正確釋放 processing 鎖，後續改買剩餘 1 張配額可成功 (累計 4 張)，再買第 5 張仍被拒絕。
    """
    event, _, buyers = await _setup_event_and_buyers(
        db_session,
        title="Dual Key Race Event",
        zones_spec=[("A", "A 區", Decimal("1200.00"), 10)],
        buyer_count=1,
        email_prefix="dual_key_race",
    )
    buyer = buyers[0]

    key_a = f"dual-key-a-{event.id}"
    key_b = f"dual-key-b-{event.id}"
    key_c = f"dual-key-c-{event.id}"
    redis = get_redis()
    await redis.delete(
        f"idempotency:tickets:{key_a}",
        f"idempotency:tickets:{key_b}",
        f"idempotency:tickets:{key_c}",
    )

    start_gun = asyncio.Event()

    async def _alloc_with_key(idem_key: str, qty: int):
        await start_gun.wait()
        async with TestSessionLocal() as sess:
            try:
                resp = await allocate_zone_seats(
                    sess, event.id, buyer, "A", qty, idempotency_key=idem_key
                )
                return ("ok", idem_key, resp)
            except HTTPException as exc:
                return ("err", idem_key, exc.status_code)

    t_a = asyncio.create_task(_alloc_with_key(key_a, 3))
    t_b = asyncio.create_task(_alloc_with_key(key_b, 3))
    await asyncio.sleep(0.05)
    start_gun.set()
    res_a, res_b = await asyncio.gather(t_a, t_b)

    outcomes = [res_a, res_b]
    winners = [o for o in outcomes if o[0] == "ok"]
    losers = [o for o in outcomes if o[0] == "err"]
    assert len(winners) == 1
    assert len(losers) == 1
    assert losers[0][2] == 409

    winner_key = winners[0][1]
    winner_order = winners[0][2]
    loser_key = losers[0][1]

    # 確認 DB 精準持有 3 張票、events.last_seq == 1
    held_cnt = (
        await db_session.execute(
            select(func.count(OrderItem.id))
            .select_from(OrderItem)
            .join(Order, Order.id == OrderItem.order_id)
            .where(
                Order.user_id == buyer.id,
                Order.event_id == event.id,
                OrderItem.status == OrderItemStatus.HELD.value,
            )
        )
    ).scalar_one()
    assert held_cnt == 3
    await db_session.refresh(event)
    assert event.last_seq == 1

    # 勝出 Key 重送相同參數 -> 命中冪等快取，不重扣庫存也不遞增序號
    async with TestSessionLocal() as replay_sess:
        replayed = await allocate_zone_seats(
            replay_sess, event.id, buyer, "A", 3, idempotency_key=winner_key
        )
    assert replayed.is_idempotent_replay is True
    assert replayed.id == winner_order.id
    await db_session.refresh(event)
    assert event.last_seq == 1

    # 敗出 Key 的 processing 鎖必須已釋放，改買剩餘 1 張配額應成功（累計 4 張）
    async with TestSessionLocal() as retry_sess:
        fourth_order = await allocate_zone_seats(
            retry_sess, event.id, buyer, "A", 1, idempotency_key=loser_key
        )
    assert fourth_order.is_idempotent_replay is False
    assert fourth_order.sequence_id == 2
    assert len(fourth_order.items) == 1

    # 第 5 張（使用新 Key）必須因單人單場限購 4 張被拒 (409)
    with pytest.raises(HTTPException) as exc_info:
        async with TestSessionLocal() as fifth_sess:
            await allocate_zone_seats(
                fifth_sess, event.id, buyer, "A", 1, idempotency_key=key_c
            )
    assert exc_info.value.status_code == 409

    await redis.delete(
        f"idempotency:tickets:{key_a}",
        f"idempotency:tickets:{key_b}",
        f"idempotency:tickets:{key_c}",
    )


@pytest.mark.asyncio
async def test_same_idempotency_key_concurrent_storm_creates_only_one_order(db_session):
    """
    相同 Idempotency-Key 併發風暴測試：
    同一使用者以完全相同的 Idempotency-Key 與參數同時發出 10 個請求，
    必須僅建立 1 筆訂單、鎖定 2 張座位、寫入 1 筆 Outbox，其餘 9 筆回傳 409 處理中或 200 冪等快取。
    """
    event, _, buyers = await _setup_event_and_buyers(
        db_session,
        title="Same Key Storm Event",
        zones_spec=[("A", "A 區", Decimal("1000.00"), 10)],
        buyer_count=1,
        email_prefix="same_key_storm",
    )
    buyer = buyers[0]
    storm_key = f"storm-key-{event.id}"
    redis = get_redis()
    await redis.delete(f"idempotency:tickets:{storm_key}")

    start_gun = asyncio.Event()

    async def _fire():
        await start_gun.wait()
        async with TestSessionLocal() as sess:
            try:
                resp = await allocate_zone_seats(
                    sess, event.id, buyer, "A", 2, idempotency_key=storm_key
                )
                return ("replay" if resp.is_idempotent_replay else "created", resp.id)
            except HTTPException as exc:
                assert exc.status_code == 409
                return ("processing", None)

    tasks = [asyncio.create_task(_fire()) for _ in range(10)]
    await asyncio.sleep(0.05)
    start_gun.set()
    results = await asyncio.gather(*tasks)

    created = [r for r in results if r[0] == "created"]
    assert len(created) == 1
    created_order_id = created[0][1]

    for kind, oid in results:
        if kind == "replay":
            assert oid == created_order_id

    # 驗證資料庫中僅存在 1 筆訂單、2 筆訂單明細、1 筆 Outbox 事件
    order_cnt = (
        await db_session.execute(
            select(func.count(Order.id)).where(Order.event_id == event.id)
        )
    ).scalar_one()
    item_cnt = (
        await db_session.execute(
            select(func.count(OrderItem.id))
            .select_from(OrderItem)
            .join(Order, Order.id == OrderItem.order_id)
            .where(Order.event_id == event.id)
        )
    ).scalar_one()
    await db_session.refresh(event)
    assert order_cnt == 1
    assert item_cnt == 2
    assert event.last_seq == 1

    # 風暴結束後再以同 Key 重送 -> 穩定回傳同一筆訂單之冪等快取
    async with TestSessionLocal() as final_sess:
        final_resp = await allocate_zone_seats(
            final_sess, event.id, buyer, "A", 2, idempotency_key=storm_key
        )
    assert final_resp.is_idempotent_replay is True
    assert final_resp.id == created_order_id

    await redis.delete(f"idempotency:tickets:{storm_key}")
