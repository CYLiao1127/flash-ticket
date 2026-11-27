"""核心搶票流程測試：指定選位、自動配位、單人單場限購 4 張 (pg_advisory_xact_lock)、5% 服務費四捨五入與業務指標"""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import pytest
from fastapi import HTTPException
from sqlalchemy import func, select

from app.auth.seed import SEED_ACCOUNTS, seed_users
from app.core.redis import get_redis
from app.models import (
    Event,
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
from app.tickets.metrics import (
    REDIS_GLOBAL_METRICS_KEY,
    reset_local_ticket_metrics,
)
from app.tickets.service import (
    allocate_zone_seats,
    calculate_service_fee,
    reserve_specific_seats,
)
from tests.conftest import TestSessionLocal


def _login(client, email: str, password: str) -> str:
    res = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert res.status_code == 200, res.text
    return res.json()["access_token"]


def _login_seed(client, role: str) -> str:
    acc = next(a for a in SEED_ACCOUNTS if a["role"].value == role)
    return _login(client, acc["email"], acc["password"])


async def _reset_metrics_everywhere(event_ids=()) -> None:
    reset_local_ticket_metrics()
    redis = get_redis()
    keys = [REDIS_GLOBAL_METRICS_KEY] + [f"metrics:tickets:event:{eid}" for eid in event_ids]
    await redis.delete(*keys)


async def _create_published_event_with_seats(
    db_session,
    creator_id: int,
    title: str = "Flash Ticket Live",
    zones_spec=None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> tuple[Event, list[Seat]]:
    """建立已發布活動與指定分區座位供搶票測試使用（預設位於開賣期間內）"""
    if zones_spec is None:
        zones_spec = [
            ("VIP", "VIP 特區", Decimal("5800.00"), 5),
            ("A", "看台 A 區", Decimal("1050.00"), 5),
            ("B", "看台 B 區", Decimal("1000.00"), 5),
        ]
    now = datetime.now(timezone.utc)
    event = Event(
        creator_id=creator_id,
        title=title,
        description="Core reservation test event",
        category="concert",
        venue="Taipei Dome",
        start_time=start_time or (now - timedelta(hours=1)),
        end_time=end_time or (now + timedelta(days=1)),
        status=EventStatus.PUBLISHED.value,
    )
    db_session.add(event)
    await db_session.flush()

    seats: list[Seat] = []
    for z_code, z_name, z_price, seat_count in zones_spec:
        db_session.add(
            Zone(event_id=event.id, zone_code=z_code, name=z_name, price=z_price)
        )
        await db_session.flush()
        for idx in range(1, seat_count + 1):
            seat = Seat(
                event_id=event.id,
                zone_code=z_code,
                row_label="1",
                seat_number=str(idx),
                status=SeatStatus.AVAILABLE.value,
            )
            db_session.add(seat)
            seats.append(seat)

    await db_session.commit()
    for seat in seats:
        await db_session.refresh(seat)
    return event, seats


def test_service_fee_round_half_up_boundaries():
    """驗證 5% 服務費採嚴格四捨五入 (ROUND_HALF_UP)，避免 Python 銀行家捨入誤差"""
    # 2.5 -> 3 (Python round(2.5) == 2)
    assert calculate_service_fee(Decimal("50.00")) == Decimal("3")
    # 7.5 -> 8
    assert calculate_service_fee(Decimal("150.00")) == Decimal("8")
    # 12.5 -> 13 (Python round(12.5) == 12)
    assert calculate_service_fee(Decimal("250.00")) == Decimal("13")
    # 52.5 -> 53 (Python round(52.5) == 52)
    assert calculate_service_fee(Decimal("1050.00")) == Decimal("53")
    # 342.5 -> 343
    assert calculate_service_fee(Decimal("6850.00")) == Decimal("343")
    # 非 .5 邊界
    assert calculate_service_fee(Decimal("1000.00")) == Decimal("50")
    assert calculate_service_fee(Decimal("329.00")) == Decimal("16")  # 16.45 -> 16
    assert calculate_service_fee(Decimal("333.00")) == Decimal("17")  # 16.65 -> 17


@pytest.mark.asyncio
async def test_reserve_specific_seats_and_conflict_metrics(client, db_session):
    """驗證指定選位條件原子更新、草稿活動保護、原子回滾與鎖位衝突統計指標"""
    seed_users()
    admin_token = _login_seed(client, "admin")
    user_token = _login_seed(client, "user")
    viewer_token = _login_seed(client, "viewer")

    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    # 1. 草稿活動禁止購票 (409)
    now = datetime.now(timezone.utc)
    draft_event = Event(
        creator_id=admin_user.id,
        title="Draft Concert",
        category="concert",
        venue="Arena",
        start_time=now + timedelta(days=3),
        end_time=now + timedelta(days=3, hours=2),
        status=EventStatus.DRAFT.value,
    )
    db_session.add(draft_event)
    await db_session.commit()

    draft_res = client.post(
        f"/api/v1/events/{draft_event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [1]},
    )
    assert draft_res.status_code == 409

    # 2. 已發布活動：指定選位 VIP (5800) + A (1050) -> 小計 6850、服務費 343、總計 7193
    event, seats = await _create_published_event_with_seats(db_session, admin_user.id)
    await _reset_metrics_everywhere([event.id])

    vip_seat_1 = next(s for s in seats if s.zone_code == "VIP" and s.seat_number == "1")
    a_seat_1 = next(s for s in seats if s.zone_code == "A" and s.seat_number == "1")
    a_seat_2 = next(s for s in seats if s.zone_code == "A" and s.seat_number == "2")

    # Viewer 不可購票 (403)
    viewer_res = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {viewer_token}"},
        json={"seat_ids": [vip_seat_1.id]},
    )
    assert viewer_res.status_code == 403

    res = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [a_seat_1.id, vip_seat_1.id]},
    )
    assert res.status_code == 201, res.text
    order_data = res.json()
    assert order_data["status"] == "pending"
    assert Decimal(order_data["total_ticket_amount"]) == Decimal("6850.00")
    assert Decimal(order_data["service_fee"]) == Decimal("343.00")
    assert Decimal(order_data["total_amount"]) == Decimal("7193.00")
    assert len(order_data["items"]) == 2
    assert all(item["status"] == "held" for item in order_data["items"])

    created_dt = datetime.fromisoformat(order_data["created_at"].replace("Z", "+00:00"))
    expires_dt = datetime.fromisoformat(order_data["expires_at"].replace("Z", "+00:00"))
    delta_secs = (expires_dt - created_dt).total_seconds()
    assert 590 <= delta_secs <= 610

    # 3. 另一位使用者同時選到已被鎖的 vip_seat_1 與空位 a_seat_2 -> 整筆 409 回滾，a_seat_2 必須維持 available
    buyer2_email = "buyer2_conflict@flashticket.com"
    assert (
        client.post(
            "/api/v1/auth/register",
            json={"email": buyer2_email, "password": "Password123!"},
        ).status_code
        == 201
    )
    buyer2_token = _login(client, buyer2_email, "Password123!")

    conflict_res = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {buyer2_token}"},
        json={"seat_ids": [a_seat_2.id, vip_seat_1.id]},
    )
    assert conflict_res.status_code == 409

    await db_session.refresh(a_seat_2)
    assert a_seat_2.status == SeatStatus.AVAILABLE.value
    assert a_seat_2.held_by is None

    # 4. 不存在的座位 -> 404
    missing_res = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {buyer2_token}"},
        json={"seat_ids": [9999999]},
    )
    assert missing_res.status_code == 404

    # 5. 驗證業務統計指標 (Admin / Viewer 可讀，User -> 403)
    assert (
        client.get(
            f"/api/v1/admin/tickets/metrics?event_id={event.id}",
            headers={"Authorization": f"Bearer {user_token}"},
        ).status_code
        == 403
    )
    metrics_res = client.get(
        f"/api/v1/admin/tickets/metrics?event_id={event.id}",
        headers={"Authorization": f"Bearer {viewer_token}"},
    )
    assert metrics_res.status_code == 200
    m = metrics_res.json()
    assert m["reservation_success"] == 1
    assert m["seat_conflict"] == 1
    assert m["zone_sold_out"] == 0
    assert m["limit_exceeded"] == 0


@pytest.mark.asyncio
async def test_auto_allocate_zone_seats_and_sold_out_metrics(client, db_session):
    """驗證區域自動配位 (FOR UPDATE SKIP LOCKED)、庫存不足完整回滾與 zone_sold_out 指標"""
    seed_users()
    admin_token = _login_seed(client, "admin")
    user_token = _login_seed(client, "user")

    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    event, _ = await _create_published_event_with_seats(
        db_session,
        admin_user.id,
        zones_spec=[("B", "看台 B 區", Decimal("1000.00"), 5)],
    )
    await _reset_metrics_everywhere([event.id])

    # 1. 不存在的區域 -> 404
    not_found = client.post(
        f"/api/v1/events/{event.id}/tickets/allocate",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"zone_code": "NO_SUCH_ZONE", "quantity": 1},
    )
    assert not_found.status_code == 404

    # 2. User 1 配位 3 張 -> 201 (剩下 2 張)
    alloc1 = client.post(
        f"/api/v1/events/{event.id}/tickets/allocate",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"zone_code": "B", "quantity": 3},
    )
    assert alloc1.status_code == 201, alloc1.text
    assert len(alloc1.json()["items"]) == 3
    assert Decimal(alloc1.json()["total_ticket_amount"]) == Decimal("3000.00")
    assert Decimal(alloc1.json()["service_fee"]) == Decimal("150.00")

    # 3. User 2 請求配位 3 張（僅剩 2 張）-> 409 且不得鎖住任何一張
    alloc_fail = client.post(
        f"/api/v1/events/{event.id}/tickets/allocate",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"zone_code": "B", "quantity": 3},
    )
    assert alloc_fail.status_code == 409

    avail_left = (
        await db_session.execute(
            select(func.count(Seat.id)).where(
                Seat.event_id == event.id,
                Seat.zone_code == "B",
                Seat.status == SeatStatus.AVAILABLE.value,
            )
        )
    ).scalar_one()
    assert avail_left == 2

    # 4. User 2 請求配位 2 張 -> 201 成功拿走剩餘 2 張
    alloc2 = client.post(
        f"/api/v1/events/{event.id}/tickets/allocate",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"zone_code": "B", "quantity": 2},
    )
    assert alloc2.status_code == 201, alloc2.text
    assert len(alloc2.json()["items"]) == 2

    metrics = client.get(
        f"/api/v1/admin/tickets/metrics?event_id={event.id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    ).json()
    assert metrics["reservation_success"] == 2
    assert metrics["zone_sold_out"] == 1


@pytest.mark.asyncio
async def test_per_user_limit_and_concurrent_advisory_lock_serialization(client, db_session):
    """
    ★ 關鍵 Q3.2 驗證：
    1. 同使用者單場限購 4 張（可分次累計），不同活動各自獨立計算上限。
    2. 同一使用者以 5 個併發請求同時各配位 2 張，pg_advisory_xact_lock 必須嚴格序列化，
       精準放行 2 次（共 4 張）、拒絕 3 次 (409 limit_exceeded)，絕不超買。
    """
    seed_users()
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()
    buyer = (
        await db_session.execute(select(User).where(User.email == "user@flashticket.com"))
    ).scalar_one()

    event1, _ = await _create_published_event_with_seats(
        db_session,
        admin_user.id,
        title="Concurrent Limit Event 1",
        zones_spec=[("A", "A 區", Decimal("1000.00"), 20)],
    )
    event2, _ = await _create_published_event_with_seats(
        db_session,
        admin_user.id,
        title="Concurrent Limit Event 2",
        zones_spec=[("A", "A 區", Decimal("1000.00"), 10)],
    )
    await _reset_metrics_everywhere([event1.id, event2.id])

    async def _attempt_allocate(eid: int, qty: int) -> str:
        async with TestSessionLocal() as sess:
            try:
                await allocate_zone_seats(sess, eid, buyer, "A", qty)
                return "ok"
            except HTTPException as exc:
                assert exc.status_code == 409
                return "rejected"

    # 同一使用者同時發出 5 個各買 2 張之併發請求（總需求 10 張，上限 4 張）
    results = await asyncio.gather(*[_attempt_allocate(event1.id, 2) for _ in range(5)])
    assert results.count("ok") == 2
    assert results.count("rejected") == 3

    # 驗證資料庫中該使用者於 event1 精準持有 4 張票券與 4 個座位
    held_items_cnt = (
        await db_session.execute(
            select(func.count(OrderItem.id))
            .select_from(OrderItem)
            .join(Order, Order.id == OrderItem.order_id)
            .where(
                Order.user_id == buyer.id,
                Order.event_id == event1.id,
                OrderItem.status == OrderItemStatus.HELD.value,
            )
        )
    ).scalar_one()
    assert held_items_cnt == 4

    # 同一使用者在另一場活動 event2 仍擁有獨立的 4 張配額
    assert await _attempt_allocate(event2.id, 4) == "ok"
    assert await _attempt_allocate(event2.id, 1) == "rejected"


@pytest.mark.asyncio
async def test_concurrent_multi_user_race_on_same_seat_and_zone_allocation(client, db_session):
    """
    ★ 關鍵 Q1 / SKIP LOCKED 驗證：
    1. 10 名不同使用者同時指定選位搶同 1 張座位 -> 僅 1 人成功、9 人 409 衝突，無超賣。
    2. 8 名不同使用者同時向僅有 10 張座位的區域各配位 2 張 -> 精準 5 人成功（10 張互不重疊）、3 人售罄 409。
    """
    seed_users()
    admin_token = _login_seed(client, "admin")
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    event, seats = await _create_published_event_with_seats(
        db_session,
        admin_user.id,
        title="Multi-User Race Event",
        zones_spec=[
            ("VIP", "VIP 單座", Decimal("5000.00"), 1),
            ("A", "A 區十座", Decimal("2000.00"), 10),
        ],
    )
    await _reset_metrics_everywhere([event.id])

    # 建立 10 位獨立買家
    buyers: list[User] = []
    for i in range(10):
        u = User(
            email=f"racer_{i}@flashticket.com",
            hashed_password="hashed_pw_unused",
            role=UserRole.USER.value,
            is_active=True,
        )
        db_session.add(u)
        buyers.append(u)
    await db_session.commit()
    for u in buyers:
        await db_session.refresh(u)

    target_vip_seat = next(s for s in seats if s.zone_code == "VIP")

    # 1. 10 人同時搶同 1 個指定座位
    async def _race_seat(u: User) -> str:
        async with TestSessionLocal() as sess:
            try:
                await reserve_specific_seats(sess, event.id, u, [target_vip_seat.id])
                return "ok"
            except HTTPException as exc:
                assert exc.status_code == 409
                return "conflict"

    seat_race_results = await asyncio.gather(*[_race_seat(u) for u in buyers])
    assert seat_race_results.count("ok") == 1
    assert seat_race_results.count("conflict") == 9

    vip_order_items = (
        await db_session.execute(
            select(func.count(OrderItem.id)).where(
                OrderItem.seat_id == target_vip_seat.id,
                OrderItem.status.in_(
                    [OrderItemStatus.HELD.value, OrderItemStatus.PAID.value]
                ),
            )
        )
    ).scalar_one()
    assert vip_order_items == 1

    # 2. 8 人同時向 A 區（10 座）各自動配位 2 張
    async def _race_zone(u: User) -> list[int] | None:
        async with TestSessionLocal() as sess:
            try:
                resp = await allocate_zone_seats(sess, event.id, u, "A", 2)
                return [item.seat_id for item in resp.items]
            except HTTPException as exc:
                assert exc.status_code == 409
                return None

    zone_race_results = await asyncio.gather(*[_race_zone(u) for u in buyers[:8]])
    allocated_lists = [r for r in zone_race_results if r is not None]
    assert len(allocated_lists) == 5
    assert zone_race_results.count(None) == 3

    all_allocated_ids = [sid for lst in allocated_lists for sid in lst]
    assert len(all_allocated_ids) == 10
    assert len(set(all_allocated_ids)) == 10  # 10 張座位完全不重複

    # 3. 驗證該活動累計之業務統計指標
    metrics = client.get(
        f"/api/v1/admin/tickets/metrics?event_id={event.id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    ).json()
    assert metrics["reservation_success"] == 6  # 1 VIP + 5 Zone A
    assert metrics["seat_conflict"] == 9
    assert metrics["zone_sold_out"] == 3
    assert metrics["limit_exceeded"] == 0


@pytest.mark.asyncio
async def test_sale_window_enforcement_and_metrics_redis_fallback(
    client, db_session, monkeypatch
):
    """
    驗證：
    1. 僅於已發布活動的開賣期間 [start_time, end_time) 內允許購票（尚未開賣與已結束皆回傳 409）。
    2. Redis 故障時，帶 event_id 查詢統計指標僅降級回傳該活動本地計數，不混入全站或其他活動數字。
    """
    from app.tickets import metrics as ticket_metrics

    seed_users()
    user_token = _login_seed(client, "user")
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()
    now = datetime.now(timezone.utc)

    # 1a. 尚未開賣的已發布活動 -> 指定選位與自動配位皆回傳 409
    future_event, future_seats = await _create_published_event_with_seats(
        db_session,
        admin_user.id,
        title="Not Yet On Sale",
        zones_spec=[("A", "A 區", Decimal("1000.00"), 2)],
        start_time=now + timedelta(hours=2),
        end_time=now + timedelta(hours=5),
    )
    res_before_start = client.post(
        f"/api/v1/events/{future_event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [future_seats[0].id]},
    )
    assert res_before_start.status_code == 409
    assert "not started" in res_before_start.json()["detail"].lower()

    alloc_before_start = client.post(
        f"/api/v1/events/{future_event.id}/tickets/allocate",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"zone_code": "A", "quantity": 1},
    )
    assert alloc_before_start.status_code == 409
    assert "not started" in alloc_before_start.json()["detail"].lower()

    # 1b. 已結束的已發布活動 -> 指定選位與自動配位皆回傳 409
    ended_event, ended_seats = await _create_published_event_with_seats(
        db_session,
        admin_user.id,
        title="Ended Event",
        zones_spec=[("A", "A 區", Decimal("1000.00"), 2)],
        start_time=now - timedelta(hours=5),
        end_time=now - timedelta(minutes=1),
    )
    res_after_end = client.post(
        f"/api/v1/events/{ended_event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [ended_seats[0].id]},
    )
    assert res_after_end.status_code == 409
    assert "ended" in res_after_end.json()["detail"].lower()

    alloc_after_end = client.post(
        f"/api/v1/events/{ended_event.id}/tickets/allocate",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"zone_code": "A", "quantity": 1},
    )
    assert alloc_after_end.status_code == 409
    assert "ended" in alloc_after_end.json()["detail"].lower()

    # 2. Redis 故障時，各活動之本地統計降級彼此完全隔離
    await _reset_metrics_everywhere([101, 202, 303])
    await ticket_metrics.record_ticket_metric("reservation_success", event_id=101, delta=3)
    await ticket_metrics.record_ticket_metric("seat_conflict", event_id=101, delta=1)
    await ticket_metrics.record_ticket_metric("zone_sold_out", event_id=202, delta=4)

    class _BrokenRedis:
        async def hgetall(self, _key):
            raise ConnectionError("Simulated Redis outage")

    monkeypatch.setattr(ticket_metrics, "get_redis", lambda: _BrokenRedis())

    ev101 = await ticket_metrics.get_ticket_metrics_snapshot(event_id=101)
    ev202 = await ticket_metrics.get_ticket_metrics_snapshot(event_id=202)
    ev303 = await ticket_metrics.get_ticket_metrics_snapshot(event_id=303)
    global_snap = await ticket_metrics.get_ticket_metrics_snapshot()

    assert ev101 == {
        "reservation_success": 3,
        "seat_conflict": 1,
        "zone_sold_out": 0,
        "limit_exceeded": 0,
    }
    assert ev202 == {
        "reservation_success": 0,
        "seat_conflict": 0,
        "zone_sold_out": 4,
        "limit_exceeded": 0,
    }
    assert ev303 == {
        "reservation_success": 0,
        "seat_conflict": 0,
        "zone_sold_out": 0,
        "limit_exceeded": 0,
    }
    assert global_snap == {
        "reservation_success": 3,
        "seat_conflict": 1,
        "zone_sold_out": 4,
        "limit_exceeded": 0,
    }
