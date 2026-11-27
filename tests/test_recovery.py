"""故障恢復與極端邊界收斂測試 (Commit 17 / Q2.2, Q2.3, Q2.4)

驗證項目：
1. 應用時鐘偏移注入測試 (Q2.3)：同時偏移 Python `time.time()` 與 `datetime.now()` (+120s / -120s)，
   證明逾時釋放完全依據 PostgreSQL NOW()，不受任何應用層時鐘 API 漂移影響。
2. 雙 Worker 真實重疊持鎖併發清理冪等測試 (Q2.4)：Worker 1 鎖定前 3 筆過期訂單並於提交前持鎖阻塞，
   期間 Worker 2 透過 FOR UPDATE SKIP LOCKED 併發認領剩餘 3 筆並提交，驗證雙方皆實際處理 3 筆且零重複。
3. kill -9 (SIGKILL) 中斷真實多表釋放流程收斂測試 (Q2.2)：子進程執行真實 `release_expired_orders`，
   在已 flush `orders`、`order_items`、`seats`、`events.last_seq`、`event_outbox` 五表變更、
   即將 commit 前遭 SIGKILL 強殺；驗證 PostgreSQL 完整回滾所有表與序號，新 Worker 接手後狀態無縫收斂。
"""

import asyncio
import datetime as dt_module
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import pytest
from sqlalchemy import func, select, text

from app.config import settings
from app.models import (
    Event,
    EventOutbox,
    EventStatus,
    Order,
    OrderItem,
    OrderItemStatus,
    OrderStatus,
    Seat,
    SeatStatus,
    User,
    UserRole,
    Zone,
)
from app.payments import service as payment_service
from app.payments.service import release_expired_orders
from app.tickets.service import reserve_specific_seats
from tests.conftest import TestSessionLocal


async def _setup_event_and_users(db_session, *, seat_count: int, user_count: int, prefix: str):
    creator = User(
        email=f"{prefix}_creator@flashticket.com",
        hashed_password="unused_hash_for_recovery_test",
        role=UserRole.ADMIN.value,
        is_active=True,
    )
    db_session.add(creator)
    await db_session.flush()

    now = datetime.now(timezone.utc)
    event = Event(
        creator_id=creator.id,
        title=f"Recovery Test {prefix}",
        description="Fault tolerance & recovery test event",
        category="concert",
        venue="Taipei Dome",
        start_time=now - timedelta(hours=1),
        end_time=now + timedelta(days=1),
        status=EventStatus.PUBLISHED.value,
    )
    db_session.add(event)
    await db_session.flush()

    db_session.add(
        Zone(event_id=event.id, zone_code="A", name="A 區", price=Decimal("1000.00"))
    )
    await db_session.flush()

    seats: list[Seat] = []
    for idx in range(1, seat_count + 1):
        s = Seat(
            event_id=event.id,
            zone_code="A",
            row_label="1",
            seat_number=str(idx),
            status=SeatStatus.AVAILABLE.value,
        )
        db_session.add(s)
        seats.append(s)

    users: list[User] = []
    for idx in range(user_count):
        u = User(
            email=f"{prefix}_buyer_{idx}@flashticket.com",
            hashed_password="unused_hash_for_recovery_test",
            role=UserRole.USER.value,
            is_active=True,
        )
        db_session.add(u)
        users.append(u)

    await db_session.commit()
    await db_session.refresh(event)
    for s in seats:
        await db_session.refresh(s)
    for u in users:
        await db_session.refresh(u)
    return event, seats, users


@pytest.mark.asyncio
async def test_clock_skew_injection_relies_solely_on_db_clock_q23(
    db_session, monkeypatch
):
    """
    ★ 關鍵 Q2.3 驗證：同時注入應用層 `time.time()` 與 `datetime.now()` / `datetime.utcnow()`
    偏移 (+120s 與 -120s)，證明逾時釋放完全依據 PostgreSQL 伺服器時鐘 NOW()。
    """
    event, seats, users = await _setup_event_and_users(
        db_session, seat_count=3, user_count=3, prefix="clock_skew"
    )

    async with TestSessionLocal() as s1:
        o_future = await reserve_specific_seats(s1, event.id, users[0], [seats[0].id])
    async with TestSessionLocal() as s2:
        o_past_1 = await reserve_specific_seats(s2, event.id, users[1], [seats[1].id])
    async with TestSessionLocal() as s3:
        o_past_2 = await reserve_specific_seats(s3, event.id, users[2], [seats[2].id])

    await db_session.execute(
        text("UPDATE orders SET expires_at = NOW() + INTERVAL '30 seconds' WHERE id = :oid"),
        {"oid": o_future.id},
    )
    await db_session.execute(
        text("UPDATE orders SET expires_at = NOW() - INTERVAL '30 seconds' WHERE id = :oid"),
        {"oid": o_past_1.id},
    )
    await db_session.execute(
        text("UPDATE orders SET expires_at = NOW() + INTERVAL '30 seconds' WHERE id = :oid"),
        {"oid": o_past_2.id},
    )
    await db_session.commit()

    real_time = time.time
    real_datetime = dt_module.datetime
    skew_seconds = {"value": 0.0}

    class _SkewedDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return real_datetime.now(tz) + timedelta(seconds=skew_seconds["value"])

        @classmethod
        def utcnow(cls):
            return real_datetime.utcnow() + timedelta(seconds=skew_seconds["value"])

    monkeypatch.setattr(time, "time", lambda: real_time() + skew_seconds["value"])
    monkeypatch.setattr(dt_module, "datetime", _SkewedDateTime)
    if hasattr(payment_service, "datetime"):
        monkeypatch.setattr(payment_service, "datetime", _SkewedDateTime)

    # 情境 1：將應用時鐘（time.time 與 datetime.now）往前撥快 +120 秒
    skew_seconds["value"] = 120.0
    db_now = (await db_session.execute(select(func.now()))).scalar_one()
    assert (dt_module.datetime.now(timezone.utc) - db_now).total_seconds() > 90

    async with TestSessionLocal() as w1:
        released_1 = await release_expired_orders(w1)
    assert released_1 == 1

    o_future_row = (
        await db_session.execute(
            select(Order)
            .where(Order.id == o_future.id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    o_past_1_row = (
        await db_session.execute(
            select(Order)
            .where(Order.id == o_past_1.id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    assert o_future_row.status == OrderStatus.PENDING.value
    assert o_past_1_row.status == OrderStatus.CANCELLED.value

    # 情境 2：將 o_past_2 在 DB 設為已過期 -30s，但把應用時鐘往回撥慢 -120 秒
    await db_session.execute(
        text("UPDATE orders SET expires_at = NOW() - INTERVAL '30 seconds' WHERE id = :oid"),
        {"oid": o_past_2.id},
    )
    await db_session.commit()

    skew_seconds["value"] = -120.0
    db_now_2 = (await db_session.execute(select(func.now()))).scalar_one()
    assert (dt_module.datetime.now(timezone.utc) - db_now_2).total_seconds() < -90

    async with TestSessionLocal() as w2:
        released_2 = await release_expired_orders(w2)
    assert released_2 == 1

    o_past_2_row = (
        await db_session.execute(
            select(Order)
            .where(Order.id == o_past_2.id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    assert o_past_2_row.status == OrderStatus.CANCELLED.value


@pytest.mark.asyncio
async def test_dual_workers_concurrent_expiry_cleanup_idempotency_q24(db_session):
    """
    ★ 關鍵 Q2.4 驗證：雙 Worker 真實重疊持鎖併發清理冪等性：
    Worker 1 鎖定前 3 筆過期訂單並於 on_before_commit 持鎖等待；
    期間 Worker 2 同時進場透過 FOR UPDATE SKIP LOCKED 略過已被 Worker 1 鎖定之 3 筆、
    直接認領並提交剩餘 3 筆，證明雙方真實併發競爭、各釋放 3 筆且零重複 Outbox。
    """
    event, seats, users = await _setup_event_and_users(
        db_session, seat_count=6, user_count=6, prefix="dual_worker"
    )

    order_ids: list[int] = []
    for idx in range(6):
        async with TestSessionLocal() as sess:
            resp = await reserve_specific_seats(sess, event.id, users[idx], [seats[idx].id])
            order_ids.append(resp.id)

    await db_session.execute(
        text(
            "UPDATE orders SET expires_at = NOW() - INTERVAL '2 minutes' "
            "WHERE id = ANY(:oids)"
        ),
        {"oids": order_ids},
    )
    await db_session.commit()

    worker1_holding_locks = asyncio.Event()
    worker2_holding_locks = asyncio.Event()
    worker1_claimed: list[int] = []
    worker2_claimed: list[int] = []

    async def _worker1_locked(claimed_ids: list[int]) -> None:
        worker1_claimed.extend(claimed_ids)
        worker1_holding_locks.set()
        # 持續持有前 3 筆訂單的 FOR UPDATE 列鎖，直到 Worker 2 也鎖定其餘 3 筆為止
        await asyncio.wait_for(worker2_holding_locks.wait(), timeout=5.0)

    async def _worker2_locked(claimed_ids: list[int]) -> None:
        worker2_claimed.extend(claimed_ids)
        worker2_holding_locks.set()

    async def _run_worker1() -> int:
        async with TestSessionLocal() as sess:
            return await release_expired_orders(
                sess, limit=3, on_orders_locked=_worker1_locked
            )

    async def _run_worker2() -> int:
        # 確保在 Worker 1 已持有前 3 筆訂單 FOR UPDATE 列鎖期間進場競爭
        await asyncio.wait_for(worker1_holding_locks.wait(), timeout=5.0)
        async with TestSessionLocal() as sess:
            return await release_expired_orders(
                sess, limit=3, on_orders_locked=_worker2_locked
            )

    t1 = asyncio.create_task(_run_worker1())
    t2 = asyncio.create_task(_run_worker2())
    try:
        r1, r2 = await asyncio.wait_for(asyncio.gather(t1, t2), timeout=10.0)
    finally:
        worker1_holding_locks.set()
        worker2_holding_locks.set()

    assert r1 == 3
    assert r2 == 3
    assert len(worker1_claimed) == 3
    assert len(worker2_claimed) == 3
    assert set(worker1_claimed).isdisjoint(set(worker2_claimed))
    assert sorted(worker1_claimed + worker2_claimed) == sorted(order_ids)

    # 驗證全部 6 筆訂單為 cancelled、全部 6 張座位回 available、Outbox 精準 6 筆且無跳號
    cancelled_cnt = (
        await db_session.execute(
            select(func.count(Order.id)).where(
                Order.id.in_(order_ids),
                Order.status == OrderStatus.CANCELLED.value,
            )
        )
    ).scalar_one()
    avail_cnt = (
        await db_session.execute(
            select(func.count(Seat.id)).where(
                Seat.event_id == event.id,
                Seat.status == SeatStatus.AVAILABLE.value,
            )
        )
    ).scalar_one()
    assert cancelled_cnt == 6
    assert avail_cnt == 6

    release_events = list(
        (
            await db_session.execute(
                select(EventOutbox)
                .where(
                    EventOutbox.event_id == event.id,
                    EventOutbox.event_type == "seat_released",
                )
                .order_by(EventOutbox.sequence_id.asc())
            )
        )
        .scalars()
        .all()
    )
    assert len(release_events) == 6
    assert [e.sequence_id for e in release_events] == [7, 8, 9, 10, 11, 12]
    assert sorted(e.payload["order_id"] for e in release_events) == sorted(order_ids)


@pytest.mark.asyncio
async def test_kill_9_mid_transaction_recovery_and_state_convergence_q22(db_session):
    """
    ★ 關鍵 Q2.2 驗證：真實 SIGKILL (kill -9) 中斷實際多表釋放流程之狀態收斂：
    1. 獨立子進程實際執行 `release_expired_orders(sess, on_before_commit=...)`，
       已 flush `orders`、`order_items`、`seats`、`events.last_seq`、`event_outbox` 全部變更，
       於 `commit()` 前遭 SIGKILL 強殺。
    2. 子進程持鎖期間，另一名 Worker 透過 SKIP LOCKED 安全略過不阻塞。
    3. SIGKILL 後 PostgreSQL 自動回滾所有未提交表變更（含 events.last_seq），
       新 Worker 接手後完成原子收斂，且序號嚴格連續無空洞、他人可立即重購。
    """
    event, seats, users = await _setup_event_and_users(
        db_session, seat_count=2, user_count=2, prefix="kill9"
    )

    async with TestSessionLocal() as sess:
        order_resp = await reserve_specific_seats(sess, event.id, users[0], [seats[0].id])
    order_id = order_resp.id
    assert order_resp.sequence_id == 1

    await db_session.execute(
        text("UPDATE orders SET expires_at = NOW() - INTERVAL '2 minutes' WHERE id = :oid"),
        {"oid": order_id},
    )
    await db_session.commit()

    # 子進程實際執行完整的 release_expired_orders 流程，於 flush 多表後、commit 前阻塞待 SIGKILL
    child_script = (
        "import asyncio\n"
        "from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession\n"
        "from sqlalchemy.pool import NullPool\n"
        "from app.config import settings\n"
        "from app.payments.service import release_expired_orders\n"
        "async def run():\n"
        "    eng = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)\n"
        "    SL = async_sessionmaker(bind=eng, class_=AsyncSession, expire_on_commit=False)\n"
        "    async def hang_before_commit(claimed_ids):\n"
        "        print(f'READY_BEFORE_COMMIT:{claimed_ids}', flush=True)\n"
        "        await asyncio.sleep(60)\n"
        "    async with SL() as sess:\n"
        "        await release_expired_orders(sess, on_before_commit=hang_before_commit)\n"
        "asyncio.run(run())\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", child_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        ready_line = await asyncio.to_thread(proc.stdout.readline)
        assert ready_line.strip() == f"READY_BEFORE_COMMIT:[{order_id}]", (
            f"Child did not reach pre-commit hook: stdout={ready_line!r}, "
            f"stderr={proc.stderr.read() if proc.stderr else ''!r}"
        )

        # 子進程已 flush 五張表但未提交、仍持有列鎖期間，其他 Worker 必透過 SKIP LOCKED 安全略過
        async with TestSessionLocal() as skip_sess:
            skipped_res = await release_expired_orders(skip_sess)
        assert skipped_res == 0

        # 以真實 SIGKILL (kill -9) 強殺子進程
        os.kill(proc.pid, signal.SIGKILL)
        exit_code = await asyncio.to_thread(proc.wait)
        assert exit_code == -signal.SIGKILL
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    # 等待 PostgreSQL 偵測死連線並回滾五張表變更後，新 Worker 接手完成收斂
    recovered_total = 0
    for _ in range(30):
        async with TestSessionLocal() as recovery_sess:
            recovered_total = await release_expired_orders(recovery_sess)
        if recovered_total == 1:
            break
        await asyncio.sleep(0.05)

    assert recovered_total == 1

    # 驗證最終所有資料表狀態一致收斂，且 events.last_seq 嚴格連續為 2（無空洞）
    order_row = (
        await db_session.execute(
            select(Order)
            .where(Order.id == order_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    item_row = (
        await db_session.execute(
            select(OrderItem)
            .where(OrderItem.order_id == order_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    seat_row = (
        await db_session.execute(
            select(Seat)
            .where(Seat.id == seats[0].id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    event_row = (
        await db_session.execute(
            select(Event)
            .where(Event.id == event.id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()

    assert order_row.status == OrderStatus.CANCELLED.value
    assert item_row.status == OrderItemStatus.CANCELLED.value
    assert seat_row.status == SeatStatus.AVAILABLE.value
    assert seat_row.held_by is None
    assert event_row.last_seq == 2

    outbox_rows = list(
        (
            await db_session.execute(
                select(EventOutbox)
                .where(EventOutbox.event_id == event.id)
                .order_by(EventOutbox.sequence_id.asc())
            )
        )
        .scalars()
        .all()
    )
    assert [(r.sequence_id, r.event_type) for r in outbox_rows] == [
        (1, "seat_held"),
        (2, "seat_released"),
    ]

    # 收斂後的座位可立即由另一位使用者重新成功購買
    async with TestSessionLocal() as rebook_sess:
        new_order = await reserve_specific_seats(
            rebook_sess, event.id, users[1], [seats[0].id]
        )
    assert new_order.status == OrderStatus.PENDING.value
    assert new_order.sequence_id == 3
    assert new_order.items[0].seat_id == seats[0].id
