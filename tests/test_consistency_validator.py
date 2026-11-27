"""壓測後一致性校驗腳本自動化單元與整合測試 (Commit 22)"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select, text

from app.auth.seed import SEED_ACCOUNTS, seed_users
from app.db import SyncSessionLocal
from app.models import Event, EventStatus, Seat, SeatStatus, User, Zone
from scripts.verify_consistency import verify_event_consistency


def _login_seed(client, role: str) -> str:
    acc = next(a for a in SEED_ACCOUNTS if a["role"].value == role)
    res = client.post(
        "/api/v1/auth/login",
        json={"email": acc["email"], "password": acc["password"]},
    )
    assert res.status_code == 200, res.text
    return res.json()["access_token"]


@pytest.mark.asyncio
async def test_consistency_validator_passes_valid_orders_and_catches_mismatch(
    client, db_session
):
    """驗證 verify_event_consistency 對正常搶票/付款交易全數 PASS，並能抓出資料狀態不一致"""
    seed_users()
    user_token = _login_seed(client, "user")
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    now = datetime.now(timezone.utc)
    ev = Event(
        creator_id=admin_user.id,
        title="Consistency Validator Event",
        category="concert",
        venue="Taipei Arena",
        start_time=now - timedelta(hours=1),
        end_time=now + timedelta(days=1),
        status=EventStatus.PUBLISHED.value,
    )
    db_session.add(ev)
    await db_session.flush()
    db_session.add(Zone(event_id=ev.id, zone_code="A", name="A 區", price=Decimal("1000.00")))
    await db_session.flush()
    seats = [
        Seat(
            event_id=ev.id,
            zone_code="A",
            row_label="1",
            seat_number=str(i),
            status=SeatStatus.AVAILABLE.value,
        )
        for i in range(1, 5)
    ]
    db_session.add_all(seats)
    await db_session.commit()
    event_id = ev.id
    seat_ids = [s.id for s in seats]

    # 1. 鎖定 2 張座位 + 付款 1 張
    r1 = client.post(
        f"/api/v1/events/{event_id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [seat_ids[0]]},
    )
    assert r1.status_code == 201, r1.text
    o1_id = r1.json()["id"]

    r2 = client.post(
        f"/api/v1/events/{event_id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [seat_ids[1]]},
    )
    assert r2.status_code == 201, r2.text

    p1 = client.post(
        "/api/v1/payments/mock/pay",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"order_id": o1_id, "outcome": "succeeded"},
    )
    assert p1.status_code == 200, p1.text

    # 正常交易後六大不變量必須全數通過
    with SyncSessionLocal() as sync_db:
        report = verify_event_consistency(sync_db, event_id)
        assert report.passed, report.failures
        assert report.seats_sold == 1 and report.order_items_sold == 1
        assert report.seats_held == 1 and report.order_items_held == 1
        assert report.duplicate_active_seats == 0
        assert report.max_tickets_per_user == 2
        assert report.invalid_fee_orders == 0
        assert report.event_last_seq == 3 and report.outbox_count == 3

        # 刻意破壞單一座位狀態（模擬異常寫入導致 seats 與 order_items 不一致）-> 校驗器必須抓出失敗
        sync_db.execute(
            text("UPDATE seats SET status = 'available' WHERE id = :sid"),
            {"sid": seat_ids[1]},
        )
        sync_db.commit()
        broken = verify_event_consistency(sync_db, event_id)
        assert not broken.passed
        assert any("Held seat mismatch" in f for f in broken.failures)
