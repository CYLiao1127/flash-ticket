"""金流模擬、HMAC-SHA256 簽章防重放、訂單查詢／自主取消與 DB-Clock 逾時釋放測試 (Commit 15)"""

import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import pytest
from sqlalchemy import select, text

from app.auth.seed import SEED_ACCOUNTS, seed_users
from app.core.redis import get_redis
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
    Zone,
)
from app.payments.service import release_expired_orders
from app.payments.signature import compute_webhook_signature


def _login(client, email: str, password: str) -> str:
    res = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert res.status_code == 200, res.text
    return res.json()["access_token"]


def _login_seed(client, role: str) -> str:
    acc = next(a for a in SEED_ACCOUNTS if a["role"].value == role)
    return _login(client, acc["email"], acc["password"])


async def _create_live_event_with_seats(db_session, creator_id: int, seat_count: int = 4):
    now = datetime.now(timezone.utc)
    event = Event(
        creator_id=creator_id,
        title="Payment Flow Concert",
        description="Payment & timeout test event",
        category="concert",
        venue="Taipei Arena",
        start_time=now - timedelta(hours=1),
        end_time=now + timedelta(days=1),
        status=EventStatus.PUBLISHED.value,
    )
    db_session.add(event)
    await db_session.flush()

    db_session.add(
        Zone(event_id=event.id, zone_code="A", name="A 區", price=Decimal("1050.00"))
    )
    await db_session.flush()

    seats = []
    for i in range(1, seat_count + 1):
        seat = Seat(
            event_id=event.id,
            zone_code="A",
            row_label="1",
            seat_number=str(i),
            status=SeatStatus.AVAILABLE.value,
        )
        db_session.add(seat)
        seats.append(seat)

    await db_session.commit()
    for s in seats:
        await db_session.refresh(s)
    return event, seats


@pytest.mark.asyncio
async def test_webhook_hmac_signature_5min_replay_and_event_idempotency(
    client, db_session
):
    """
    驗證：
    1. 缺少簽章標頭、簽章不符、竄改 Body、超過 5 分鐘時間戳（過去或未來）皆回傳 400。
    2. 合法 HMAC-SHA256 簽章成功將訂單轉為 paid、座位轉為 sold、寫入 seat_sold Outbox。
    3. 相同 webhook_event_id 重送回傳 200 (duplicate=True) 且不重複遞增 events.last_seq。
    """
    seed_users()
    user_token = _login_seed(client, "user")
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    event, seats = await _create_live_event_with_seats(db_session, admin_user.id)

    # 預訂 1 張座位（票價 1050 -> 5% 服務費 ROUND_HALF_UP = 53 -> 總計 1103）
    res_order = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [seats[0].id]},
    )
    assert res_order.status_code == 201, res_order.text
    order = res_order.json()
    assert Decimal(order["total_amount"]) == Decimal("1103.00")

    redis = get_redis()
    wh_id = f"wh_test_signed_{order['id']}"
    await redis.delete(f"webhook:payment:event:{wh_id}")

    payload_dict = {
        "webhook_event_id": wh_id,
        "order_id": order["id"],
        "payment_status": "succeeded",
        "amount": "1103.00",
    }
    raw_body = json.dumps(payload_dict).encode("utf-8")
    now_ts = int(time.time())

    # 1. 缺簽章標頭 -> 400
    assert (
        client.post(
            "/api/v1/payments/webhook",
            content=raw_body,
            headers={"Content-Type": "application/json"},
        ).status_code
        == 400
    )

    # 2. 錯誤簽章 -> 400
    assert (
        client.post(
            "/api/v1/payments/webhook",
            content=raw_body,
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Timestamp": str(now_ts),
                "X-Webhook-Signature": "deadbeef" * 8,
            },
        ).status_code
        == 400
    )

    # 3. 超過 5 分鐘防重放視窗（過去 301 秒與未來 301 秒）-> 400
    for stale_ts in (now_ts - 301, now_ts + 301):
        stale_sig = compute_webhook_signature(str(stale_ts), raw_body)
        stale_res = client.post(
            "/api/v1/payments/webhook",
            content=raw_body,
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Timestamp": str(stale_ts),
                "X-Webhook-Signature": stale_sig,
            },
        )
        assert stale_res.status_code == 400
        assert "5-minute" in stale_res.json()["detail"]

    # 4. 竄改金額後的 Body 搭配原簽章 -> 400
    valid_sig = compute_webhook_signature(str(now_ts), raw_body)
    tampered_body = json.dumps({**payload_dict, "amount": "1.00"}).encode("utf-8")
    assert (
        client.post(
            "/api/v1/payments/webhook",
            content=tampered_body,
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Timestamp": str(now_ts),
                "X-Webhook-Signature": valid_sig,
            },
        ).status_code
        == 400
    )

    # 5. 合法簽章成功入帳 -> 200, order=paid, seat=sold, outbox=seat_sold (seq=2)
    ok_res = client.post(
        "/api/v1/payments/webhook",
        content=raw_body,
        headers={
            "Content-Type": "application/json",
            "X-Webhook-Timestamp": str(now_ts),
            "X-Webhook-Signature": valid_sig,
        },
    )
    assert ok_res.status_code == 200, ok_res.text
    ok_data = ok_res.json()
    assert ok_data["order_status"] == "paid"
    assert ok_data["action"] == "paid"
    assert ok_data["duplicate"] is False
    assert ok_data["sequence_id"] == 2

    await db_session.refresh(seats[0])
    assert seats[0].status == SeatStatus.SOLD.value
    assert seats[0].expires_at is None

    # 6. 相同 webhook_event_id 重送 -> 200 (duplicate=True)，不重複遞增 events.last_seq
    dup_res = client.post(
        "/api/v1/payments/webhook",
        content=raw_body,
        headers={
            "Content-Type": "application/json",
            "X-Webhook-Timestamp": str(now_ts),
            "X-Webhook-Signature": valid_sig,
        },
    )
    assert dup_res.status_code == 200, dup_res.text
    assert dup_res.json()["duplicate"] is True
    assert dup_res.json()["order_status"] == "paid"

    await db_session.refresh(event)
    assert event.last_seq == 2

    # 7. 驗證已完成快取 TTL 採用長 TTL（> 處理鎖 TTL），且處理鎖 TTL 獨立維持短時效
    from app.payments.service import (
        WEBHOOK_COMPLETED_CACHE_TTL_SECONDS,
        WEBHOOK_PROCESSING_LOCK_TTL_SECONDS,
    )

    assert WEBHOOK_PROCESSING_LOCK_TTL_SECONDS < WEBHOOK_COMPLETED_CACHE_TTL_SECONDS
    completed_ttl = await redis.ttl(f"webhook:payment:event:{wh_id}")
    assert WEBHOOK_PROCESSING_LOCK_TTL_SECONDS < completed_ttl <= WEBHOOK_COMPLETED_CACHE_TTL_SECONDS
    await redis.delete(f"webhook:payment:event:{wh_id}")


@pytest.mark.asyncio
async def test_mock_payment_gateway_order_queries_and_rbac(client, db_session):
    """驗證訂單清單／明細查詢權限、模擬金流失敗與成功入帳，以及已付款訂單禁止取消"""
    seed_users()
    user_token = _login_seed(client, "user")
    viewer_token = _login_seed(client, "viewer")
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    other_email = "other_order_viewer@flashticket.com"
    assert (
        client.post(
            "/api/v1/auth/register",
            json={"email": other_email, "password": "Password123!"},
        ).status_code
        == 201
    )
    other_token = _login(client, other_email, "Password123!")

    event, seats = await _create_live_event_with_seats(db_session, admin_user.id)

    order_id = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [seats[0].id]},
    ).json()["id"]

    # 本人與 Viewer 可查詢明細，其他一般使用者 -> 403
    assert (
        client.get(
            f"/api/v1/orders/{order_id}",
            headers={"Authorization": f"Bearer {user_token}"},
        ).status_code
        == 200
    )
    assert (
        client.get(
            f"/api/v1/orders/{order_id}",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ).status_code
        == 200
    )
    assert (
        client.get(
            f"/api/v1/orders/{order_id}",
            headers={"Authorization": f"Bearer {other_token}"},
        ).status_code
        == 403
    )

    # 訂單清單：本人看到 1 筆，其他一般使用者看到 0 筆
    assert (
        client.get(
            "/api/v1/orders",
            headers={"Authorization": f"Bearer {user_token}"},
        ).json()["total"]
        == 1
    )
    assert (
        client.get(
            "/api/v1/orders",
            headers={"Authorization": f"Bearer {other_token}"},
        ).json()["total"]
        == 0
    )

    # 他人或 Viewer 嘗試代付 -> 403
    assert (
        client.post(
            "/api/v1/payments/mock/pay",
            headers={"Authorization": f"Bearer {other_token}"},
            json={"order_id": order_id, "outcome": "succeeded"},
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/api/v1/payments/mock/pay",
            headers={"Authorization": f"Bearer {viewer_token}"},
            json={"order_id": order_id, "outcome": "succeeded"},
        ).status_code
        == 403
    )

    # 模擬金流失敗 -> 訂單仍維持 pending 可重試
    fail_pay = client.post(
        "/api/v1/payments/mock/pay",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"order_id": order_id, "outcome": "failed"},
    )
    assert fail_pay.status_code == 200
    assert fail_pay.json()["webhook_result"]["order_status"] == "pending"

    # 模擬金流成功 -> 轉為 paid
    ok_pay = client.post(
        "/api/v1/payments/mock/pay",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"order_id": order_id, "outcome": "succeeded"},
    )
    assert ok_pay.status_code == 200, ok_pay.text
    assert ok_pay.json()["webhook_result"]["order_status"] == "paid"

    # 已付款訂單不可自主取消 -> 409
    assert (
        client.post(
            f"/api/v1/orders/{order_id}/cancel",
            headers={"Authorization": f"Bearer {user_token}"},
        ).status_code
        == 409
    )


@pytest.mark.asyncio
async def test_user_self_cancel_order_releases_seats_and_writes_outbox(
    client, db_session
):
    """驗證自主取消待付款訂單：同事務釋放 seats 與 order_items、寫入 seat_released Outbox，且他人可立即重購"""
    seed_users()
    user_token = _login_seed(client, "user")
    admin_token = _login_seed(client, "admin")
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    event, seats = await _create_live_event_with_seats(db_session, admin_user.id)

    res = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [seats[0].id, seats[1].id]},
    )
    assert res.status_code == 201
    order_id = res.json()["id"]

    cancel_res = client.post(
        f"/api/v1/orders/{order_id}/cancel",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert cancel_res.status_code == 200, cancel_res.text
    cancelled = cancel_res.json()
    assert cancelled["status"] == "cancelled"
    assert all(item["status"] == "cancelled" for item in cancelled["items"])

    # 重複取消 -> 409
    assert (
        client.post(
            f"/api/v1/orders/{order_id}/cancel",
            headers={"Authorization": f"Bearer {user_token}"},
        ).status_code
        == 409
    )

    # 驗證座位已釋放且 Outbox 寫入 seat_released (sequence_id=2)
    for s in (seats[0], seats[1]):
        await db_session.refresh(s)
        assert s.status == SeatStatus.AVAILABLE.value
        assert s.held_by is None
        assert s.expires_at is None

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
    assert [r.event_type for r in outbox_rows] == ["seat_held", "seat_released"]
    assert [r.sequence_id for r in outbox_rows] == [1, 2]
    assert outbox_rows[1].payload["reason"] == "user_cancelled"

    # 另一位買家應可立即重新鎖定同一組座位（證明 uq_active_seat_order 已釋放）
    rebook = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"seat_ids": [seats[0].id, seats[1].id]},
    )
    assert rebook.status_code == 201, rebook.text


@pytest.mark.asyncio
async def test_db_clock_timeout_worker_releases_expired_orders_idempotently(
    client, db_session
):
    """驗證 DB-Clock 逾時釋放 Worker：僅釋放 expires_at < NOW() 之過期訂單，未過期者不受影響且重複執行冪等"""
    seed_users()
    user_token = _login_seed(client, "user")
    admin_token = _login_seed(client, "admin")
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    event, seats = await _create_live_event_with_seats(db_session, admin_user.id)

    # 建立 2 筆待付款訂單
    o1_id = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [seats[0].id]},
    ).json()["id"]
    o2_id = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"seat_ids": [seats[1].id]},
    ).json()["id"]

    # 將 o1 設為 1 分鐘前過期（依資料庫時鐘 NOW()）
    await db_session.execute(
        text(
            "UPDATE orders SET expires_at = NOW() - INTERVAL '1 minute' WHERE id = :oid"
        ),
        {"oid": o1_id},
    )
    await db_session.commit()

    released_count = await release_expired_orders(db_session)
    assert released_count == 1

    # 重複執行必須冪等回傳 0
    assert await release_expired_orders(db_session) == 0

    o1 = (await db_session.execute(select(Order).where(Order.id == o1_id))).scalar_one()
    o2 = (await db_session.execute(select(Order).where(Order.id == o2_id))).scalar_one()
    assert o1.status == OrderStatus.CANCELLED.value
    assert o2.status == OrderStatus.PENDING.value

    await db_session.refresh(seats[0])
    await db_session.refresh(seats[1])
    assert seats[0].status == SeatStatus.AVAILABLE.value
    assert seats[1].status == SeatStatus.HELD.value

    # 驗證 o1 釋放後產生 seat_released Outbox 事件 (reason='payment_timeout')
    timeout_outbox = (
        await db_session.execute(
            select(EventOutbox).where(
                EventOutbox.event_id == event.id,
                EventOutbox.event_type == "seat_released",
            )
        )
    ).scalar_one()
    assert timeout_outbox.payload["order_id"] == o1_id
    assert timeout_outbox.payload["reason"] == "payment_timeout"

    # 被逾時釋放的 seats[0] 可立即由他人重新購買
    rebook = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"seat_ids": [seats[0].id]},
    )
    assert rebook.status_code == 201, rebook.text


@pytest.mark.asyncio
async def test_late_webhook_revives_cancelled_order_when_all_seats_available(
    client, db_session
):
    """
    ★ 關鍵 Q2.1 測試（復活分支）：
    訂單逾時釋放轉為 cancelled 後，遲到的付款成功 Webhook 到達時，
    若原訂單的所有座位仍為 available，應於同一交易內復活訂單為 paid 並將座位轉為 sold。
    """
    seed_users()
    user_token = _login_seed(client, "user")
    admin_token = _login_seed(client, "admin")
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()
    buyer = (
        await db_session.execute(select(User).where(User.email == "user@flashticket.com"))
    ).scalar_one()

    event, seats = await _create_live_event_with_seats(db_session, admin_user.id)

    order_resp = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [seats[0].id, seats[1].id]},
    ).json()
    order_id = order_resp["id"]
    total_amount = order_resp["total_amount"]

    # 模擬逾時並由 Worker 釋放為 cancelled
    await db_session.execute(
        text("UPDATE orders SET expires_at = NOW() - INTERVAL '2 minutes' WHERE id = :oid"),
        {"oid": order_id},
    )
    await db_session.commit()
    assert await release_expired_orders(db_session) == 1

    # 發送遲到的付款成功簽章 Webhook
    redis = get_redis()
    wh_id = f"wh_late_revive_{order_id}"
    await redis.delete(f"webhook:payment:event:{wh_id}")

    raw_body = json.dumps(
        {
            "webhook_event_id": wh_id,
            "order_id": order_id,
            "payment_status": "succeeded",
            "amount": str(total_amount),
        }
    ).encode("utf-8")
    ts = str(int(time.time()))
    sig = compute_webhook_signature(ts, raw_body)

    res = client.post(
        "/api/v1/payments/webhook",
        content=raw_body,
        headers={
            "Content-Type": "application/json",
            "X-Webhook-Timestamp": ts,
            "X-Webhook-Signature": sig,
        },
    )
    assert res.status_code == 200, res.text
    data = res.json()
    assert data["action"] == "revived"
    assert data["order_status"] == "paid"
    assert data["refund_triggered"] is False
    assert data["sequence_id"] == 3  # 1: held, 2: released, 3: revived sold

    # 驗證原座位全部轉為 sold 且屬於原買家，order_items 轉回 paid
    for s in (seats[0], seats[1]):
        await db_session.refresh(s)
        assert s.status == SeatStatus.SOLD.value
        assert s.held_by == buyer.id
        assert s.expires_at is None

    order_detail = client.get(
        f"/api/v1/orders/{order_id}",
        headers={"Authorization": f"Bearer {user_token}"},
    ).json()
    assert order_detail["status"] == "paid"
    assert all(item["status"] == "paid" for item in order_detail["items"])

    # 驗證 Outbox 記錄 revived=True 之 seat_sold 事件
    revived_outbox = (
        await db_session.execute(
            select(EventOutbox).where(
                EventOutbox.event_id == event.id,
                EventOutbox.event_type == "seat_sold",
            )
        )
    ).scalar_one()
    assert revived_outbox.payload["revived"] is True
    assert revived_outbox.payload["order_id"] == order_id

    # 復活後的座位他人不可再搶購 (409)
    assert (
        client.post(
            f"/api/v1/events/{event.id}/tickets/reserve",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={"seat_ids": [seats[0].id]},
        ).status_code
        == 409
    )
    await redis.delete(f"webhook:payment:event:{wh_id}")


@pytest.mark.asyncio
async def test_late_webhook_triggers_refund_when_any_seat_taken_by_another_user(
    client, db_session
):
    """
    ★ 關鍵 Q2.1 測試（退款分支與部分佔用全筆退款保證）：
    訂單逾時釋放後，原 2 張座位中有 1 張已被他人搶走、另 1 張仍為 available；
    遲到付款成功 Webhook 到達時必須整筆標記為 cancelled_refund_required 觸發退款，
    不得覆寫他人座位，也不得部分復活剩餘座位。
    """
    seed_users()
    user_token = _login_seed(client, "user")
    admin_token = _login_seed(client, "admin")
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    event, seats = await _create_live_event_with_seats(db_session, admin_user.id)

    order_resp = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [seats[0].id, seats[1].id]},
    ).json()
    order_id = order_resp["id"]
    total_amount = order_resp["total_amount"]

    # 1. 逾時釋放
    await db_session.execute(
        text("UPDATE orders SET expires_at = NOW() - INTERVAL '2 minutes' WHERE id = :oid"),
        {"oid": order_id},
    )
    await db_session.commit()
    assert await release_expired_orders(db_session) == 1

    # 2. 另一位買家 (Admin) 搶走 seats[0]，留下 seats[1] 仍為 available
    rebook_res = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"seat_ids": [seats[0].id]},
    )
    assert rebook_res.status_code == 201

    # 3. 遲到付款成功 Webhook 到達 -> 必須整筆轉 cancelled_refund_required
    redis = get_redis()
    wh_id_1 = f"wh_late_refund_1_{order_id}"
    wh_id_2 = f"wh_late_refund_2_{order_id}"
    await redis.delete(
        f"webhook:payment:event:{wh_id_1}",
        f"webhook:payment:event:{wh_id_2}",
    )

    def _send_webhook(wh_event_id: str):
        body = json.dumps(
            {
                "webhook_event_id": wh_event_id,
                "order_id": order_id,
                "payment_status": "succeeded",
                "amount": str(total_amount),
            }
        ).encode("utf-8")
        ts = str(int(time.time()))
        sig = compute_webhook_signature(ts, body)
        return client.post(
            "/api/v1/payments/webhook",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Timestamp": ts,
                "X-Webhook-Signature": sig,
            },
        )

    res1 = _send_webhook(wh_id_1)
    assert res1.status_code == 200, res1.text
    d1 = res1.json()
    assert d1["action"] == "refund_required"
    assert d1["order_status"] == "cancelled_refund_required"
    assert d1["refund_triggered"] is True

    # 驗證 seats[0] 仍屬於 Admin (held)，seats[1] 仍為 available（不部分復活）
    await db_session.refresh(seats[0])
    await db_session.refresh(seats[1])
    assert seats[0].status == SeatStatus.HELD.value
    assert seats[0].held_by == admin_user.id
    assert seats[1].status == SeatStatus.AVAILABLE.value

    order_detail = client.get(
        f"/api/v1/orders/{order_id}",
        headers={"Authorization": f"Bearer {user_token}"},
    ).json()
    assert order_detail["status"] == "cancelled_refund_required"
    assert all(item["status"] == "cancelled" for item in order_detail["items"])

    # 驗證 Outbox 記錄 order_refund_required 事件且標出衝突座位 ID
    refund_outbox = (
        await db_session.execute(
            select(EventOutbox).where(
                EventOutbox.event_id == event.id,
                EventOutbox.event_type == "order_refund_required",
            )
        )
    ).scalar_one()
    assert refund_outbox.payload["order_id"] == order_id
    assert refund_outbox.payload["conflicting_seat_ids"] == [seats[0].id]

    # 4. 驗證 Worker 消費 order_refund_required Outbox 實際執行模擬自動退款，且重複執行冪等不重複退款
    from app.payments.service import execute_mock_refund
    from app.workers.runner import poll_and_relay_outbox

    refund_receipt_key = f"payment:refund:order:{order_id}"
    await redis.delete(refund_receipt_key)

    relayed = await poll_and_relay_outbox(db_session, redis=redis)
    assert relayed >= 1

    receipt_raw = await redis.get(refund_receipt_key)
    assert receipt_raw is not None
    receipt = json.loads(receipt_raw)
    assert receipt["order_id"] == order_id
    assert receipt["status"] == "refunded"
    assert Decimal(receipt["amount"]) == Decimal(str(total_amount))

    # 重複呼叫退款閘道必須冪等回傳同一筆 refund_id（不重複退款）
    dup_refund = await execute_mock_refund(
        order_id=order_id,
        amount=str(total_amount),
        webhook_event_id=wh_id_1,
        redis=redis,
    )
    assert dup_refund["duplicate"] is True
    assert dup_refund["refund_id"] == receipt["refund_id"]

    # 5. 若後續再收到不同 event_id 的遲到 Webhook，應冪等維持 already_refund_required
    res2 = _send_webhook(wh_id_2)
    assert res2.status_code == 200, res2.text
    d2 = res2.json()
    assert d2["action"] == "already_refund_required"
    assert d2["order_status"] == "cancelled_refund_required"
    assert d2["refund_triggered"] is True

    await redis.delete(
        f"webhook:payment:event:{wh_id_1}",
        f"webhook:payment:event:{wh_id_2}",
        refund_receipt_key,
    )
