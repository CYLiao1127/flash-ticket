"""活動 CRUD、樂觀鎖、Keyset 分頁與座位快照整合及並發測試"""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import pytest
from fastapi import HTTPException
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from app.auth.seed import SEED_ACCOUNTS, seed_users
from app.events.schemas import EventUpdateRequest, ZoneCreateRequest
from app.events.service import update_event
from app.models import Event, EventStatus, Order, OrderStatus, Seat, SeatStatus, User, Zone
from tests.conftest import TestSessionLocal


def _login(client, email: str, password: str) -> str:
    res = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert res.status_code == 200, f"Login failed for {email}: {res.text}"
    return res.json()["access_token"]


def _login_seed(client, role: str) -> str:
    acc = next(a for a in SEED_ACCOUNTS if a["role"].value == role)
    return _login(client, acc["email"], acc["password"])


def _sample_event_payload(
    title: str = "2026 Taipei Dome Concert",
    category: str = "concert",
    venue: str = "Taipei Dome",
    hours_from_now: int = 24,
):
    now = datetime.now(timezone.utc)
    return {
        "title": title,
        "description": "Grand opening live show",
        "category": category,
        "venue": venue,
        "start_time": (now + timedelta(hours=hours_from_now)).isoformat(),
        "end_time": (now + timedelta(hours=hours_from_now + 3)).isoformat(),
        "zones": [
            {"zone_code": "VIP", "name": "搖滾特區", "price": "5800.00"},
            {"zone_code": "A", "name": "看台 A 區", "price": "3200.00"},
        ],
    }


@pytest.mark.asyncio
async def test_event_crud_and_ownership_permissions(client, db_session):
    """驗證活動 CRUD 與建立者 / Admin / Viewer 角色存取控制"""
    seed_users()
    user_token = _login_seed(client, "user")
    viewer_token = _login_seed(client, "viewer")
    admin_token = _login_seed(client, "admin")

    # 註冊第二位一般使用者 (非活動建立者)
    other_email = "other_creator@flashticket.com"
    other_pw = "OtherPass123!"
    assert (
        client.post(
            "/api/v1/auth/register",
            json={"email": other_email, "password": other_pw},
        ).status_code
        == 201
    )
    other_token = _login(client, other_email, other_pw)

    payload = _sample_event_payload()

    # 1. Viewer 無法建立活動 (403)
    assert (
        client.post(
            "/api/v1/events",
            headers={"Authorization": f"Bearer {viewer_token}"},
            json=payload,
        ).status_code
        == 403
    )

    # 2. User 成功建立活動 -> 201, draft, version=1, last_seq=0
    create_res = client.post(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {user_token}"},
        json=payload,
    )
    assert create_res.status_code == 201, create_res.text
    created = create_res.json()
    event_id = created["id"]
    assert created["status"] == "draft"
    assert created["version"] == 1
    assert created["last_seq"] == 0
    assert len(created["zones"]) == 2

    # 3. Viewer 可讀取列表、詳情與座位快照 (200)
    assert (
        client.get(
            "/api/v1/events",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ).status_code
        == 200
    )
    assert (
        client.get(
            f"/api/v1/events/{event_id}",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ).status_code
        == 200
    )
    assert (
        client.get(
            f"/api/v1/events/{event_id}/seats",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ).status_code
        == 200
    )

    # 4. 非建立者 (other_user) 不可修改、發布或刪除該活動 (403)
    assert (
        client.patch(
            f"/api/v1/events/{event_id}",
            headers={"Authorization": f"Bearer {other_token}"},
            json={"version": 1, "title": "Hijacked"},
        ).status_code
        == 403
    )
    assert (
        client.post(
            f"/api/v1/events/{event_id}/publish",
            headers={"Authorization": f"Bearer {other_token}"},
        ).status_code
        == 403
    )
    assert (
        client.delete(
            f"/api/v1/events/{event_id}",
            headers={"Authorization": f"Bearer {other_token}"},
        ).status_code
        == 403
    )

    # 5. 建立者可更新標題與區域 (version=1 -> 2)
    update_res = client.patch(
        f"/api/v1/events/{event_id}",
        headers={"Authorization": f"Bearer {user_token}"},
        json={
            "version": 1,
            "title": "2026 Taipei Dome Concert (Encore)",
            "zones": [
                {"zone_code": "VIP", "name": "VIP 特區", "price": "6000.00"},
                {"zone_code": "B", "name": "看台 B 區", "price": "2200.00"},
            ],
        },
    )
    assert update_res.status_code == 200, update_res.text
    updated = update_res.json()
    assert updated["title"] == "2026 Taipei Dome Concert (Encore)"
    assert updated["version"] == 2
    assert {z["zone_code"]: Decimal(z["price"]) for z in updated["zones"]} == {
        "VIP": Decimal("6000.00"),
        "B": Decimal("2200.00"),
    }

    # 6. Admin 可更新與刪除其他建立者的活動 (200 / 204)
    admin_update_res = client.patch(
        f"/api/v1/events/{event_id}",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"version": 2, "title": "Admin Approved Title"},
    )
    assert admin_update_res.status_code == 200
    assert admin_update_res.json()["version"] == 3

    del_res = client.delete(
        f"/api/v1/events/{event_id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert del_res.status_code == 204


@pytest.mark.asyncio
async def test_optimistic_lock_prevents_concurrent_overwrite(client, db_session):
    """驗證樂觀鎖可擋下陳舊版本更新與真實並發寫入覆蓋 (409 Conflict)"""
    seed_users()
    user_token = _login_seed(client, "user")

    create_res = client.post(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {user_token}"},
        json=_sample_event_payload(title="Initial Title"),
    )
    assert create_res.status_code == 201
    event_id = create_res.json()["id"]

    # 1. 循序陳舊版本碰撞：Client A 與 Client B 皆讀到 version=1
    res_a = client.patch(
        f"/api/v1/events/{event_id}",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"version": 1, "title": "Client A Title"},
    )
    assert res_a.status_code == 200
    assert res_a.json()["version"] == 2

    res_b = client.patch(
        f"/api/v1/events/{event_id}",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"version": 1, "title": "Client B Stale Overwrite"},
    )
    assert res_b.status_code == 409

    # 資料庫保留 Client A 的標題與 version=2
    detail = client.get(
        f"/api/v1/events/{event_id}",
        headers={"Authorization": f"Bearer {user_token}"},
    ).json()
    assert detail["title"] == "Client A Title"
    assert detail["version"] == 2

    # 2. 真實資料庫並發衝突：兩個獨立 Session 同時以 version=2 更新同一活動
    actor = (
        await db_session.execute(select(User).where(User.email == "user@flashticket.com"))
    ).scalar_one()

    async def concurrent_patch(new_title: str):
        async with TestSessionLocal() as sess:
            return await update_event(
                sess,
                event_id,
                actor,
                EventUpdateRequest(version=2, title=new_title),
            )

    results = await asyncio.gather(
        concurrent_patch("Concurrent Writer 1"),
        concurrent_patch("Concurrent Writer 2"),
        return_exceptions=True,
    )

    successes = [r for r in results if isinstance(r, Event)]
    conflicts = [
        r for r in results if isinstance(r, HTTPException) and r.status_code == 409
    ]
    assert len(successes) == 1, f"Expected 1 winner, got results={results}"
    assert len(conflicts) == 1, f"Expected 1 conflict, got results={results}"
    assert successes[0].version == 3


@pytest.mark.asyncio
async def test_event_status_machine_draft_to_published(client, db_session):
    """驗證活動狀態機 (draft -> published)，禁止重複發布 (409)"""
    seed_users()
    user_token = _login_seed(client, "user")

    create_res = client.post(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {user_token}"},
        json=_sample_event_payload(),
    )
    assert create_res.status_code == 201
    event_id = create_res.json()["id"]
    assert create_res.json()["status"] == "draft"

    pub_res = client.post(
        f"/api/v1/events/{event_id}/publish",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert pub_res.status_code == 200
    assert pub_res.json()["status"] == "published"
    assert pub_res.json()["version"] == 2

    dup_pub_res = client.post(
        f"/api/v1/events/{event_id}/publish",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert dup_pub_res.status_code == 409


@pytest.mark.asyncio
async def test_delete_event_blocked_when_orders_exist(client, db_session):
    """驗證無訂單活動可正常刪除 (204)，已有訂單之活動嚴禁刪除 (409)"""
    seed_users()
    user_token = _login_seed(client, "user")

    # 1. 無訂單活動 -> 刪除成功
    ev1 = client.post(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {user_token}"},
        json=_sample_event_payload(title="Deletable Event"),
    ).json()
    assert (
        client.delete(
            f"/api/v1/events/{ev1['id']}",
            headers={"Authorization": f"Bearer {user_token}"},
        ).status_code
        == 204
    )
    assert (
        client.get(
            f"/api/v1/events/{ev1['id']}",
            headers={"Authorization": f"Bearer {user_token}"},
        ).status_code
        == 404
    )

    # 2. 已有訂單活動 -> 禁止刪除 (409 Conflict)
    ev2 = client.post(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {user_token}"},
        json=_sample_event_payload(title="Protected Event With Orders"),
    ).json()
    user_obj = (
        await db_session.execute(select(User).where(User.email == "user@flashticket.com"))
    ).scalar_one()

    db_session.add(
        Order(
            user_id=user_obj.id,
            event_id=ev2["id"],
            total_ticket_amount=Decimal("5800.00"),
            service_fee=Decimal("290.00"),
            total_amount=Decimal("6090.00"),
            status=OrderStatus.PENDING.value,
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
        )
    )
    await db_session.commit()

    del_blocked = client.delete(
        f"/api/v1/events/{ev2['id']}",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert del_blocked.status_code == 409

    # 確認活動依然存在
    assert (
        client.get(
            f"/api/v1/events/{ev2['id']}",
            headers={"Authorization": f"Bearer {user_token}"},
        ).status_code
        == 200
    )


@pytest.mark.asyncio
async def test_keyset_pagination_no_duplicates_or_omissions_under_concurrent_writes(
    client, db_session
):
    """
    ★ 關鍵分頁正確性測試：
    在持續寫入新活動的情況下，以 (created_at, id) Keyset 遊標翻頁不得重複或遺漏既有資料；
    並驗證相同 created_at 的 tie-breaker、篩選、排序與非法游標檢查。
    """
    seed_users()
    viewer_token = _login_seed(client, "viewer")
    user_obj = (
        await db_session.execute(select(User).where(User.email == "user@flashticket.com"))
    ).scalar_one()

    base_time = datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc)
    # 故意安排第 2、3 筆具備完全相同的 created_at 驗證複合欄位 tie-breaker
    offsets_minutes = [10, 20, 20, 30, 40, 50, 60]
    original_ids = []
    for idx, mins in enumerate(offsets_minutes):
        ev = Event(
            creator_id=user_obj.id,
            title=f"Keyset Event {idx}",
            category="sports" if idx % 2 == 0 else "music",
            venue=f"Arena {idx}",
            start_time=base_time + timedelta(days=idx + 1),
            end_time=base_time + timedelta(days=idx + 1, hours=3),
            status=EventStatus.PUBLISHED.value if idx % 2 == 0 else EventStatus.DRAFT.value,
            created_at=base_time + timedelta(minutes=mins),
            updated_at=base_time + timedelta(minutes=mins),
        )
        ev.zones.append(Zone(zone_code="A", name="Zone A", price=Decimal("1000.00")))
        db_session.add(ev)
        await db_session.flush()
        original_ids.append(ev.id)
    await db_session.commit()

    # 1. 讀取第 1 頁 (limit=3, desc)
    page1 = client.get(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {viewer_token}"},
        params={"limit": 3, "sort_by": "created_at", "sort_order": "desc"},
    )
    assert page1.status_code == 200
    p1_data = page1.json()
    assert len(p1_data["items"]) == 3
    assert p1_data["has_more"] is True
    assert p1_data["next_cursor"] is not None

    collected_ids = [item["id"] for item in p1_data["items"]]
    cursor = p1_data["next_cursor"]

    # 2. ★ 翻頁過程中突然有 2 筆更新的活動寫入頂部 (Offset 分頁在此必定發生重複!)
    for new_idx in range(2):
        new_ev = Event(
            creator_id=user_obj.id,
            title=f"Concurrent Newly Inserted Event {new_idx}",
            category="concert",
            venue="New Hall",
            start_time=base_time + timedelta(days=30 + new_idx),
            end_time=base_time + timedelta(days=30 + new_idx, hours=2),
            status=EventStatus.PUBLISHED.value,
            created_at=base_time + timedelta(hours=5 + new_idx),
            updated_at=base_time + timedelta(hours=5 + new_idx),
        )
        new_ev.zones.append(Zone(zone_code="A", name="Zone A", price=Decimal("1500.00")))
        db_session.add(new_ev)
    await db_session.commit()

    # 3. 繼續以 cursor 讀取第 2、3 頁直至結束
    while cursor is not None:
        page_res = client.get(
            "/api/v1/events",
            headers={"Authorization": f"Bearer {viewer_token}"},
            params={
                "limit": 3,
                "cursor": cursor,
                "sort_by": "created_at",
                "sort_order": "desc",
            },
        )
        assert page_res.status_code == 200
        page_data = page_res.json()
        collected_ids.extend(item["id"] for item in page_data["items"])
        cursor = page_data["next_cursor"]

    # 嚴格比對：零重複且完整包含原本 7 筆，不受寫入干擾
    assert len(collected_ids) == 7
    assert len(set(collected_ids)) == 7
    assert set(collected_ids) == set(original_ids)

    # 4. 驗證篩選與其他排序 (start_time asc + category + search)
    filtered = client.get(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {viewer_token}"},
        params={
            "category": "sports",
            "status": "published",
            "search": "Arena 2",
            "sort_by": "start_time",
            "sort_order": "asc",
        },
    )
    assert filtered.status_code == 200
    assert [e["title"] for e in filtered.json()["items"]] == ["Keyset Event 2"]

    # 5. 非法或排序條件不匹配的游標應回傳 400
    assert (
        client.get(
            "/api/v1/events",
            headers={"Authorization": f"Bearer {viewer_token}"},
            params={"cursor": "not-valid-base64"},
        ).status_code
        == 400
    )
    assert (
        client.get(
            "/api/v1/events",
            headers={"Authorization": f"Bearer {viewer_token}"},
            params={"cursor": p1_data["next_cursor"], "sort_by": "title"},
        ).status_code
        == 400
    )


@pytest.mark.asyncio
async def test_event_seats_snapshot_api(client, db_session):
    """驗證活動座位狀態快照 API (last_seq、分區統計、狀態篩選與禁止移除有座位區域)"""
    seed_users()
    user_token = _login_seed(client, "user")
    viewer_token = _login_seed(client, "viewer")

    created = client.post(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {user_token}"},
        json=_sample_event_payload(title="Snapshot Arena"),
    ).json()
    event_id = created["id"]

    # 設定 last_seq=42 並插入 5 個不同狀態的座位 (VIP:3, A:2)
    ev_row = (await db_session.execute(select(Event).where(Event.id == event_id))).scalar_one()
    ev_row.last_seq = 42
    seats = [
        Seat(event_id=event_id, zone_code="VIP", row_label="1", seat_number="1", status=SeatStatus.AVAILABLE.value),
        Seat(event_id=event_id, zone_code="VIP", row_label="1", seat_number="2", status=SeatStatus.HELD.value),
        Seat(event_id=event_id, zone_code="VIP", row_label="1", seat_number="3", status=SeatStatus.SOLD.value),
        Seat(event_id=event_id, zone_code="A", row_label="2", seat_number="1", status=SeatStatus.AVAILABLE.value),
        Seat(event_id=event_id, zone_code="A", row_label="2", seat_number="2", status=SeatStatus.AVAILABLE.value),
    ]
    db_session.add_all(seats)
    await db_session.commit()

    # 1. 完整快照與分區統計驗證
    snap_res = client.get(
        f"/api/v1/events/{event_id}/seats",
        headers={"Authorization": f"Bearer {viewer_token}"},
    )
    assert snap_res.status_code == 200
    snap = snap_res.json()
    assert snap["last_seq"] == 42
    assert snap["total_seats"] == 5
    assert snap["has_more"] is False
    zones_map = {z["zone_code"]: z for z in snap["zones"]}
    assert zones_map["VIP"]["available_seats"] == 1
    assert zones_map["VIP"]["held_seats"] == 1
    assert zones_map["VIP"]["sold_seats"] == 1
    assert zones_map["A"]["available_seats"] == 2

    # 2. 篩選 zone_code & status
    filtered_snap = client.get(
        f"/api/v1/events/{event_id}/seats",
        headers={"Authorization": f"Bearer {viewer_token}"},
        params={"zone_code": "VIP", "status": "held"},
    )
    assert filtered_snap.status_code == 200
    assert len(filtered_snap.json()["seats"]) == 1
    assert filtered_snap.json()["seats"][0]["seat_number"] == "2"

    # 3. after_id Keyset 分頁走訪全部 5 個座位
    all_seat_ids = []
    after_id = None
    while True:
        params = {"limit": 2}
        if after_id is not None:
            params["after_id"] = after_id
        page = client.get(
            f"/api/v1/events/{event_id}/seats",
            headers={"Authorization": f"Bearer {viewer_token}"},
            params=params,
        ).json()
        all_seat_ids.extend(s["id"] for s in page["seats"])
        if not page["has_more"]:
            break
        after_id = page["next_after_id"]
    assert len(all_seat_ids) == 5
    assert len(set(all_seat_ids)) == 5

    # 4. 嘗試透過 PATCH 移除已存在座位的 VIP 區域必須回傳 400
    remove_zone_res = client.patch(
        f"/api/v1/events/{event_id}",
        headers={"Authorization": f"Bearer {user_token}"},
        json={
            "version": 1,
            "zones": [{"zone_code": "A", "name": "看台 A 區", "price": "3200.00"}],
        },
    )
    assert remove_zone_res.status_code == 400


@pytest.mark.asyncio
async def test_concurrent_seat_insert_and_zone_removal_never_loses_seat(client, db_session):
    """
    ★ 資料安全關鍵並發測試：
    驗證當移除區域與並發新增座位同時發生時，因具備「Zone FOR UPDATE 行鎖 + fk ON DELETE RESTRICT」，
    絕不會發生檢查與刪除之間的時間窗繞過，也不可能級聯刪掉已寫入的新座位。
    """
    seed_users()
    user_token = _login_seed(client, "user")
    actor = (
        await db_session.execute(select(User).where(User.email == "user@flashticket.com"))
    ).scalar_one()

    # 1. 建立含 VIP 與 A 兩個分區、0 個座位的活動
    ev1 = client.post(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {user_token}"},
        json=_sample_event_payload(title="Concurrent Zone Safety Event 1"),
    ).json()
    eid1 = ev1["id"]

    # 交錯順序 A：交易 1 先插入 VIP 座位 (flush 取得 Zone FOR KEY SHARE，尚未 commit)
    # 此時交易 2 執行 update_event 嘗試移除 VIP，必須被 Zone FOR UPDATE 阻塞等待
    async with TestSessionLocal() as sess_insert:
        sess_insert.add(
            Seat(
                event_id=eid1,
                zone_code="VIP",
                row_label="1",
                seat_number="1",
                status=SeatStatus.AVAILABLE.value,
            )
        )
        await sess_insert.flush()

        async def attempt_remove_vip():
            async with TestSessionLocal() as sess_update:
                return await update_event(
                    sess_update,
                    eid1,
                    actor,
                    EventUpdateRequest(
                        version=1,
                        zones=[
                            ZoneCreateRequest(
                                zone_code="A",
                                name="看台 A 區",
                                price=Decimal("3200.00"),
                            )
                        ],
                    ),
                )

        task_update = asyncio.create_task(attempt_remove_vip())
        await asyncio.sleep(0.1)
        # update_event 正在等待 Zone FOR UPDATE，不可能繞過尚未提交的並發寫入
        assert not task_update.done()

        # 交易 1 提交新座位 -> 交易 2 解除阻塞後立即偵測到座位並拋出 400
        await sess_insert.commit()

        with pytest.raises(HTTPException) as exc_info:
            await task_update
        assert exc_info.value.status_code == 400
        assert "existing seats" in exc_info.value.detail

    # 確認 VIP 分區與新座位完好無損，活動版本仍為 1
    async with TestSessionLocal() as verify_sess:
        vip_zone = (
            await verify_sess.execute(
                select(Zone).where(Zone.event_id == eid1, Zone.zone_code == "VIP")
            )
        ).scalar_one_or_none()
        vip_seat = (
            await verify_sess.execute(
                select(Seat).where(Seat.event_id == eid1, Seat.zone_code == "VIP")
            )
        ).scalar_one_or_none()
        ev_state = (
            await verify_sess.execute(select(Event).where(Event.id == eid1))
        ).scalar_one()
        assert vip_zone is not None
        assert vip_seat is not None
        assert ev_state.version == 1

        # 2. 資料庫底層實體防線驗證：即便繞過應用層直接執行 SQL DELETE FROM zones，
        # fk_seats_zones_event_zone (ON DELETE RESTRICT) 也必須拋出 IntegrityError 阻擋級聯刪除
        with pytest.raises(IntegrityError):
            await verify_sess.execute(
                delete(Zone).where(Zone.event_id == eid1, Zone.zone_code == "VIP")
            )
            await verify_sess.flush()
        await verify_sess.rollback()

    # 交錯順序 B：刪區先取得 Zone FOR UPDATE 鎖定，並發增座後抵達等待鎖釋放後必須報錯失敗
    ev2 = client.post(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {user_token}"},
        json=_sample_event_payload(title="Concurrent Zone Safety Event 2"),
    ).json()
    eid2 = ev2["id"]

    async with TestSessionLocal() as sess_del:
        vip_row = (
            await sess_del.execute(
                select(Zone)
                .where(Zone.event_id == eid2, Zone.zone_code == "VIP")
                .with_for_update()
            )
        ).scalar_one()
        await sess_del.delete(vip_row)
        await sess_del.flush()

        async def concurrent_seat_insert():
            async with TestSessionLocal() as sess_ins2:
                sess_ins2.add(
                    Seat(
                        event_id=eid2,
                        zone_code="VIP",
                        row_label="1",
                        seat_number="1",
                        status=SeatStatus.AVAILABLE.value,
                    )
                )
                await sess_ins2.commit()

        task_ins = asyncio.create_task(concurrent_seat_insert())
        await asyncio.sleep(0.1)
        assert not task_ins.done()

        await sess_del.commit()
        with pytest.raises(IntegrityError):
            await task_ins

    # 3. 驗證在 ON DELETE RESTRICT 下，刪除「已有座位但無訂單」的活動仍能正確執行 (204)
    del_with_seats = client.delete(
        f"/api/v1/events/{eid1}",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert del_with_seats.status_code == 204
    assert (
        client.get(
            f"/api/v1/events/{eid1}",
            headers={"Authorization": f"Bearer {user_token}"},
        ).status_code
        == 404
    )


@pytest.mark.asyncio
async def test_naive_datetime_rejected_with_422(client, db_session):
    """驗證建立或單獨更新時間欄位時，若缺乏時區資訊必須在 Schema 層直接拒絕 (422)，不致引發 TypeError (500)"""
    seed_users()
    user_token = _login_seed(client, "user")

    # 1. 建立活動傳入無時區時間 -> 422
    bad_payload = _sample_event_payload()
    bad_payload["start_time"] = "2026-12-01T10:00:00"
    create_bad = client.post(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {user_token}"},
        json=bad_payload,
    )
    assert create_bad.status_code == 422

    # 2. 先建立合法有時區活動，再於 PATCH 僅傳入無時區的 end_time -> 422（不可為 500 TypeError）
    valid_ev = client.post(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {user_token}"},
        json=_sample_event_payload(title="Timezone Validation Event"),
    )
    assert valid_ev.status_code == 201
    event_id = valid_ev.json()["id"]

    patch_naive_end = client.patch(
        f"/api/v1/events/{event_id}",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"version": 1, "end_time": "2030-01-01T12:00:00"},
    )
    assert patch_naive_end.status_code == 422

    patch_naive_start = client.patch(
        f"/api/v1/events/{event_id}",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"version": 1, "start_time": "2026-01-01T12:00:00"},
    )
    assert patch_naive_start.status_code == 422

    # 3. 傳入附帶時區 (+08:00) 的單一更新時間可正常比較並轉為 UTC 更新 (200)
    new_end_taipei = (datetime.now(timezone.utc) + timedelta(hours=48)).astimezone(
        timezone(timedelta(hours=8))
    )
    patch_valid = client.patch(
        f"/api/v1/events/{event_id}",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"version": 1, "end_time": new_end_taipei.isoformat()},
    )
    assert patch_valid.status_code == 200
    assert patch_valid.json()["version"] == 2
