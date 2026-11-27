"""Leader-Elected Outbox Relay、WebSocket 跨副本廣播、斷線補償與慢客戶端保護測試 (Commit 20 / Q4)"""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json

import pytest
from sqlalchemy import select
from starlette.websockets import WebSocketDisconnect

from app.auth.seed import SEED_ACCOUNTS, seed_users
from app.core.outbox import create_outbox_event
from app.models import (
    Event,
    EventOutbox,
    EventStatus,
    OutboxStatus,
    Seat,
    SeatStatus,
    User,
    Zone,
)
from app.realtime.hub import EventBroadcastHub, WS_CLOSE_SLOW_CLIENT
from app.workers.runner import poll_and_relay_outbox
from tests.conftest import TestSessionLocal


def _login_seed(client, role: str) -> str:
    acc = next(a for a in SEED_ACCOUNTS if a["role"].value == role)
    res = client.post(
        "/api/v1/auth/login",
        json={"email": acc["email"], "password": acc["password"]},
    )
    assert res.status_code == 200, res.text
    return res.json()["access_token"]


async def _create_published_event_with_seats(db_session, seat_count: int = 3):
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()
    now = datetime.now(timezone.utc)
    event = Event(
        creator_id=admin_user.id,
        title="Realtime WS Concert",
        category="concert",
        venue="Taipei Dome",
        start_time=now - timedelta(hours=1),
        end_time=now + timedelta(days=1),
        status=EventStatus.PUBLISHED.value,
    )
    db_session.add(event)
    await db_session.flush()
    db_session.add(Zone(event_id=event.id, zone_code="A", name="A 區", price=Decimal("1000.00")))
    await db_session.flush()
    seats = [
        Seat(
            event_id=event.id,
            zone_code="A",
            row_label="1",
            seat_number=str(i + 1),
            status=SeatStatus.AVAILABLE.value,
        )
        for i in range(seat_count)
    ]
    db_session.add_all(seats)
    await db_session.commit()
    for s in seats:
        await db_session.refresh(s)
    return event, seats


@pytest.mark.asyncio
async def test_leader_elected_outbox_relay_prevents_concurrent_reordering_q4(db_session):
    """
    ★ 關鍵 Q4 驗證：
    雙 Worker 同時執行 `poll_and_relay_outbox` 時，僅能有 1 位透過 `pg_try_advisory_xact_lock`
    成為 Leader 並嚴格依 `(event_id, sequence_id ASC)` 順序發布；另一位非 Leader 立即跳過回傳 0。
    """
    seed_users()
    event, _ = await _create_published_event_with_seats(db_session, seat_count=1)

    for seq in (1, 2, 3):
        db_session.add(
            create_outbox_event(
                event_id=event.id,
                sequence_id=seq,
                event_type="seat_held",
                payload={"seat_number": seq, "request_id": f"req-seq-{seq}"},
            )
        )
    await db_session.commit()

    leader_acquired = asyncio.Event()
    release_leader = asyncio.Event()
    published_sequences = []

    class RecordingRedis:
        async def publish(self, channel, message):
            data = json.loads(message)
            published_sequences.append(data["sequence_id"])
            return 1

    recording_redis = RecordingRedis()

    async def hold_leader_lock():
        leader_acquired.set()
        await asyncio.wait_for(release_leader.wait(), timeout=3.0)

    async def run_worker_1():
        async with TestSessionLocal() as s1:
            return await poll_and_relay_outbox(
                s1,
                redis=recording_redis,
                on_leader_acquired=hold_leader_lock,
            )

    async def run_worker_2():
        await asyncio.wait_for(leader_acquired.wait(), timeout=3.0)
        async with TestSessionLocal() as s2:
            # Worker 1 正持有 Leader 諮詢鎖，Worker 2 必須立刻跳過回傳 0
            return await poll_and_relay_outbox(s2, redis=recording_redis)

    t1 = asyncio.create_task(run_worker_1())
    t2 = asyncio.create_task(run_worker_2())

    worker2_count = await t2
    assert worker2_count == 0

    release_leader.set()
    worker1_count = await t1
    assert worker1_count == 3
    assert published_sequences == [1, 2, 3]

    # 驗證所有 Outbox 事件皆已標記為 published
    pending_left = list(
        (
            await db_session.execute(
                select(EventOutbox).where(
                    EventOutbox.event_id == event.id,
                    EventOutbox.status == OutboxStatus.PENDING.value,
                )
            )
        )
        .scalars()
        .all()
    )
    assert pending_left == []


@pytest.mark.asyncio
async def test_websocket_auth_live_broadcast_and_reconnect_catchup_q4(client, db_session):
    """
    驗證 WebSocket 認證、即時座位廣播與斷線補償（含 REST `/updates` 與 WS `?last_seq=`）：
    1. 未帶 Token 或 Token 無效時 WebSocket 連線必被拒絕 (4401)。
    2. 合法 JWT 連線至 `/ws/events/{id}` 立即收到 `connected` 幀與 `last_seq`，且支援 `ping`/`pong`。
    3. 搶票寫入 Outbox -> Leader Relay 發布至 Redis Pub/Sub -> 已連線 WS 客戶端即時收到 `seat_held` 與正確 `sequence_id`。
    4. 斷線補償 REST API (`GET /api/v1/events/{id}/updates?after_seq=0`) 與 WS 重連 (`?last_seq=0`)
       皆能精準補齊遺漏序號；缺口超過 limit 時回傳 `resync_required=True`。
    """
    seed_users()
    user_token = _login_seed(client, "user")
    event, seats = await _create_published_event_with_seats(db_session, seat_count=2)

    # 1. 未授權 WebSocket 必被 4401 拒絕
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect(f"/ws/events/{event.id}"):
            pass
    assert exc_info.value.code == 4401

    # 2. 合法 JWT 連線並驗證 connected 握手幀與 ping/pong
    with client.websocket_connect(f"/ws/events/{event.id}?token={user_token}") as ws:
        welcome = ws.receive_json()
        assert welcome["type"] == "connected"
        assert welcome["event_id"] == event.id
        assert welcome["last_seq"] == 0

        ws.send_text("ping")
        assert ws.receive_json() == {"type": "pong"}

        # 3. 搶票產生 sequence_id=1 的 Outbox，並由 Leader Relay 發布至 Redis Pub/Sub
        res = client.post(
            f"/api/v1/events/{event.id}/tickets/reserve",
            headers={"Authorization": f"Bearer {user_token}"},
            json={"seat_ids": [seats[0].id]},
        )
        assert res.status_code == 201, res.text

        relayed = await poll_and_relay_outbox(db_session)
        assert relayed == 1

        # 客戶端即時收到該筆座位變更推播
        live_msg = ws.receive_json()
        assert live_msg["event_id"] == event.id
        assert live_msg["sequence_id"] == 1
        assert live_msg["event_type"] == "seat_held"
        assert seats[0].id in [s["seat_id"] for s in live_msg["payload"]["seats"]]

    # 4. 再搶一張票產生 sequence_id=2，驗證 REST 斷線補償 API 與 resync_required 邏輯
    res2 = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [seats[1].id]},
    )
    assert res2.status_code == 201, res2.text

    # REST 增量補償 (after_seq=0 -> 取得 seq 1, 2)
    delta_res = client.get(
        f"/api/v1/events/{event.id}/updates?after_seq=0&limit=10",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert delta_res.status_code == 200, delta_res.text
    delta_body = delta_res.json()
    assert delta_body["current_last_seq"] == 2
    assert delta_body["resync_required"] is False
    assert [e["sequence_id"] for e in delta_body["events"]] == [1, 2]

    # 當缺口超過 limit (limit=1 但缺 2 筆) -> resync_required=True 引導重拉快照
    resync_res = client.get(
        f"/api/v1/events/{event.id}/updates?after_seq=0&limit=1",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert resync_res.status_code == 200
    assert resync_res.json()["resync_required"] is True
    assert resync_res.json()["events"] == []

    # WS 斷線重連帶 ?last_seq=0 -> 自動補發 seq 1 與 seq 2
    with client.websocket_connect(
        f"/ws/events/{event.id}?token={user_token}&last_seq=0"
    ) as ws_reconn:
        w2 = ws_reconn.receive_json()
        assert w2["type"] == "connected"
        assert w2["last_seq"] == 2
        c1 = ws_reconn.receive_json()
        c2 = ws_reconn.receive_json()
        assert c1["type"] == "catchup" and c1["sequence_id"] == 1
        assert c2["type"] == "catchup" and c2["sequence_id"] == 2


@pytest.mark.asyncio
async def test_slow_websocket_client_is_dropped_without_blocking_fast_clients_q4():
    """
    ★ 關鍵 Q4 慢客戶端保護測試：
    當慢客戶端的有界發送佇列已滿時，EventBroadcastHub 必須主動關閉該慢客戶端 (Close Code 1008)
    並移出房間防 OOM，同時確保同房間的健康快客戶端完全不受影響、持續收到全部訊息。
    """
    hub = EventBroadcastHub()
    slow_block = asyncio.Event()
    fast_received = []
    slow_closed_codes = []

    class FakeSlowWebSocket:
        async def send_text(self, data: str):
            await slow_block.wait()

        async def close(self, code: int = 1000, reason: str = ""):
            slow_closed_codes.append((code, reason))

    class FakeFastWebSocket:
        async def send_text(self, data: str):
            fast_received.append(data)

        async def close(self, code: int = 1000, reason: str = ""):
            pass

    slow_ws = FakeSlowWebSocket()
    fast_ws = FakeFastWebSocket()

    await hub.connect(
        event_id=99,
        websocket=slow_ws,  # type: ignore[arg-type]
        max_queue_size=2,
        start_redis_listener=False,
    )
    await hub.connect(
        event_id=99,
        websocket=fast_ws,  # type: ignore[arg-type]
        max_queue_size=16,
        start_redis_listener=False,
    )
    assert hub.active_client_count(99) == 2

    # 連續廣播 6 則訊息：第 1 則卡在 slow_ws.send_text，第 2-3 則填滿 max_queue_size=2，第 4 則觸發溢位斷線！
    for i in range(1, 7):
        await hub.broadcast_raw(99, json.dumps({"seq": i}))
        await asyncio.sleep(0.02)

    await asyncio.sleep(0.05)

    # 慢客戶端已被以 1008 主動踢除，房間僅剩快客戶端
    assert slow_closed_codes, "Slow client should have been actively closed"
    assert slow_closed_codes[0][0] == WS_CLOSE_SLOW_CLIENT
    assert hub.active_client_count(99) == 1

    # 快客戶端完整收到全部 6 則訊息，毫無阻塞
    assert [json.loads(m)["seq"] for m in fast_received] == [1, 2, 3, 4, 5, 6]

    slow_block.set()
    await hub.shutdown()


@pytest.mark.asyncio
async def test_redis_listener_auto_recovers_and_connect_waits_for_subscription_ready():
    """
    驗證：
    1. `connect()` 必須等到 `pubsub.subscribe()` 完成後才返回（杜絕訂閱就緒前漏事件競態）。
    2. Redis 監聽器發生異常中斷後會自動重建訂閱、通知客戶端 `resync_required`，並繼續收到後續推播；
       若 `_listener_tasks` 內殘留已結束 task，後續連線亦會自動重建。
    """
    subscribe_started = asyncio.Event()
    allow_subscribe = asyncio.Event()
    received_messages = []
    attempt_counter = {"count": 0}

    class FlakyPubSub:
        def __init__(self, attempt: int):
            self.attempt = attempt
            self.step = 0

        async def subscribe(self, channel: str):
            subscribe_started.set()
            await allow_subscribe.wait()

        async def get_message(self, ignore_subscribe_messages=True, timeout=1.0):
            self.step += 1
            if self.attempt == 1:
                if self.step == 1:
                    return {"type": "message", "data": json.dumps({"seq": 1})}
                raise ConnectionError("Simulated Redis disconnect")
            else:
                if self.step == 1:
                    return {"type": "message", "data": json.dumps({"seq": 2})}
                await asyncio.sleep(0.05)
                return None

        async def unsubscribe(self, channel: str):
            pass

        async def aclose(self):
            pass

    class FlakyRedis:
        def pubsub(self):
            attempt_counter["count"] += 1
            return FlakyPubSub(attempt_counter["count"])

    class FakeWS:
        async def send_text(self, data: str):
            received_messages.append(json.loads(data))

        async def close(self, code: int = 1000, reason: str = ""):
            pass

    hub = EventBroadcastHub(
        redis_factory=FlakyRedis,
        retry_base_delay=0.05,
        retry_max_delay=0.1,
    )

    # 1. 驗證 connect() 會等待 subscribe() 完成才返回
    connect_task = asyncio.create_task(
        hub.connect(event_id=77, websocket=FakeWS())  # type: ignore[arg-type]
    )
    await asyncio.wait_for(subscribe_started.wait(), timeout=2.0)
    await asyncio.sleep(0.05)
    assert not connect_task.done(), "connect() must wait for pubsub.subscribe() readiness"

    allow_subscribe.set()
    await asyncio.wait_for(connect_task, timeout=2.0)

    # 2. 等待收到 seq=1 -> 模擬 Redis 斷線 -> 自動重連推送 resync_required -> 繼續收到 seq=2
    deadline = asyncio.get_running_loop().time() + 3.0
    while len(received_messages) < 3 and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.02)

    assert attempt_counter["count"] >= 2
    assert received_messages[0] == {"seq": 1}
    assert received_messages[1]["type"] == "resync_required"
    assert received_messages[1]["reason"] == "pubsub_reconnected"
    assert received_messages[2] == {"seq": 2}

    await hub.shutdown()

    # 3. 驗證訂閱等待超時會快速失敗（拋出 TimeoutError 並清理房間，絕不誤送 connected）
    never_ready = asyncio.Event()

    class HangingPubSub:
        async def subscribe(self, channel: str):
            await never_ready.wait()

        async def unsubscribe(self, channel: str):
            pass

        async def aclose(self):
            pass

    class HangingRedis:
        def pubsub(self):
            return HangingPubSub()

    timeout_hub = EventBroadcastHub(redis_factory=HangingRedis)
    with pytest.raises(TimeoutError):
        await timeout_hub.connect(
            event_id=88,
            websocket=FakeWS(),  # type: ignore[arg-type]
            subscribe_timeout=0.1,
        )
    assert timeout_hub.active_client_count(88) == 0
    await timeout_hub.shutdown()


@pytest.mark.asyncio
async def test_cross_replica_broadcast_across_two_independent_hubs(db_session):
    """
    驗證跨 API 副本廣播：
    建立兩個完全獨立的 EventBroadcastHub（模擬 api-1 與 api-2），
    單次 Outbox Relay 發布至 Redis Pub/Sub 後，兩台副本上的客戶端必須同時收到同一筆推播事件。
    """
    seed_users()
    event, _ = await _create_published_event_with_seats(db_session, seat_count=1)

    hub_api_1 = EventBroadcastHub()
    hub_api_2 = EventBroadcastHub()
    recv_1 = []
    recv_2 = []

    class CaptureWS:
        def __init__(self, target):
            self.target = target

        async def send_text(self, data: str):
            self.target.append(json.loads(data))

        async def close(self, code: int = 1000, reason: str = ""):
            pass

    try:
        await hub_api_1.connect(event.id, CaptureWS(recv_1))  # type: ignore[arg-type]
        await hub_api_2.connect(event.id, CaptureWS(recv_2))  # type: ignore[arg-type]

        db_session.add(
            create_outbox_event(
                event_id=event.id,
                sequence_id=1,
                event_type="seat_sold",
                payload={"order_id": 999, "request_id": "req-cross-replica-001"},
            )
        )
        await db_session.commit()

        relayed = await poll_and_relay_outbox(db_session)
        assert relayed == 1

        deadline = asyncio.get_running_loop().time() + 3.0
        while (not recv_1 or not recv_2) and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.02)

        assert len(recv_1) == 1 and len(recv_2) == 1
        assert recv_1[0]["sequence_id"] == 1
        assert recv_2[0]["sequence_id"] == 1
        assert recv_1[0]["event_type"] == "seat_sold"
        assert recv_2[0]["event_type"] == "seat_sold"
    finally:
        await hub_api_1.shutdown()
        await hub_api_2.shutdown()


@pytest.mark.asyncio
async def test_bootstrap_live_buffering_preserves_order_and_deduplicates_with_catchup():
    """
    驗證訂閱生效後、快照／補發尚未送完期間湧入即時事件的嚴格順序與去重，
    並確認 WS_MAX_CATCHUP_EVENTS 嚴格小於 WS_CLIENT_MAX_QUEUE_SIZE（不因補發撐爆佇列）：
    1. 客戶端以 `buffer_live_until_ready=True` 加入房間。
    2. 在 `connected` 與 `catchup(seq=2)` 寫入前，即時廣播先湧入 `seq=2`（重複）與 `seq=4`、`seq=3`（亂序）。
    3. 依序寫入 `connected(last_seq=2)` -> `catchup(seq=2)` 後呼叫 `flush_live_buffer(min_sequence_exclusive=2)`。
    4. 客戶端實際收到的順序必須嚴格為：connected -> catchup(2) -> live(3) -> live(4)，且 seq=2 不重複。
    """
    from app.realtime.hub import WS_CLIENT_MAX_QUEUE_SIZE
    from app.realtime.router import WS_MAX_CATCHUP_EVENTS

    assert WS_MAX_CATCHUP_EVENTS < WS_CLIENT_MAX_QUEUE_SIZE - 1

    hub = EventBroadcastHub()
    received = []

    class CaptureWS:
        async def send_text(self, data: str):
            received.append(json.loads(data))

        async def close(self, code: int = 1000, reason: str = ""):
            pass

    try:
        client = await hub.connect(
            event_id=55,
            websocket=CaptureWS(),  # type: ignore[arg-type]
            start_redis_listener=False,
            buffer_live_until_ready=True,
        )

        # 模擬訂閱剛生效、DB 快照與補發查詢期間，即時廣播先抵達（含已包含的 seq=2 與亂序 seq=4, 3）
        await hub.broadcast_raw(55, json.dumps({"event_type": "seat_held", "sequence_id": 2}))
        await hub.broadcast_raw(55, json.dumps({"event_type": "seat_held", "sequence_id": 4}))
        await hub.broadcast_raw(55, json.dumps({"event_type": "seat_held", "sequence_id": 3}))

        # 尚未 flush 前，任何即時事件皆不得搶先送出
        await asyncio.sleep(0.03)
        assert received == []

        # 寫入握手幀與補發幀，再原子沖刷暫存即時事件
        assert client.try_enqueue(json.dumps({"type": "connected", "last_seq": 2}))
        assert client.try_enqueue(json.dumps({"type": "catchup", "sequence_id": 2}))
        assert client.flush_live_buffer(min_sequence_exclusive=2)

        # 後續再抵達的即時事件直接走串流路徑
        await hub.broadcast_raw(55, json.dumps({"event_type": "seat_sold", "sequence_id": 5}))

        deadline = asyncio.get_running_loop().time() + 2.0
        while len(received) < 5 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.02)

        assert [
            (m.get("type", "live"), m.get("sequence_id", m.get("last_seq")))
            for m in received
        ] == [
            ("connected", 2),
            ("catchup", 2),
            ("live", 3),
            ("live", 4),
            ("live", 5),
        ]
    finally:
        await hub.shutdown()
