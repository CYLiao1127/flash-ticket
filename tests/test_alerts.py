"""即時異常告警引擎與 SSE 推播通道測試 (Commit 19 / Q6.2)

涵蓋範圍：
1. 系統面告警：5xx 錯誤率 > 5%、p99 延遲 > 2000ms、Outbox 待發布堆積 > 1000 筆。
2. 業務面告警 (Q6.2)：HTTP 5xx 為 0% 時，購票失敗率 > 90% 仍能準確觸發並拆解衝突／售罄／超限原因。
3. 原子冷卻去重：冷卻視窗內不重複轟炸相同規則。
4. 即時推播通道：Redis Pub/Sub `admin:alerts` 與 SSE `GET /api/v1/admin/monitoring/alerts/stream` 即時送達。
5. 權限矩陣：User 存取告警 API 回 403；Viewer 可查詢與訂閱但呼叫手動評估回 403；Admin 具備完整權限。
"""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import time

import pytest
from sqlalchemy import select

from app.auth.seed import SEED_ACCOUNTS, seed_users
from app.core.redis import get_redis
from app.models import Event, EventOutbox, EventStatus, OutboxStatus, SystemMetric, User
from app.monitoring.alerts import (
    ALERT_CHANNEL,
    ALERT_COOLDOWN_PREFIX,
    ALERT_HISTORY_KEY,
    AlertThresholds,
    evaluate_and_dispatch_alerts,
    get_recent_alerts,
)
from app.tickets.metrics import (
    TICKET_SEC_BUCKET_PREFIX,
    record_ticket_metric,
    reset_local_ticket_metrics,
)


def _login_seed(client, role: str) -> str:
    acc = next(a for a in SEED_ACCOUNTS if a["role"].value == role)
    res = client.post(
        "/api/v1/auth/login",
        json={"email": acc["email"], "password": acc["password"]},
    )
    assert res.status_code == 200, res.text
    return res.json()["access_token"]


async def _clean_alert_redis(ts_range=()) -> None:
    redis = get_redis()
    rules = (
        "high_5xx_error_rate",
        "high_p99_latency",
        "outbox_backlog_high",
        "high_booking_failure_rate",
    )
    keys = [ALERT_HISTORY_KEY] + [f"{ALERT_COOLDOWN_PREFIX}{r}" for r in rules]
    for sec in ts_range:
        keys.append(f"{TICKET_SEC_BUCKET_PREFIX}{sec}")
    await redis.delete(*keys)


@pytest.mark.asyncio
async def test_all_four_alert_rules_trigger_and_cooldown_deduplication(db_session):
    """
    驗證四大告警規則（5xx > 5%、p99 > 2s、Outbox > 1000、購票失敗率 > 90%）同時觸發、
    透過 Redis Pub/Sub 即時送達，且冷卻期內不重複派送。
    """
    seed_users()
    reset_local_ticket_metrics()
    now_epoch = int(time.time())
    now_dt = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
    await _clean_alert_redis(ts_range=range(now_epoch - 65, now_epoch + 5))

    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    # 建立一筆活動供 Outbox 外鍵關聯
    event = Event(
        creator_id=admin_user.id,
        title="Alert Storm Concert",
        category="concert",
        venue="Taipei Arena",
        start_time=now_dt - timedelta(hours=1),
        end_time=now_dt + timedelta(hours=5),
        status=EventStatus.PUBLISHED.value,
    )
    db_session.add(event)
    await db_session.flush()

    # 1 & 2. 寫入 5xx > 5% (8/100 = 8%) 且 p99 > 2000ms (2450ms) 的秒級指標
    db_session.add(
        SystemMetric(
            timestamp=now_dt - timedelta(seconds=5),
            total_requests=100,
            error_requests=8,
            p50_latency_ms=Decimal("45.00"),
            p95_latency_ms=Decimal("820.00"),
            p99_latency_ms=Decimal("2450.00"),
            active_connections=120,
        )
    )

    # 3. 寫入 1005 筆 pending Outbox 模擬堆積超過 1000 筆門檻
    db_session.add_all(
        [
            EventOutbox(
                event_id=event.id,
                sequence_id=idx + 1,
                event_type="seat_held",
                payload={"seat_id": idx + 1},
                status=OutboxStatus.PENDING.value,
            )
            for idx in range(1005)
        ]
    )
    await db_session.commit()

    # 4. 寫入業務層購票失敗率 > 90%（1 成功、19 失敗 = 95% 失敗率）
    await record_ticket_metric("reservation_success", event_id=event.id, delta=1, timestamp_sec=now_epoch - 2)
    await record_ticket_metric("seat_conflict", event_id=event.id, delta=15, timestamp_sec=now_epoch - 2)
    await record_ticket_metric("zone_sold_out", event_id=event.id, delta=2, timestamp_sec=now_epoch - 1)
    await record_ticket_metric("limit_exceeded", event_id=event.id, delta=2, timestamp_sec=now_epoch - 1)

    # 訂閱即時告警 Pub/Sub 頻道
    redis = get_redis()
    pubsub = redis.pubsub()
    await pubsub.subscribe(ALERT_CHANNEL)
    try:
        dispatched = await evaluate_and_dispatch_alerts(
            db_session,
            redis=redis,
            thresholds=AlertThresholds(cooldown_seconds=60),
            now_epoch=now_epoch,
        )
        rules_fired = {a["rule"] for a in dispatched}
        assert rules_fired == {
            "high_5xx_error_rate",
            "high_p99_latency",
            "outbox_backlog_high",
            "high_booking_failure_rate",
        }

        # 確認 Pub/Sub 通道即時收到全部 4 筆告警
        received_rules = set()
        deadline = time.monotonic() + 3.0
        while len(received_rules) < 4 and time.monotonic() < deadline:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
            if msg and msg.get("type") == "message":
                payload = json.loads(msg["data"])
                received_rules.add(payload["rule"])
        assert received_rules == rules_fired

        # 驗證冷卻去重：同一秒立即再次評估，4 條規則均在冷卻中，不應重複發送
        second_run = await evaluate_and_dispatch_alerts(
            db_session,
            redis=redis,
            thresholds=AlertThresholds(cooldown_seconds=60),
            now_epoch=now_epoch,
        )
        assert second_run == []

        # 驗證歷史查詢紀錄保留全部 4 筆
        history = await get_recent_alerts(limit=10, redis=redis)
        assert len(history) == 4
    finally:
        await pubsub.unsubscribe(ALERT_CHANNEL)
        await pubsub.aclose()
        await _clean_alert_redis(ts_range=range(now_epoch - 65, now_epoch + 5))


@pytest.mark.asyncio
async def test_business_booking_failure_alert_fires_even_with_zero_5xx_q62(db_session):
    """
    ★ 關鍵 Q6.2 驗證：
    即使系統層 HTTP 5xx 錯誤率為 0%（全數回傳 201 / 409）且延遲正常（p99 = 35ms），
    當業務層購票失敗率突破 90% 時，仍必須立即觸發 `high_booking_failure_rate` 告警並拆解失敗原因。
    """
    reset_local_ticket_metrics()
    now_epoch = int(time.time())
    now_dt = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
    await _clean_alert_redis(ts_range=range(now_epoch - 65, now_epoch + 5))

    # 系統面一片綠燈：0 筆 5xx、p99 僅 35ms
    db_session.add(
        SystemMetric(
            timestamp=now_dt - timedelta(seconds=2),
            total_requests=500,
            error_requests=0,
            p50_latency_ms=Decimal("12.00"),
            p95_latency_ms=Decimal("28.00"),
            p99_latency_ms=Decimal("35.00"),
            active_connections=50,
        )
    )
    await db_session.commit()

    # 業務面：2 筆成功、38 筆失敗（失敗率 95% > 90%）
    await record_ticket_metric("reservation_success", delta=2, timestamp_sec=now_epoch - 3)
    await record_ticket_metric("seat_conflict", delta=30, timestamp_sec=now_epoch - 2)
    await record_ticket_metric("limit_exceeded", delta=8, timestamp_sec=now_epoch - 1)

    dispatched = await evaluate_and_dispatch_alerts(
        db_session,
        thresholds=AlertThresholds(cooldown_seconds=60),
        now_epoch=now_epoch,
    )
    assert len(dispatched) == 1
    alert = dispatched[0]
    assert alert["rule"] == "high_booking_failure_rate"
    assert alert["metric_value"] == 0.95
    assert alert["details"]["seat_conflict"] == 30
    assert alert["details"]["limit_exceeded"] == 8
    assert alert["details"]["reservation_success"] == 2

    await _clean_alert_redis(ts_range=range(now_epoch - 65, now_epoch + 5))


@pytest.mark.asyncio
async def test_admin_alerts_rest_api_rbac_and_sse_stream_delivery(client, db_session):
    """
    驗證告警 REST API、RBAC 權限控管與 SSE 即時推播串流：
    1. User 呼叫告警端點回 403；Viewer 可查詢/訂閱但不可手動 evaluate (403)。
    2. Admin 呼叫 POST /alerts/evaluate 觸發告警，並可由 GET /alerts 與 GET /alerts/stream 取得。
    """
    seed_users()
    admin_token = _login_seed(client, "admin")
    viewer_token = _login_seed(client, "viewer")
    user_token = _login_seed(client, "user")

    now_epoch = int(time.time())
    now_dt = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
    await _clean_alert_redis(ts_range=range(now_epoch - 65, now_epoch + 5))

    # 製造高 p99 延遲指標
    db_session.add(
        SystemMetric(
            timestamp=now_dt - timedelta(seconds=3),
            total_requests=30,
            error_requests=0,
            p50_latency_ms=Decimal("100.00"),
            p95_latency_ms=Decimal("1500.00"),
            p99_latency_ms=Decimal("3200.00"),
            active_connections=15,
        )
    )
    await db_session.commit()

    # 1. User 被 403 拒絕
    assert (
        client.get(
            "/api/v1/admin/monitoring/alerts",
            headers={"Authorization": f"Bearer {user_token}"},
        ).status_code
        == 403
    )
    # 2. Viewer 呼叫手動評估被 403 拒絕
    assert (
        client.post(
            "/api/v1/admin/monitoring/alerts/evaluate",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ).status_code
        == 403
    )

    # 3. Admin 觸發評估 -> 產生 high_p99_latency 告警
    eval_res = client.post(
        "/api/v1/admin/monitoring/alerts/evaluate",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert eval_res.status_code == 200, eval_res.text
    eval_body = eval_res.json()
    assert eval_body["dispatched_count"] == 1
    assert eval_body["dispatched"][0]["rule"] == "high_p99_latency"

    # 4. Viewer 查詢歷史告警與 SSE 初始快照
    list_res = client.get(
        "/api/v1/admin/monitoring/alerts?rule=high_p99_latency",
        headers={"Authorization": f"Bearer {viewer_token}"},
    )
    assert list_res.status_code == 200, list_res.text
    list_body = list_res.json()
    assert list_body["total"] == 1
    assert list_body["thresholds"]["p99_latency_ms_threshold"] == 2000.0

    with client.stream(
        "GET",
        "/api/v1/admin/monitoring/alerts/stream?max_events=0",
        headers={"Authorization": f"Bearer {viewer_token}"},
    ) as stream_res:
        assert stream_res.status_code == 200
        assert stream_res.headers.get("x-accel-buffering") == "no"
        body = "".join(stream_res.iter_text())
        assert "event: connected" in body
        assert "high_p99_latency" in body

    await _clean_alert_redis(ts_range=range(now_epoch - 65, now_epoch + 5))


@pytest.mark.asyncio
async def test_worker_cycle_triggers_alert_and_connected_sse_client_receives_live_event(
    client, db_session
):
    """
    端對端驗證：
    1. SSE 客戶端先連線至 GET /api/v1/admin/monitoring/alerts/stream 並收到 `event: connected`。
    2. 背景 Worker 執行 `run_worker_cycle(db_session)`，自動呼叫異常告警評估並觸發 `high_p99_latency`。
    3. 已連線的 SSE 客戶端即時收到 `event: alert` 推送事件（而非初始快照）。
    """
    from app.main import app
    from app.monitoring.collector import (
        DIRTY_SECONDS_KEY,
        INFLIGHT_CLAIMS_ZSET_KEY,
        record_http_observation,
    )
    from app.workers.runner import run_worker_cycle

    seed_users()
    admin_token = _login_seed(client, "admin")

    now_epoch = int(time.time())
    bucket_ts = now_epoch - 2
    redis = get_redis()
    # 清理前序測試殘留的監控秒桶與告警狀態，改走真實 record_http_observation -> Worker 全鏈路
    dirty = await redis.smembers(DIRTY_SECONDS_KEY)
    cleanup_keys = [DIRTY_SECONDS_KEY, INFLIGHT_CLAIMS_ZSET_KEY]
    for sec in set(str(s) for s in dirty) | {str(bucket_ts)}:
        cleanup_keys.extend(
            [
                f"metrics:sec:{sec}:counts",
                f"metrics:sec:{sec}:latencies",
                f"metrics:sec:{sec}:merged_latencies",
                f"metrics:sec:{sec}:merged_claims",
            ]
        )
    await redis.delete(*cleanup_keys)
    await _clean_alert_redis(ts_range=range(now_epoch - 65, now_epoch + 5))

    # 透過真實收集管線寫入一筆高延遲請求（2800ms），交由 run_worker_cycle 自動入庫並觸發告警
    await record_http_observation(
        request_id="worker-sse-live-001",
        user_id=None,
        method="POST",
        path="/api/v1/events/1/tickets/reserve",
        status_code=201,
        latency_ms=2800.0,
        client_ip="127.0.0.1",
        timestamp_sec=bucket_ts,
    )

    connected_evt = asyncio.Event()
    received_live_alerts = []
    status_holder = []
    disconnect_evt = asyncio.Event()

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/v1/admin/monitoring/alerts/stream",
        "raw_path": b"/api/v1/admin/monitoring/alerts/stream",
        "query_string": b"max_events=1&timeout_seconds=5",
        "headers": [(b"authorization", f"Bearer {admin_token}".encode())],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }

    async def receive():
        await disconnect_evt.wait()
        return {"type": "http.disconnect"}

    sse_buffer = ""

    async def send(message):
        nonlocal sse_buffer
        if message["type"] == "http.response.start":
            status_holder.append(message["status"])
        elif message["type"] == "http.response.body":
            chunk = message.get("body", b"").decode("utf-8")
            if not chunk:
                return
            sse_buffer += chunk
            while "\n\n" in sse_buffer:
                frame, sse_buffer = sse_buffer.split("\n\n", 1)
                ev_name = None
                data_str = None
                for line in frame.splitlines():
                    if line.startswith("event:"):
                        ev_name = line.split(":", 1)[1].strip()
                    elif line.startswith("data:"):
                        data_str = line.split(":", 1)[1].strip()
                if ev_name == "connected" and data_str:
                    payload = json.loads(data_str)
                    assert payload["recent_alerts"] == []
                    connected_evt.set()
                elif ev_name == "alert" and data_str:
                    received_live_alerts.append(json.loads(data_str))

    sse_task = asyncio.create_task(app(scope, receive, send))
    try:
        # 1. 等待 SSE 客戶端完成訂閱並即時收到初始 connected 幀
        await asyncio.wait_for(connected_evt.wait(), timeout=3.0)
        assert status_holder == [200]

        # 2. 執行背景 Worker 週期函式，驗證其自動執行告警評估與派送
        summary = await run_worker_cycle(db_session)
        assert summary["alerts_dispatched"] == 1

        # 3. 等待已連線的 SSE 客戶端即時收到該筆 event: alert
        await asyncio.wait_for(sse_task, timeout=5.0)
        assert len(received_live_alerts) == 1
        assert received_live_alerts[0]["rule"] == "high_p99_latency"
        assert received_live_alerts[0]["metric_value"] == 2800.0
    finally:
        disconnect_evt.set()
        if not sse_task.done():
            sse_task.cancel()
            try:
                await sse_task
            except asyncio.CancelledError:
                pass
        await _clean_alert_redis(ts_range=range(now_epoch - 65, now_epoch + 5))
