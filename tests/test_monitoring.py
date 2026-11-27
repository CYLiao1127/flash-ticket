"""每秒系統指標、存取日誌批次寫入、Admin 全鏈路追蹤與 20s 雙副本時鐘偏差分析測試 (Commit 18 / Q6.3)"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import pytest
from sqlalchemy import select

from app.auth.seed import SEED_ACCOUNTS, seed_users
from app.core.redis import get_redis
from app.models import (
    Event,
    EventStatus,
    Seat,
    SeatStatus,
    SystemMetric,
    User,
    Zone,
)
from app.monitoring.collector import (
    ACCESS_LOG_QUEUE_KEY,
    DIRTY_SECONDS_KEY,
    compute_percentile,
    flush_access_logs_batch,
    flush_per_second_metrics,
    record_http_observation,
)


def _login(client, email: str, password: str) -> str:
    res = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert res.status_code == 200, res.text
    return res.json()["access_token"]


def _login_seed(client, role: str) -> str:
    acc = next(a for a in SEED_ACCOUNTS if a["role"].value == role)
    return _login(client, acc["email"], acc["password"])


async def _clean_monitoring_redis(extra_seconds=()) -> None:
    from app.monitoring.collector import INFLIGHT_CLAIMS_ZSET_KEY

    redis = get_redis()
    dirty = await redis.smembers(DIRTY_SECONDS_KEY)
    all_secs = set(str(s) for s in dirty) | {str(s) for s in extra_seconds}
    keys = [ACCESS_LOG_QUEUE_KEY, DIRTY_SECONDS_KEY, INFLIGHT_CLAIMS_ZSET_KEY]
    for sec in all_secs:
        keys.append(f"metrics:sec:{sec}:counts")
        keys.append(f"metrics:sec:{sec}:latencies")
        keys.append(f"metrics:sec:{sec}:merged_latencies")
        keys.append(f"metrics:sec:{sec}:merged_claims")
    if keys:
        await redis.delete(*keys)


def test_compute_percentile_accuracy():
    """驗證 p50 / p95 / p99 百分位計算正確性"""
    samples = [float(i) for i in range(1, 101)]  # 1.0 .. 100.0
    assert compute_percentile(samples, 50) == Decimal("50.00")
    assert compute_percentile(samples, 95) == Decimal("95.00")
    assert compute_percentile(samples, 99) == Decimal("99.00")
    assert compute_percentile([], 99) == Decimal("0.00")


@pytest.mark.asyncio
async def test_access_log_batching_and_admin_request_id_trace(client, db_session):
    """
    驗證 6.6 全鏈路追蹤與批次存取日誌：
    1. 帶有自訂 X-Request-ID 的搶票請求經 Middleware 進入 Redis 佇列，由 Worker 批次寫入 access_logs（含認證 user_id）。
    2. Admin / Viewer 呼叫 GET /api/v1/admin/monitoring/trace/{request_id} 可同時串聯 HTTP AccessLog 與 Outbox 領域事件。
    3. 一般 User 呼叫監控與追蹤端點必被 403 拒絕。
    """
    await _clean_monitoring_redis()
    seed_users()
    admin_token = _login_seed(client, "admin")
    user_token = _login_seed(client, "user")
    viewer_token = _login_seed(client, "viewer")

    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()
    normal_user = (
        await db_session.execute(select(User).where(User.email == "user@flashticket.com"))
    ).scalar_one()

    now = datetime.now(timezone.utc)
    event = Event(
        creator_id=admin_user.id,
        title="Monitoring Trace Concert",
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
    seat = Seat(
        event_id=event.id,
        zone_code="A",
        row_label="1",
        seat_number="1",
        status=SeatStatus.AVAILABLE.value,
    )
    db_session.add(seat)
    await db_session.commit()
    await db_session.refresh(seat)

    custom_trace_id = "trace-c18-end-to-end-001"
    res = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={
            "Authorization": f"Bearer {user_token}",
            "X-Request-ID": custom_trace_id,
        },
        json={"seat_ids": [seat.id]},
    )
    assert res.status_code == 201, res.text
    assert res.headers.get("X-Request-ID") == custom_trace_id

    # 背景 Worker 批次刷入存取日誌
    flushed = await flush_access_logs_batch(db_session)
    assert flushed >= 1

    # 一般 User 不可查詢監控端點 (403)
    assert (
        client.get(
            f"/api/v1/admin/monitoring/trace/{custom_trace_id}",
            headers={"Authorization": f"Bearer {user_token}"},
        ).status_code
        == 403
    )

    # Admin 與 Viewer 可查到完整全鏈路（HTTP AccessLog + EventOutbox）
    trace_res = client.get(
        f"/api/v1/admin/monitoring/trace/{custom_trace_id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert trace_res.status_code == 200, trace_res.text
    trace_data = trace_res.json()
    assert trace_data["request_id"] == custom_trace_id
    assert len(trace_data["access_logs"]) == 1
    log_entry = trace_data["access_logs"][0]
    assert log_entry["user_id"] == normal_user.id
    assert log_entry["method"] == "POST"
    assert log_entry["path"] == f"/api/v1/events/{event.id}/tickets/reserve"
    assert log_entry["status_code"] == 201
    assert len(trace_data["outbox_events"]) == 1
    assert trace_data["outbox_events"][0]["event_type"] == "seat_held"
    assert trace_data["outbox_events"][0]["payload"]["request_id"] == custom_trace_id

    # 測試 access-logs 篩選 API（Viewer 亦可讀）
    logs_res = client.get(
        f"/api/v1/admin/monitoring/access-logs?request_id={custom_trace_id}",
        headers={"Authorization": f"Bearer {viewer_token}"},
    )
    assert logs_res.status_code == 200
    assert logs_res.json()["total"] == 1
    await _clean_monitoring_redis()


@pytest.mark.asyncio
async def test_per_second_metrics_aggregation_and_20s_replica_clock_skew_q63(
    client, db_session
):
    """
    ★ 關鍵 Q6.3 驗證：
    1. 驗證每秒指標 (RPS、5xx 錯誤率、p50/p95/p99) 批次寫入 `metrics_per_second` 並可由 Admin API 查詢。
    2. 模擬雙副本中 api-1 時鐘正常 (T)、api-2 時鐘慢 20 秒 (T - 20)：
       - 歷史秒桶 T-20 先有 5 筆已入庫；
       - 在真實時刻 T 同時湧入 20 筆請求（api-1 收到 10 筆寫入桶 T、api-2 收到 10 筆因慢 20s 寫入桶 T-20）；
       - 驗證 ON CONFLICT (timestamp) DO UPDATE 安全合併不報錯、總請求數零遺失 (5 + 10 + 10 = 25)，
         並重現「T 桶僅顯示一半即時流量、歷史桶 T-20 事後暴增」之時鐘偏差現象。
    """
    await _clean_monitoring_redis()
    seed_users()
    admin_token = _login_seed(client, "admin")

    base_t = 1_760_000_100  # 固定秒級基準時間戳
    past_t = base_t - 20
    await _clean_monitoring_redis(extra_seconds=[base_t, past_t])

    # 1. 歷史時刻 T - 20 先產生 5 筆正常流量（含 1 筆 500 錯誤）並刷入 DB
    for idx, lat in enumerate([10.0, 20.0, 30.0, 40.0, 250.0]):
        await record_http_observation(
            request_id=f"hist-{idx}",
            user_id=None,
            method="GET",
            path="/api/v1/events",
            status_code=500 if idx == 4 else 200,
            latency_ms=lat,
            client_ip="127.0.0.1",
            timestamp_sec=past_t,
        )
    flushed_hist = await flush_per_second_metrics(
        db_session, flush_before_epoch=past_t + 1
    )
    assert flushed_hist == 1

    # 2. 真實時刻 T 同時湧入 20 筆請求：api-1 寫入 T (10 筆)、慢 20s 的 api-2 寫入 T - 20 (10 筆)
    for idx in range(10):
        await record_http_observation(
            request_id=f"api1-{idx}",
            user_id=None,
            method="POST",
            path="/api/v1/events/1/tickets/allocate",
            status_code=500 if idx == 9 else 201,
            latency_ms=float((idx + 1) * 10),  # 10..100ms
            client_ip="10.0.0.1",
            timestamp_sec=base_t,
        )
        await record_http_observation(
            request_id=f"api2-skew-{idx}",
            user_id=None,
            method="POST",
            path="/api/v1/events/1/tickets/allocate",
            status_code=201,
            latency_ms=float((idx + 1) * 15),
            client_ip="10.0.0.2",
            timestamp_sec=past_t,  # 慢 20 秒！
        )

    flushed_skew = await flush_per_second_metrics(
        db_session, flush_before_epoch=base_t + 1
    )
    assert flushed_skew == 2

    # 3. 驗證 DB 兩個秒桶：T 只有 10 筆（即時流量減半），T-20 被合併為 5 + 10 = 15 筆（零遺失、不撞唯一鍵）
    rows = list(
        (
            await db_session.execute(
                select(SystemMetric).order_by(SystemMetric.timestamp.asc())
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 2
    bucket_past, bucket_now = rows[0], rows[1]
    assert bucket_past.total_requests == 15  # 5 筆歷史 + 10 筆 api-2 慢 20s 遲到流量
    assert bucket_past.error_requests == 1
    # 驗證遲到副本與歷史樣本合併重算完整百分位：
    # 15 筆合併排序 = [10, 15, 20, 30, 30, 40, 45, 60, 75, 90, 105, 120, 135, 150, 250]
    # p50 = 第 8 筆 (60.00)、p95 = 第 15 筆 (250.00)、p99 = 第 15 筆 (250.00，不被遲到最大 150 覆蓋！)
    assert Decimal(bucket_past.p50_latency_ms) == Decimal("60.00")
    assert Decimal(bucket_past.p95_latency_ms) == Decimal("250.00")
    assert Decimal(bucket_past.p99_latency_ms) == Decimal("250.00")

    assert bucket_now.total_requests == 10  # api-1 正常時鐘流量
    assert bucket_now.error_requests == 1
    assert Decimal(bucket_now.p50_latency_ms) == Decimal("50.00")
    assert Decimal(bucket_now.p95_latency_ms) == Decimal("100.00")
    assert Decimal(bucket_now.p99_latency_ms) == Decimal("100.00")

    # 4. 驗證 Admin 指標查詢 API 彙總結果
    res = client.get(
        "/api/v1/admin/monitoring/metrics?limit=10",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert res.status_code == 200, res.text
    summary = res.json()
    assert summary["total_requests"] == 25
    assert summary["error_requests"] == 2
    assert summary["error_rate"] == round(2 / 25, 6)
    assert len(summary["series"]) == 2
    await _clean_monitoring_redis(extra_seconds=[base_t, past_t])


@pytest.mark.asyncio
async def test_inflight_bucket_recovery_and_active_connection_ttl_expiry(db_session):
    """
    驗證：
    1. Worker 搬移秒桶至 inflight 後若於 DB commit 前異常中斷，下一輪 Worker 可自動回收該過期 inflight 桶，指標零遺失。
    2. Worker 若於 DB commit 成功後、清除 Redis inflight 鍵前遭強制終止，下一輪 recovery 透過 `metric_flush_claims`
       冪等去重，絕不重複累加 request/error，且會安全補完 Redis 清理與 merged_latencies。
    3. 各 API 副本活躍連線數具獨立 TTL，異常離線的副本鍵過期後自動從叢集總數排除並清理。
    4. `ActiveConnectionASGIMiddleware` 完整追蹤長連線 SSE (HTTP streaming) 與 WebSocket 連線生命週期。
    """
    import asyncio
    import time as time_mod
    from app.monitoring.collector import (
        ACTIVE_CONN_INSTANCES_SET_KEY,
        INFLIGHT_CLAIMS_ZSET_KEY,
        ActiveConnectionASGIMiddleware,
        _active_conn_node_key,
        _inflight_counts_key,
        _inflight_latencies_key,
        get_cluster_active_connections,
    )

    redis = get_redis()
    ts_pre_crash = 1_760_000_250
    ts_post_crash = 1_760_000_260
    await _clean_monitoring_redis(extra_seconds=[ts_pre_crash, ts_post_crash])

    # 1. 模擬 commit 前中斷：inflight 鍵存在且 40s 過期、DB 尚未寫入 -> recovery 必須補寫入
    pre_claim_id = "crashed_pre"
    pre_member = f"{ts_pre_crash}:{pre_claim_id}"
    await redis.hset(_inflight_counts_key(ts_pre_crash, pre_claim_id), mapping={"total": "4", "errors": "1"})
    await redis.rpush(_inflight_latencies_key(ts_pre_crash, pre_claim_id), "12.00", "25.00", "40.00", "95.00")
    await redis.zadd(INFLIGHT_CLAIMS_ZSET_KEY, {pre_member: int(time_mod.time()) - 40})

    recovered = await flush_per_second_metrics(
        db_session,
        flush_before_epoch=ts_pre_crash + 1,
        inflight_stale_seconds=30,
    )
    assert recovered == 1

    metric_row = (
        await db_session.execute(
            select(SystemMetric).where(
                SystemMetric.timestamp == datetime.fromtimestamp(ts_pre_crash, tz=timezone.utc)
            )
        )
    ).scalar_one()
    assert metric_row.total_requests == 4
    assert metric_row.error_requests == 1
    assert Decimal(metric_row.p99_latency_ms) == Decimal("95.00")
    assert await redis.zscore(INFLIGHT_CLAIMS_ZSET_KEY, pre_member) is None

    # 2. 模擬 DB commit 後、清除 Redis inflight 鍵前遭強制終止 -> recovery 絕不可重複計數
    for idx, lat in enumerate([15.0, 35.0, 120.0]):
        await record_http_observation(
            request_id=f"post-crash-{idx}",
            user_id=None,
            method="POST",
            path="/api/v1/events/1/tickets/reserve",
            status_code=500 if idx == 2 else 201,
            latency_ms=lat,
            client_ip="10.0.0.5",
            timestamp_sec=ts_post_crash,
        )

    class SimulatedPostCommitCrash(RuntimeError):
        pass

    async def crash_right_after_commit():
        raise SimulatedPostCommitCrash("SIGKILL between DB commit and Redis cleanup")

    with pytest.raises(SimulatedPostCommitCrash):
        await flush_per_second_metrics(
            db_session,
            flush_before_epoch=ts_post_crash + 1,
            on_after_commit=crash_right_after_commit,
        )

    # 此時 DB 已提交 (3 requests, 1 error)，但 Redis inflight 鍵仍遺留在 INFLIGHT_CLAIMS_ZSET_KEY
    leftover_claims = await redis.zrange(INFLIGHT_CLAIMS_ZSET_KEY, 0, -1)
    assert len(leftover_claims) == 1
    # 將該遺留 claim 時間往回調 40 秒模擬逾時進入下輪 recovery
    await redis.zadd(INFLIGHT_CLAIMS_ZSET_KEY, {leftover_claims[0]: int(time_mod.time()) - 40})

    dup_recovered = await flush_per_second_metrics(
        db_session,
        flush_before_epoch=ts_post_crash + 1,
        inflight_stale_seconds=30,
    )
    assert dup_recovered == 0  # 已提交過之 claim 不得重複計數
    assert await redis.zrange(INFLIGHT_CLAIMS_ZSET_KEY, 0, -1) == []

    post_row = (
        await db_session.execute(
            select(SystemMetric).where(
                SystemMetric.timestamp == datetime.fromtimestamp(ts_post_crash, tz=timezone.utc)
            )
        )
    ).scalar_one()
    assert post_row.total_requests == 3  # 維持 3，未變成 6
    assert post_row.error_requests == 1  # 維持 1，未變成 2
    assert Decimal(post_row.p99_latency_ms) == Decimal("120.00")

    # 驗證剛才 post-commit crash 的樣本也已正確補入 merged_latencies，後續遲到請求仍能保留 120.00 的 p99
    await record_http_observation(
        request_id="post-crash-late",
        user_id=None,
        method="GET",
        path="/api/v1/events",
        status_code=200,
        latency_ms=20.0,
        client_ip="10.0.0.6",
        timestamp_sec=ts_post_crash,
    )
    assert (
        await flush_per_second_metrics(db_session, flush_before_epoch=ts_post_crash + 1)
        == 1
    )
    await db_session.refresh(post_row)
    assert post_row.total_requests == 4
    assert Decimal(post_row.p99_latency_ms) == Decimal("120.00")

    # 3. 模擬雙副本連線數：live-node 有未過期 TTL 鍵 (=7)，dead-node 鍵已過期不復存在 -> 總數應只計入 7 並清理 dead-node
    await redis.delete(
        ACTIVE_CONN_INSTANCES_SET_KEY,
        _active_conn_node_key("live-node"),
        _active_conn_node_key("dead-node"),
    )
    await redis.sadd(ACTIVE_CONN_INSTANCES_SET_KEY, "live-node", "dead-node")
    await redis.set(_active_conn_node_key("live-node"), "7", ex=30)

    total_conns = await get_cluster_active_connections(redis=redis)
    assert total_conns == 7
    assert await redis.sismember(ACTIVE_CONN_INSTANCES_SET_KEY, "dead-node") == 0
    await redis.delete(ACTIVE_CONN_INSTANCES_SET_KEY, _active_conn_node_key("live-node"))

    # 4. 驗證 ActiveConnectionASGIMiddleware 同時涵蓋長連線 SSE (HTTP) 與 WebSocket 連線
    sse_opened = asyncio.Event()
    ws_opened = asyncio.Event()
    release_conns = asyncio.Event()

    async def dummy_asgi_app(scope, receive, send):
        if scope["type"] == "http":
            sse_opened.set()
        elif scope["type"] == "websocket":
            ws_opened.set()
        await release_conns.wait()

    wrapped = ActiveConnectionASGIMiddleware(dummy_asgi_app)
    sse_task = asyncio.create_task(wrapped({"type": "http"}, None, None))
    ws_task = asyncio.create_task(wrapped({"type": "websocket"}, None, None))
    await asyncio.wait_for(asyncio.gather(sse_opened.wait(), ws_opened.wait()), timeout=2.0)

    # 兩個長連線皆尚未關閉，叢集連線數必須同時計入 HTTP/SSE + WebSocket (>= 2)
    assert await get_cluster_active_connections(redis=redis) >= 2

    release_conns.set()
    await asyncio.gather(sse_task, ws_task)
    assert await get_cluster_active_connections(redis=redis) == 0

    # 5. 驗證 Redis 暫存 TTL（24h）與 metric_flush_claims 週期性保留清理策略（48h）
    from app.models import MetricFlushClaim
    from app.monitoring.collector import (
        METRIC_CLAIM_RETENTION_SECONDS,
        SEC_BUCKET_TTL_SECONDS,
        prune_old_metric_flush_claims,
    )

    assert SEC_BUCKET_TTL_SECONDS == 86400
    assert METRIC_CLAIM_RETENTION_SECONDS > SEC_BUCKET_TTL_SECONDS

    old_claim = MetricFlushClaim(
        claim_id="expired-claim-001",
        bucket_timestamp=datetime.now(timezone.utc) - timedelta(days=3),
        created_at=datetime.now(timezone.utc) - timedelta(hours=72),
    )
    fresh_claim = MetricFlushClaim(
        claim_id="fresh-claim-001",
        bucket_timestamp=datetime.now(timezone.utc),
        created_at=datetime.now(timezone.utc),
    )
    db_session.add_all([old_claim, fresh_claim])
    await db_session.commit()

    pruned = await prune_old_metric_flush_claims(db_session)
    assert pruned >= 1
    remaining_ids = set(
        (await db_session.execute(select(MetricFlushClaim.claim_id))).scalars().all()
    )
    assert "expired-claim-001" not in remaining_ids
    assert "fresh-claim-001" in remaining_ids

    await _clean_monitoring_redis(extra_seconds=[ts_pre_crash, ts_post_crash])
