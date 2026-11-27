"""每秒系統指標收集器與存取日誌批次入庫引擎 (Commit 18 / Q6.3)

設計重點：
1. 熱路徑非阻塞：HTTP Middleware 僅透過單次 Redis Pipeline 寫入秒級桶與日誌佇列，
   絕不在請求路徑上同步執行 PostgreSQL INSERT，避免監控拖垮搶票吞吐。
2. 背景 Worker 每秒批次將 `metrics:access_logs:queue` 與已完成之秒級指標桶 (`metrics:dirty_seconds`)
   批次寫入 PostgreSQL (`access_logs`, `metrics_per_second`)。
3. 端對端恰好一次（Exactly-Once）持久化：
   - Redis Lua 將活躍秒桶原子 RENAME 至 `inflight` 暫存鍵，不提前刪除，防止 DB commit 前崩潰遺失資料。
   - PostgreSQL 同一交易內先寫入 `metric_flush_claims(claim_id)`（PK 冪等鎖），再 Upsert `metrics_per_second`，
     防止 Worker 於 DB commit 後、清除 Redis 前被強殺導致下輪 recovery 重複計數。
4. 針對多副本時鐘偏差（例如 api-2 慢 20 秒，Q6.3），`metrics_per_second` 採用
   `ON CONFLICT (timestamp) DO UPDATE` 安全合併，並以 300s `merged_latencies` 窗口重算完整 p50/p95/p99。
5. 純 ASGI `ActiveConnectionASGIMiddleware` 同時涵蓋一般 HTTP、長連線 SSE 與 WebSocket 完整連線生命週期，
   並以節點級 TTL 鍵彙總叢集即時在線連線數。
"""

import json
import logging
import math
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Awaitable, Callable, List, Optional

from sqlalchemy import delete, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.redis import get_redis
from app.models import AccessLog, MetricFlushClaim, SystemMetric

logger = logging.getLogger("flash_ticket.monitoring")

ACCESS_LOG_QUEUE_KEY = "metrics:access_logs:queue"
DIRTY_SECONDS_KEY = "metrics:dirty_seconds"
INFLIGHT_CLAIMS_ZSET_KEY = "metrics:inflight_claims"
ACTIVE_CONN_INSTANCES_SET_KEY = "metrics:active_conn:instances"
ACTIVE_CONN_KEY_PREFIX = "metrics:active_conn:node:"
ACTIVE_CONN_TTL_SECONDS = 30  # 單一副本連線數心跳 TTL，副本異常終止 30s 後自動退出彙總
SEC_BUCKET_TTL_SECONDS = 86400  # 未入庫秒桶與 inflight 保留 24 小時，容忍 DB / Worker 長時間中斷
MERGED_LATENCIES_TTL_SECONDS = 3600  # 已入庫秒桶之延遲合併窗口（供遲到副本重算百分位）
METRIC_CLAIM_RETENTION_SECONDS = 172800  # DB 冪等紀錄保留 48 小時（嚴格大於 Redis 24h TTL）
METRIC_CLAIM_PRUNE_INTERVAL_SECONDS = 300  # 每 5 分鐘背景清理一次過期 claim 紀錄
INFLIGHT_STALE_SECONDS = 30

_active_http_connections = 0
_active_ws_connections = 0
_last_claim_prune_epoch = 0.0


def _sec_counts_key(ts_sec: int) -> str:
    return f"metrics:sec:{ts_sec}:counts"


def _sec_latencies_key(ts_sec: int) -> str:
    return f"metrics:sec:{ts_sec}:latencies"


def _sec_merged_latencies_key(ts_sec: int) -> str:
    return f"metrics:sec:{ts_sec}:merged_latencies"


def _sec_merged_claims_key(ts_sec: int) -> str:
    return f"metrics:sec:{ts_sec}:merged_claims"


def _inflight_counts_key(ts_sec: int, claim_id: str) -> str:
    return f"metrics:sec:{ts_sec}:inflight:{claim_id}:counts"


def _inflight_latencies_key(ts_sec: int, claim_id: str) -> str:
    return f"metrics:sec:{ts_sec}:inflight:{claim_id}:latencies"


def _active_conn_node_key(instance_id: str) -> str:
    return f"{ACTIVE_CONN_KEY_PREFIX}{instance_id}"


# 原子將活躍秒桶搬移至 inflight 暫存鍵（不提前刪除！待 DB commit 成功後才清除，防 Worker kill -9 遺失）
_STAGE_SECOND_BUCKET_LUA = """
local dirty_key = KEYS[1]
local counts_key = KEYS[2]
local lat_key = KEYS[3]
local inflight_zset = KEYS[4]
local inflight_counts = KEYS[5]
local inflight_lat = KEYS[6]
local ts_member = ARGV[1]
local claim_member = ARGV[2]
local now_epoch = tonumber(ARGV[3])
local ttl_sec = tonumber(ARGV[4])

local has_counts = redis.call('EXISTS', counts_key)
local has_lat = redis.call('EXISTS', lat_key)
redis.call('SREM', dirty_key, ts_member)

if has_counts == 0 and has_lat == 0 then
    return {'0', '0', {}}
end

if has_counts == 1 then
    redis.call('RENAME', counts_key, inflight_counts)
    redis.call('EXPIRE', inflight_counts, ttl_sec)
end
if has_lat == 1 then
    redis.call('RENAME', lat_key, inflight_lat)
    redis.call('EXPIRE', inflight_lat, ttl_sec)
end

redis.call('ZADD', inflight_zset, now_epoch, claim_member)

local total = redis.call('HGET', inflight_counts, 'total') or '0'
local errors = redis.call('HGET', inflight_counts, 'errors') or '0'
local latencies = redis.call('LRANGE', inflight_lat, 0, -1)
return {total, errors, latencies}
"""

# 原子清理 inflight 暫存鍵並以 claim_id 去重追加至 merged_latencies（確保 post-commit 崩潰回復不重複追加）
_FINALIZE_INFLIGHT_LUA = """
local inflight_zset = KEYS[1]
local inflight_counts = KEYS[2]
local inflight_lat = KEYS[3]
local merged_lat_key = KEYS[4]
local merged_claims_key = KEYS[5]
local claim_member = ARGV[1]
local claim_id = ARGV[2]
local ttl_sec = tonumber(ARGV[3])

local added = redis.call('SADD', merged_claims_key, claim_id)
if added == 1 then
    local lats = redis.call('LRANGE', inflight_lat, 0, -1)
    if #lats > 0 then
        redis.call('RPUSH', merged_lat_key, unpack(lats))
        redis.call('EXPIRE', merged_lat_key, ttl_sec)
    end
    redis.call('EXPIRE', merged_claims_key, ttl_sec)
end

redis.call('ZREM', inflight_zset, claim_member)
redis.call('DEL', inflight_counts, inflight_lat)
return added
"""


def compute_percentile(latencies: List[float], percentile: float) -> Decimal:
    """以升冪排序計算指定百分位數 (p50/p95/p99)，回傳兩位小數 Decimal"""
    if not latencies:
        return Decimal("0.00")
    ordered = sorted(latencies)
    rank = max(1, math.ceil((percentile / 100.0) * len(ordered)))
    val = ordered[min(rank - 1, len(ordered) - 1)]
    return Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


async def increment_active_connections(
    kind: str = "http",
    delta: int = 1,
    instance_id: Optional[str] = None,
    ttl_seconds: int = ACTIVE_CONN_TTL_SECONDS,
    redis=None,
) -> int:
    """追蹤本副本即時在線連線數 (HTTP/SSE + WebSocket)，並以帶 TTL 之獨立節點鍵同步至 Redis"""
    global _active_http_connections, _active_ws_connections
    if kind == "ws":
        _active_ws_connections = max(0, _active_ws_connections + delta)
    else:
        _active_http_connections = max(0, _active_http_connections + delta)

    total_local = _active_http_connections + _active_ws_connections
    node_id = instance_id or settings.INSTANCE_ID
    try:
        client = redis or get_redis()
        pipe = client.pipeline(transaction=False)
        pipe.sadd(ACTIVE_CONN_INSTANCES_SET_KEY, node_id)
        pipe.set(_active_conn_node_key(node_id), str(total_local), ex=ttl_seconds)
        await pipe.execute()
    except Exception:
        pass
    return total_local


async def refresh_active_connection_heartbeat(
    instance_id: Optional[str] = None,
    ttl_seconds: int = ACTIVE_CONN_TTL_SECONDS,
    redis=None,
) -> int:
    """為長連線（SSE / WebSocket）定時續期本副本在 Redis 中的連線計數鍵 TTL"""
    total_local = _active_http_connections + _active_ws_connections
    if total_local <= 0:
        return 0
    node_id = instance_id or settings.INSTANCE_ID
    try:
        client = redis or get_redis()
        pipe = client.pipeline(transaction=False)
        pipe.sadd(ACTIVE_CONN_INSTANCES_SET_KEY, node_id)
        pipe.set(_active_conn_node_key(node_id), str(total_local), ex=ttl_seconds)
        await pipe.execute()
    except Exception:
        pass
    return total_local


async def get_cluster_active_connections(redis=None) -> int:
    """彙總叢集所有存活（未過期 TTL）API 副本的即時在線連線數，並自動清理已離線節點"""
    try:
        client = redis or get_redis()
        instance_ids = sorted(await client.smembers(ACTIVE_CONN_INSTANCES_SET_KEY))
        if instance_ids:
            node_keys = [_active_conn_node_key(nid) for nid in instance_ids]
            values = await client.mget(node_keys)
            total = 0
            expired_nodes: List[str] = []
            for nid, val in zip(instance_ids, values):
                if val is None:
                    expired_nodes.append(nid)
                else:
                    total += int(val)
            if expired_nodes:
                await client.srem(ACTIVE_CONN_INSTANCES_SET_KEY, *expired_nodes)
            return total
    except Exception:
        pass
    return _active_http_connections + _active_ws_connections


class ActiveConnectionASGIMiddleware:
    """
    純 ASGI 層級即時連線數追蹤中介層：
    - 同時攔截 `http`（含長連線 SSE `StreamingResponse`，直到串流結束或斷線才遞減）
      與 `websocket`（直到 WebSocket 斷線才遞減），克服 `BaseHTTPMiddleware` 對串流與 WS 的限制。
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        scope_type = scope.get("type")
        if scope_type == "http":
            kind = "http"
        elif scope_type == "websocket":
            kind = "ws"
        else:
            await self.app(scope, receive, send)
            return

        await increment_active_connections(kind, 1)
        try:
            await self.app(scope, receive, send)
        finally:
            await increment_active_connections(kind, -1)


async def record_http_observation(
    *,
    request_id: str,
    user_id: Optional[int],
    method: str,
    path: str,
    status_code: int,
    latency_ms: float,
    client_ip: Optional[str],
    timestamp_sec: Optional[int] = None,
    redis=None,
) -> None:
    """
    於 HTTP Middleware 完成時以單次 Redis Pipeline 記錄：
    1. AccessLog 佇列項目
    2. 該秒的 RPS、5xx 錯誤數與延遲樣本
    （盡力記錄，任何 Redis 異常絕不影響主請求回應）
    """
    ts_sec = int(time.time()) if timestamp_sec is None else int(timestamp_sec)
    latency_rounded = round(float(latency_ms), 2)
    log_entry = json.dumps(
        {
            "request_id": request_id[:64],
            "user_id": user_id,
            "method": method[:10],
            "path": path[:255],
            "status_code": int(status_code),
            "latency_ms": f"{latency_rounded:.2f}",
            "client_ip": (client_ip or "")[:45] or None,
            "created_at_epoch": ts_sec,
        }
    )

    try:
        client = redis or get_redis()
        counts_key = _sec_counts_key(ts_sec)
        lat_key = _sec_latencies_key(ts_sec)
        pipe = client.pipeline(transaction=False)
        pipe.rpush(ACCESS_LOG_QUEUE_KEY, log_entry)
        pipe.sadd(DIRTY_SECONDS_KEY, str(ts_sec))
        pipe.hincrby(counts_key, "total", 1)
        if status_code >= 500:
            pipe.hincrby(counts_key, "errors", 1)
        pipe.expire(counts_key, SEC_BUCKET_TTL_SECONDS)
        pipe.rpush(lat_key, f"{latency_rounded:.2f}")
        pipe.expire(lat_key, SEC_BUCKET_TTL_SECONDS)
        await pipe.execute()
    except Exception as exc:
        logger.debug(f"Skipped Redis monitoring record for {request_id}: {exc}")


async def flush_access_logs_batch(
    session: AsyncSession,
    redis=None,
    batch_size: int = 500,
) -> int:
    """從 Redis 佇列批次取出存取日誌並一次寫入 PostgreSQL `access_logs`"""
    client = redis or get_redis()
    raw_items = await client.lrange(ACCESS_LOG_QUEUE_KEY, 0, batch_size - 1)
    if not raw_items:
        return 0

    await client.ltrim(ACCESS_LOG_QUEUE_KEY, len(raw_items), -1)

    rows: List[AccessLog] = []
    for raw in raw_items:
        try:
            data = json.loads(raw)
            ts_epoch = data.get("created_at_epoch")
            created_dt = (
                datetime.fromtimestamp(int(ts_epoch), tz=timezone.utc)
                if ts_epoch is not None
                else datetime.now(timezone.utc)
            )
            rows.append(
                AccessLog(
                    request_id=data["request_id"],
                    user_id=data.get("user_id"),
                    method=data["method"],
                    path=data["path"],
                    status_code=int(data["status_code"]),
                    latency_ms=Decimal(str(data["latency_ms"])),
                    client_ip=data.get("client_ip"),
                    created_at=created_dt,
                )
            )
        except Exception as exc:
            logger.warning(f"Dropped malformed access log entry: {exc}")

    if not rows:
        return 0

    try:
        session.add_all(rows)
        await session.commit()
        return len(rows)
    except Exception:
        await session.rollback()
        # 寫庫失敗時放回佇列前端待下輪重試
        try:
            await client.lpush(ACCESS_LOG_QUEUE_KEY, *reversed(raw_items))
        except Exception:
            pass
        raise


async def prune_old_metric_flush_claims(
    session: AsyncSession,
    retention_seconds: int = METRIC_CLAIM_RETENTION_SECONDS,
) -> int:
    """清理超過保留期限（預設 48h，嚴格大於 Redis 24h TTL）的 metric_flush_claims，避免資料表無限增長"""
    stmt = delete(MetricFlushClaim).where(
        MetricFlushClaim.created_at
        < text("NOW() - make_interval(secs => :secs)").bindparams(secs=int(retention_seconds))
    )
    res = await session.execute(stmt)
    await session.commit()
    return int(res.rowcount or 0)


async def _finalize_inflight_in_redis(
    client,
    *,
    ts_sec: int,
    claim_id: str,
    claim_member: str,
) -> None:
    """透過 Lua 腳本去重追加延遲樣本至 merged_latencies 並移除 inflight 鍵"""
    await client.eval(
        _FINALIZE_INFLIGHT_LUA,
        5,
        INFLIGHT_CLAIMS_ZSET_KEY,
        _inflight_counts_key(ts_sec, claim_id),
        _inflight_latencies_key(ts_sec, claim_id),
        _sec_merged_latencies_key(ts_sec),
        _sec_merged_claims_key(ts_sec),
        claim_member,
        claim_id,
        str(MERGED_LATENCIES_TTL_SECONDS),
    )


async def _persist_staged_bucket(
    session: AsyncSession,
    client,
    *,
    ts_sec: int,
    claim_id: str,
    claim_member: str,
    total_req: int,
    error_req: int,
    new_lat_raw: List[str],
    active_conns: int,
    on_after_commit: Optional[Callable[[], Awaitable[None]]] = None,
) -> bool:
    """
    將已暫存於 inflight 的秒桶以「恰好一次 (Exactly-Once)」語意寫入 PostgreSQL：
    1. 同一 DB 交易內先寫入 `metric_flush_claims(claim_id)`（PK 唯一鍵 ON CONFLICT DO NOTHING）。
    2. 若該 `claim_id` 已存在，代表前次 Worker 於 DB commit 後、清除 Redis 前崩潰 -> 略過累加、僅補完 Redis 清理。
    3. 若為首次認領，執行 `metrics_per_second` Upsert 並一次 commit；commit 成功後才清除 Redis inflight 鍵。
    """
    inflight_counts = _inflight_counts_key(ts_sec, claim_id)
    inflight_lat = _inflight_latencies_key(ts_sec, claim_id)
    merged_lat_key = _sec_merged_latencies_key(ts_sec)

    if total_req <= 0:
        pipe = client.pipeline(transaction=False)
        pipe.zrem(INFLIGHT_CLAIMS_ZSET_KEY, claim_member)
        pipe.delete(inflight_counts, inflight_lat)
        await pipe.execute()
        return False

    bucket_dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
    existing_merged = await client.lrange(merged_lat_key, 0, -1)
    combined_lat_raw = list(existing_merged) + new_lat_raw
    latencies = [float(x) for x in combined_lat_raw]
    p50 = compute_percentile(latencies, 50)
    p95 = compute_percentile(latencies, 95)
    p99 = compute_percentile(latencies, 99)

    claim_stmt = (
        pg_insert(MetricFlushClaim)
        .values(claim_id=claim_member, bucket_timestamp=bucket_dt)
        .on_conflict_do_nothing(index_elements=["claim_id"])
        .returning(MetricFlushClaim.claim_id)
    )

    stmt = pg_insert(SystemMetric).values(
        timestamp=bucket_dt,
        total_requests=total_req,
        error_requests=error_req,
        p50_latency_ms=p50,
        p95_latency_ms=p95,
        p99_latency_ms=p99,
        active_connections=active_conns,
    )
    upsert_stmt = stmt.on_conflict_do_update(
        constraint="uq_metrics_timestamp",
        set_={
            "total_requests": SystemMetric.total_requests + stmt.excluded.total_requests,
            "error_requests": SystemMetric.error_requests + stmt.excluded.error_requests,
            "p50_latency_ms": stmt.excluded.p50_latency_ms,
            "p95_latency_ms": stmt.excluded.p95_latency_ms,
            "p99_latency_ms": stmt.excluded.p99_latency_ms,
            "active_connections": stmt.excluded.active_connections,
        },
    )

    try:
        inserted_claim = (await session.execute(claim_stmt)).scalar_one_or_none()
        if inserted_claim is None:
            # 該 claim 已於先前交易成功提交（DB commit 後、Redis 清除前遭強殺）-> 不得重複計數
            await session.rollback()
            await _finalize_inflight_in_redis(
                client, ts_sec=ts_sec, claim_id=claim_id, claim_member=claim_member
            )
            return False

        await session.execute(upsert_stmt)
        await session.commit()
    except Exception:
        await session.rollback()
        raise

    if on_after_commit is not None:
        await on_after_commit()

    # 僅在 PostgreSQL commit 成功後才清除 inflight 鍵，並以 claim_id 去重追加至 merged_latencies
    await _finalize_inflight_in_redis(
        client, ts_sec=ts_sec, claim_id=claim_id, claim_member=claim_member
    )
    return True


async def flush_per_second_metrics(
    session: AsyncSession,
    redis=None,
    flush_before_epoch: Optional[int] = None,
    inflight_stale_seconds: int = INFLIGHT_STALE_SECONDS,
    on_after_commit: Optional[Callable[[], Awaitable[None]]] = None,
) -> int:
    """
    將已完成之秒級指標桶批次彙總並 Upsert 至 PostgreSQL `metrics_per_second`：
    1. 先回收因前次 Worker 異常中斷而遺留的過期 inflight 暫存桶：
       - commit 前中斷 -> 正常補寫入 DB，零遺失。
       - commit 後、清 Redis 前中斷 -> 透過 `metric_flush_claims` 冪等防重，零重複計數。
    2. 透過 Redis Lua 腳本將活躍秒桶原子 RENAME 至 inflight 暫存鍵，僅於 DB commit 成功後才刪除 inflight。
    3. 透過 `merged_latencies` 保留 300s 合併窗口，遲到副本寫入時完整合併樣本重算 p50/p95/p99。
    """
    client = redis or get_redis()
    now_epoch = int(time.time())
    cutoff = now_epoch if flush_before_epoch is None else int(flush_before_epoch)
    active_conns = await get_cluster_active_connections(redis=client)
    flushed = 0

    # 1. 回收因前次 Worker 中斷而遺留的過期 inflight 暫存桶
    stale_before = now_epoch - inflight_stale_seconds
    stale_claims = await client.zrangebyscore(
        INFLIGHT_CLAIMS_ZSET_KEY, "-inf", stale_before
    )
    for claim_member in stale_claims:
        parts = str(claim_member).split(":", 1)
        if len(parts) != 2 or not parts[0].isdigit():
            await client.zrem(INFLIGHT_CLAIMS_ZSET_KEY, claim_member)
            continue
        ts_sec = int(parts[0])
        claim_id = parts[1]
        inf_counts = await client.hgetall(_inflight_counts_key(ts_sec, claim_id))
        inf_lat = await client.lrange(_inflight_latencies_key(ts_sec, claim_id), 0, -1)
        total_req = int(inf_counts.get("total", 0)) if inf_counts else 0
        error_req = int(inf_counts.get("errors", 0)) if inf_counts else 0
        if await _persist_staged_bucket(
            session,
            client,
            ts_sec=ts_sec,
            claim_id=claim_id,
            claim_member=str(claim_member),
            total_req=total_req,
            error_req=error_req,
            new_lat_raw=list(inf_lat),
            active_conns=active_conns,
            on_after_commit=on_after_commit,
        ):
            flushed += 1

    # 2. 原子搬移並處理新的待寫入秒桶
    dirty_members = await client.smembers(DIRTY_SECONDS_KEY)
    if not dirty_members:
        return flushed

    ready_seconds = sorted(
        int(m) for m in dirty_members if str(m).isdigit() and int(m) < cutoff
    )
    for ts_sec in ready_seconds:
        claim_id = uuid.uuid4().hex[:12]
        claim_member = f"{ts_sec}:{claim_id}"
        claimed = await client.eval(
            _STAGE_SECOND_BUCKET_LUA,
            6,
            DIRTY_SECONDS_KEY,
            _sec_counts_key(ts_sec),
            _sec_latencies_key(ts_sec),
            INFLIGHT_CLAIMS_ZSET_KEY,
            _inflight_counts_key(ts_sec, claim_id),
            _inflight_latencies_key(ts_sec, claim_id),
            str(ts_sec),
            claim_member,
            str(now_epoch),
            str(SEC_BUCKET_TTL_SECONDS),
        )
        total_req = int(claimed[0]) if claimed and claimed[0] else 0
        error_req = int(claimed[1]) if claimed and claimed[1] else 0
        new_lat_raw = list(claimed[2]) if claimed and len(claimed) > 2 and claimed[2] else []

        if await _persist_staged_bucket(
            session,
            client,
            ts_sec=ts_sec,
            claim_id=claim_id,
            claim_member=claim_member,
            total_req=total_req,
            error_req=error_req,
            new_lat_raw=new_lat_raw,
            active_conns=active_conns,
            on_after_commit=on_after_commit,
        ):
            flushed += 1

    # 3. 週期性清理超過保留期限（48h）的 metric_flush_claims，防止冪等表無限膨脹
    global _last_claim_prune_epoch
    mono_now = time.monotonic()
    if mono_now - _last_claim_prune_epoch >= METRIC_CLAIM_PRUNE_INTERVAL_SECONDS:
        try:
            await prune_old_metric_flush_claims(session)
            _last_claim_prune_epoch = mono_now
        except Exception as exc:
            await session.rollback()
            logger.warning(f"Failed to prune expired metric_flush_claims: {exc}")

    return flushed
