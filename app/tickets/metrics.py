"""業務層搶票統計指標追蹤（支援行程內即時讀取與 Redis 跨副本累計）"""

import logging
import time
from typing import Dict, Optional

from app.core.redis import get_redis

logger = logging.getLogger("flash_ticket.tickets.metrics")

TICKET_METRIC_KEYS = (
    "reservation_success",
    "seat_conflict",
    "zone_sold_out",
    "limit_exceeded",
)

REDIS_GLOBAL_METRICS_KEY = "metrics:tickets:global"
TICKET_SEC_BUCKET_PREFIX = "metrics:tickets:sec:"
TICKET_SEC_BUCKET_TTL_SECONDS = 300

_local_counters: Dict[str, int] = {k: 0 for k in TICKET_METRIC_KEYS}
_local_event_counters: Dict[int, Dict[str, int]] = {}


def _empty_metrics() -> Dict[str, int]:
    return {k: 0 for k in TICKET_METRIC_KEYS}


def reset_local_ticket_metrics() -> None:
    """重設行程內本地統計計數器（含全站與各活動計數，供測試隔離使用）"""
    for k in TICKET_METRIC_KEYS:
        _local_counters[k] = 0
    _local_event_counters.clear()


def get_local_ticket_metrics(event_id: Optional[int] = None) -> Dict[str, int]:
    """回傳目前行程內的搶票統計快照（依 event_id 隔離，未指定則回傳全站）"""
    if event_id is None:
        return dict(_local_counters)
    return dict(_local_event_counters.get(event_id, _empty_metrics()))


async def record_ticket_metric(
    metric_name: str,
    event_id: Optional[int] = None,
    delta: int = 1,
    timestamp_sec: Optional[int] = None,
) -> None:
    """記錄業務層搶票指標（同步更新本地、Redis 累計與秒級滑動視窗桶，供 Q6.2 業務異常告警使用）"""
    if metric_name not in _local_counters:
        raise ValueError(f"Unknown ticket metric: {metric_name}")
    _local_counters[metric_name] += delta
    if event_id is not None:
        ev_bucket = _local_event_counters.setdefault(event_id, _empty_metrics())
        ev_bucket[metric_name] += delta

    ts_sec = int(time.time()) if timestamp_sec is None else int(timestamp_sec)
    sec_key = f"{TICKET_SEC_BUCKET_PREFIX}{ts_sec}"
    try:
        redis = get_redis()
        pipe = redis.pipeline(transaction=False)
        pipe.hincrby(REDIS_GLOBAL_METRICS_KEY, metric_name, delta)
        pipe.hincrby(sec_key, metric_name, delta)
        pipe.expire(sec_key, TICKET_SEC_BUCKET_TTL_SECONDS)
        if event_id is not None:
            pipe.hincrby(f"metrics:tickets:event:{event_id}", metric_name, delta)
        await pipe.execute()
    except Exception as exc:
        logger.warning(f"Failed to persist ticket metric '{metric_name}' to Redis: {exc}")


async def get_recent_ticket_metrics_window(
    window_seconds: int = 60,
    now_epoch: Optional[int] = None,
    redis=None,
) -> Dict[str, int]:
    """讀取最近 N 秒滑動視窗內的業務層搶票統計（供 Q6.2 購票失敗率異常偵測）"""
    totals = _empty_metrics()
    end_sec = int(time.time()) if now_epoch is None else int(now_epoch)
    start_sec = end_sec - max(1, int(window_seconds)) + 1
    try:
        client = redis or get_redis()
        pipe = client.pipeline(transaction=False)
        for sec in range(start_sec, end_sec + 1):
            pipe.hgetall(f"{TICKET_SEC_BUCKET_PREFIX}{sec}")
        results = await pipe.execute()
        for bucket in results:
            if not bucket:
                continue
            for k in TICKET_METRIC_KEYS:
                if k in bucket:
                    totals[k] += int(bucket[k])
    except Exception as exc:
        logger.warning(f"Failed to read ticket sliding-window metrics from Redis: {exc}")
    return totals


async def get_ticket_metrics_snapshot(event_id: Optional[int] = None) -> Dict[str, int]:
    """讀取業務層搶票統計指標（優先由 Redis 跨副本彙總，Redis 不可用時依 event_id 降級回傳本地值）"""
    key = (
        f"metrics:tickets:event:{event_id}"
        if event_id is not None
        else REDIS_GLOBAL_METRICS_KEY
    )
    try:
        redis = get_redis()
        raw = await redis.hgetall(key)
        if raw:
            return {k: int(raw.get(k, 0)) for k in TICKET_METRIC_KEYS}
        if event_id is not None:
            return _empty_metrics()
    except Exception as exc:
        logger.warning(f"Failed to read ticket metrics from Redis: {exc}")
    return get_local_ticket_metrics(event_id=event_id)
