"""系統面與業務面異常即時告警引擎 (Commit 19 / Q6.2)

偵測四大異常指標並透過 Redis Pub/Sub (`admin:alerts`) 即時推播給所有在線 Admin/Viewer：
1. `high_5xx_error_rate`: 滑動視窗內 5xx 錯誤率 > 5%
2. `high_p99_latency`: 滑動視窗內 p99 延遲 > 2000ms (2s)
3. `outbox_backlog_high`: `event_outbox` 待發布 (`status='pending'`) 堆積 > 1000 筆
4. `high_booking_failure_rate` (Q6.2 業務層告警): 滑動視窗內購票失敗率 > 90%
   （即使 HTTP 5xx 為 0%、系統表面「一片綠燈」，仍可即時捕捉異常搶票失敗潮並附上衝突／售罄／限購原因拆解）

告警去重冷卻：
- 每個規則以 Redis `SET metrics:alerts:cooldown:<rule> 1 NX EX <cooldown_seconds>` 實作原子冷卻，
  避免持續異常時每秒重複轟炸管理員面板。
- 每次觸發同時寫入 `metrics:alerts:history`（保留最近 200 筆）並 `PUBLISH admin:alerts` 即時廣播。
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.redis import get_redis
from app.models import EventOutbox, OutboxStatus, SystemMetric
from app.tickets.metrics import get_recent_ticket_metrics_window

logger = logging.getLogger("flash_ticket.alerts")

ALERT_CHANNEL = "admin:alerts"
ALERT_HISTORY_KEY = "metrics:alerts:history"
ALERT_COOLDOWN_PREFIX = "metrics:alerts:cooldown:"
ALERT_HISTORY_MAX_ITEMS = 200


@dataclass(frozen=True)
class AlertThresholds:
    """異常告警規則門檻設定"""

    window_seconds: int = 60
    error_rate_threshold: float = settings.ALERT_ERROR_RATE_THRESHOLD
    min_requests_for_error_rate: int = 10
    p99_latency_ms_threshold: float = float(settings.ALERT_P99_LATENCY_MS)
    outbox_pending_threshold: int = settings.ALERT_OUTBOX_LAG_THRESHOLD
    booking_failure_rate_threshold: float = settings.ALERT_BOOKING_FAILURE_RATE_THRESHOLD
    min_booking_attempts: int = 10
    cooldown_seconds: int = 60


DEFAULT_ALERT_THRESHOLDS = AlertThresholds()


def _cooldown_key(rule: str) -> str:
    return f"{ALERT_COOLDOWN_PREFIX}{rule}"


async def _dispatch_alert(
    client,
    *,
    rule: str,
    severity: str,
    title: str,
    message: str,
    metric_value: float,
    threshold: float,
    details: Dict[str, Any],
    cooldown_seconds: int,
    triggered_at: datetime,
) -> Optional[Dict[str, Any]]:
    """原子取得規則冷卻鎖；成功時寫入歷史清單並透過 Redis Pub/Sub 即時廣播"""
    if cooldown_seconds > 0:
        acquired = await client.set(
            _cooldown_key(rule), "1", nx=True, ex=int(cooldown_seconds)
        )
        if not acquired:
            return None

    alert_payload: Dict[str, Any] = {
        "alert_id": uuid.uuid4().hex,
        "rule": rule,
        "severity": severity,
        "title": title,
        "message": message,
        "metric_value": round(float(metric_value), 6),
        "threshold": round(float(threshold), 6),
        "details": details,
        "triggered_at": triggered_at.isoformat(),
    }
    serialized = json.dumps(alert_payload, ensure_ascii=False)
    pipe = client.pipeline(transaction=False)
    pipe.lpush(ALERT_HISTORY_KEY, serialized)
    pipe.ltrim(ALERT_HISTORY_KEY, 0, ALERT_HISTORY_MAX_ITEMS - 1)
    pipe.publish(ALERT_CHANNEL, serialized)
    await pipe.execute()

    logger.warning(
        f"[ALERT][{severity.upper()}] rule={rule} value={metric_value} "
        f"threshold={threshold} message={message}"
    )
    return alert_payload


async def evaluate_and_dispatch_alerts(
    session: AsyncSession,
    redis=None,
    thresholds: AlertThresholds = DEFAULT_ALERT_THRESHOLDS,
    now_epoch: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    評估四大系統與業務異常指標，超標且未處於冷卻期時立即觸發告警推播。
    回傳本輪實際發送之告警清單。
    """
    client = redis or get_redis()
    eval_epoch = int(time.time()) if now_epoch is None else int(now_epoch)
    now_dt = datetime.fromtimestamp(eval_epoch, tz=timezone.utc)
    window_start_dt = now_dt - timedelta(seconds=thresholds.window_seconds)
    dispatched: List[Dict[str, Any]] = []

    # 1 & 2. 系統面：查詢最近 window_seconds 內的每秒指標（5xx 錯誤率、p99 延遲）
    metric_rows = list(
        (
            await session.execute(
                select(SystemMetric)
                .where(
                    SystemMetric.timestamp >= window_start_dt,
                    SystemMetric.timestamp <= now_dt,
                )
                .order_by(SystemMetric.timestamp.asc())
            )
        )
        .scalars()
        .all()
    )

    total_reqs = sum(r.total_requests for r in metric_rows)
    error_reqs = sum(r.error_requests for r in metric_rows)
    max_p99 = max((float(r.p99_latency_ms) for r in metric_rows), default=0.0)

    if total_reqs >= thresholds.min_requests_for_error_rate:
        error_rate = error_reqs / total_reqs
        if error_rate > thresholds.error_rate_threshold:
            alert = await _dispatch_alert(
                client,
                rule="high_5xx_error_rate",
                severity="critical",
                title="5xx 伺服器錯誤率過高",
                message=(
                    f"最近 {thresholds.window_seconds} 秒內 5xx 錯誤率達 "
                    f"{error_rate * 100:.2f}% ({error_reqs}/{total_reqs})，"
                    f"超過門檻 {thresholds.error_rate_threshold * 100:.1f}%"
                ),
                metric_value=error_rate,
                threshold=thresholds.error_rate_threshold,
                details={
                    "window_seconds": thresholds.window_seconds,
                    "total_requests": total_reqs,
                    "error_requests": error_reqs,
                },
                cooldown_seconds=thresholds.cooldown_seconds,
                triggered_at=now_dt,
            )
            if alert:
                dispatched.append(alert)

    if max_p99 > thresholds.p99_latency_ms_threshold:
        severity = "critical" if max_p99 >= 5000.0 else "warning"
        alert = await _dispatch_alert(
            client,
            rule="high_p99_latency",
            severity=severity,
            title="API p99 回應延遲過高",
            message=(
                f"最近 {thresholds.window_seconds} 秒內 p99 延遲峰值達 "
                f"{max_p99:.2f}ms，超過門檻 {thresholds.p99_latency_ms_threshold:.0f}ms"
            ),
            metric_value=max_p99,
            threshold=thresholds.p99_latency_ms_threshold,
            details={
                "window_seconds": thresholds.window_seconds,
                "max_p99_latency_ms": round(max_p99, 2),
                "sample_buckets": len(metric_rows),
            },
            cooldown_seconds=thresholds.cooldown_seconds,
            triggered_at=now_dt,
        )
        if alert:
            dispatched.append(alert)

    # 3. 非同步佇列面：查詢 event_outbox pending 堆積量（命中部分索引 ix_outbox_pending_relay）
    pending_outbox_count = int(
        (
            await session.execute(
                select(func.count(EventOutbox.id)).where(
                    EventOutbox.status == OutboxStatus.PENDING.value
                )
            )
        ).scalar_one()
    )
    if pending_outbox_count > thresholds.outbox_pending_threshold:
        alert = await _dispatch_alert(
            client,
            rule="outbox_backlog_high",
            severity="critical",
            title="Outbox 待發布事件堆積過高",
            message=(
                f"目前 event_outbox 待發布事件累積達 {pending_outbox_count} 筆，"
                f"超過門檻 {thresholds.outbox_pending_threshold} 筆（請檢查 Relay Worker 與 Redis）"
            ),
            metric_value=float(pending_outbox_count),
            threshold=float(thresholds.outbox_pending_threshold),
            details={"pending_outbox_count": pending_outbox_count},
            cooldown_seconds=thresholds.cooldown_seconds,
            triggered_at=now_dt,
        )
        if alert:
            dispatched.append(alert)

    # 4. 業務面 (Q6.2)：滑動視窗內購票失敗率 > 90%（即使 5xx 為 0% 也能偵測）
    booking_window = await get_recent_ticket_metrics_window(
        window_seconds=thresholds.window_seconds,
        now_epoch=eval_epoch,
        redis=client,
    )
    success_cnt = booking_window["reservation_success"]
    conflict_cnt = booking_window["seat_conflict"]
    sold_out_cnt = booking_window["zone_sold_out"]
    limit_cnt = booking_window["limit_exceeded"]
    failure_cnt = conflict_cnt + sold_out_cnt + limit_cnt
    total_attempts = success_cnt + failure_cnt

    if total_attempts >= thresholds.min_booking_attempts:
        failure_rate = failure_cnt / total_attempts
        if failure_rate > thresholds.booking_failure_rate_threshold:
            alert = await _dispatch_alert(
                client,
                rule="high_booking_failure_rate",
                severity="warning",
                title="業務層購票失敗率異常飆高 (Q6.2)",
                message=(
                    f"最近 {thresholds.window_seconds} 秒內購票失敗率達 "
                    f"{failure_rate * 100:.2f}% ({failure_cnt}/{total_attempts})，"
                    f"超過門檻 {thresholds.booking_failure_rate_threshold * 100:.1f}%"
                    f"（衝突={conflict_cnt}, 售罄={sold_out_cnt}, 超限={limit_cnt}）"
                ),
                metric_value=failure_rate,
                threshold=thresholds.booking_failure_rate_threshold,
                details={
                    "window_seconds": thresholds.window_seconds,
                    "reservation_success": success_cnt,
                    "seat_conflict": conflict_cnt,
                    "zone_sold_out": sold_out_cnt,
                    "limit_exceeded": limit_cnt,
                    "total_attempts": total_attempts,
                },
                cooldown_seconds=thresholds.cooldown_seconds,
                triggered_at=now_dt,
            )
            if alert:
                dispatched.append(alert)

    return dispatched


async def get_recent_alerts(
    limit: int = 50,
    rule: Optional[str] = None,
    severity: Optional[str] = None,
    redis=None,
) -> List[Dict[str, Any]]:
    """從 Redis 告警歷史清單讀取最近觸發之告警紀錄（支援依 rule / severity 篩選）"""
    client = redis or get_redis()
    raw_items = await client.lrange(ALERT_HISTORY_KEY, 0, ALERT_HISTORY_MAX_ITEMS - 1)
    alerts: List[Dict[str, Any]] = []
    for raw in raw_items:
        try:
            item = json.loads(raw)
        except Exception:
            continue
        if rule is not None and item.get("rule") != rule:
            continue
        if severity is not None and item.get("severity") != severity:
            continue
        alerts.append(item)
        if len(alerts) >= limit:
            break
    return alerts
