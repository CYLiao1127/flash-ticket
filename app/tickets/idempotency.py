"""搶票冪等性 (Idempotency-Key) 協調模組 (Commit 11)

語意規範：
- 首次成功建立訂單：回傳 201 Created，快取結果至 Redis 並持久化 Order.idempotency_key。
- 相同 Key + 相同 Payload 已完成：回傳 200 OK 快取結果（不重複建單）。
- 相同 Key 正在處理中：回傳 409 Conflict。
- 相同 Key + 不同 Payload（或跨使用者／跨活動）：回傳 422 Unprocessable Entity。
"""

import hashlib
import json
import logging
from typing import Any, Dict, Optional, Tuple

from fastapi import HTTPException, status

from app.core.redis import get_redis
from app.tickets.schemas import OrderReservationResponse

logger = logging.getLogger("flash_ticket.tickets.idempotency")

IDEMPOTENCY_LOCK_TTL_SECONDS = 300
IDEMPOTENCY_CACHE_TTL_SECONDS = 86400

_RELEASE_IF_PROCESSING_LUA = """
local val = redis.call('GET', KEYS[1])
if not val then
    return 0
end
if string.find(val, ARGV[1], 1, true) and string.find(val, '"state": "processing"', 1, true) then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


def normalize_idempotency_key(raw_key: Optional[str]) -> Optional[str]:
    """驗證並正規化 Idempotency-Key 標頭（若提供則不可為空白且長度上限 128）"""
    if raw_key is None:
        return None
    cleaned = raw_key.strip()
    if not cleaned:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Idempotency-Key header must not be blank",
        )
    if len(cleaned) > 128:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Idempotency-Key header exceeds maximum length of 128 characters",
        )
    return cleaned


def build_request_fingerprint(
    user_id: int,
    event_id: int,
    action: str,
    payload: Dict[str, Any],
) -> str:
    """建立請求指紋 SHA-256（涵蓋使用者、活動、操作類型與正規化參數）"""
    canonical = json.dumps(
        {
            "user_id": user_id,
            "event_id": event_id,
            "action": action,
            "payload": payload,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _redis_key(idempotency_key: str) -> str:
    return f"idempotency:tickets:{idempotency_key}"


async def acquire_idempotency_slot(
    idempotency_key: str,
    fingerprint: str,
) -> Tuple[Optional[OrderReservationResponse], bool]:
    """
    嘗試於 Redis 原子佔用 Idempotency-Key 處理槽位。
    回傳 (cached_response, acquired_lock)：
    - (None, True)：首次請求取得處理權，應繼續執行搶票交易。
    - (OrderReservationResponse, False)：命中已完成快取，呼叫端應直接回傳 200 OK。
    - 若相同 Key 正在處理中 -> 拋出 409 Conflict。
    - 若相同 Key 但指紋不符 -> 拋出 422 Unprocessable Entity。
    """
    redis = get_redis()
    rkey = _redis_key(idempotency_key)
    in_progress_record = json.dumps(
        {"state": "processing", "fingerprint": fingerprint},
        sort_keys=True,
    )

    try:
        acquired = await redis.set(
            rkey,
            in_progress_record,
            nx=True,
            ex=IDEMPOTENCY_LOCK_TTL_SECONDS,
        )
        if acquired:
            return None, True

        existing_raw = await redis.get(rkey)
        if existing_raw is None:
            # 剛剛好過期或釋放，再嘗試取得一次
            acquired_retry = await redis.set(
                rkey,
                in_progress_record,
                nx=True,
                ex=IDEMPOTENCY_LOCK_TTL_SECONDS,
            )
            if acquired_retry:
                return None, True
            existing_raw = await redis.get(rkey)
            if existing_raw is None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Request with this Idempotency-Key is currently being processed",
                )

        existing = json.loads(existing_raw)
        if existing.get("fingerprint") != fingerprint:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Idempotency-Key was already used with different request parameters",
            )
        if existing.get("state") == "processing":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Request with this Idempotency-Key is currently being processed",
            )
        if existing.get("state") == "completed" and "response" in existing:
            return OrderReservationResponse.model_validate(existing["response"]), False

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Request with this Idempotency-Key is currently being processed",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(
            f"Redis idempotency unavailable for key={idempotency_key}, falling back to DB: {exc}"
        )
        return None, False


async def complete_idempotency_slot(
    idempotency_key: str,
    fingerprint: str,
    response: OrderReservationResponse,
) -> None:
    """將已成功提交之訂單結果寫入 Redis 冪等快取"""
    try:
        redis = get_redis()
        rkey = _redis_key(idempotency_key)
        record = json.dumps(
            {
                "state": "completed",
                "fingerprint": fingerprint,
                "response": response.model_dump(mode="json"),
            },
            sort_keys=True,
        )
        await redis.set(rkey, record, ex=IDEMPOTENCY_CACHE_TTL_SECONDS)
    except Exception as exc:
        logger.warning(f"Failed to cache completed idempotency result for key={idempotency_key}: {exc}")


async def release_idempotency_slot(
    idempotency_key: str,
    fingerprint: str,
) -> None:
    """交易失敗時安全釋放仍處於 processing 狀態之冪等鎖（絕不誤刪已完成快取）"""
    try:
        redis = get_redis()
        rkey = _redis_key(idempotency_key)
        await redis.eval(
            _RELEASE_IF_PROCESSING_LUA,
            1,
            rkey,
            f'"fingerprint": "{fingerprint}"',
        )
    except Exception as exc:
        logger.warning(f"Failed to release idempotency lock for key={idempotency_key}: {exc}")
