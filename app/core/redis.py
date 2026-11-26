import asyncio
from typing import Dict
import redis.asyncio as aioredis
from app.config import settings

# 依據 Event Loop 分離連線客戶端，兼顧高併發複用與多迴圈環境安全隔離
_clients: Dict[int, aioredis.Redis] = {}


def get_redis() -> aioredis.Redis:
    """
    獲取與當前執行協程之 Event Loop 綁定且複用的非同步 Redis 客戶端實例
    避免每次請求重複建構連線，同時防止跨 Loop 存取 Future 衝突
    """
    try:
        loop = asyncio.get_running_loop()
        loop_id = id(loop)
    except RuntimeError:
        loop_id = 0

    if loop_id not in _clients:
        _clients[loop_id] = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            max_connections=50,
        )
    return _clients[loop_id]


async def close_redis() -> None:
    """優雅關閉所有已開啟之 Redis 連線池"""
    for client in list(_clients.values()):
        try:
            await client.aclose()
        except Exception:
            pass
    _clients.clear()
