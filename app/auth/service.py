import logging
from typing import Optional, Tuple
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from app.config import settings
from app.models import User, UserRole
from app.auth.security import hash_password, verify_password, create_access_token

logger = logging.getLogger("flash_ticket.auth")

REDIS_TOKEN_VERSION_TTL = 7 * 86400  # 7 天快取壽命


def get_token_version_key(user_id: int) -> str:
    return f"user:{user_id}:token_version"


async def get_cached_token_version(redis: aioredis.Redis, user_id: int) -> Optional[int]:
    """從 Redis 獲取快取的使用者 token_version (O(1))"""
    val = await redis.get(get_token_version_key(user_id))
    return int(val) if val is not None else None


async def set_cached_token_version(
    redis: aioredis.Redis,
    user_id: int,
    version: int,
) -> None:
    """將使用者的 token_version 寫入 Redis 快取"""
    await redis.set(get_token_version_key(user_id), str(version), ex=REDIS_TOKEN_VERSION_TTL)


async def _sync_or_invalidate_token_version(
    redis: aioredis.Redis,
    user_id: int,
    token_version: int,
) -> None:
    """更新 Redis 中的最新 token_version；若失敗則刪除快取以強制回源查庫"""
    try:
        await set_cached_token_version(redis, user_id, token_version)
    except Exception as e:
        logger.warning(f"Failed to update token_version in Redis for user {user_id}: {e}")
        try:
            await redis.delete(get_token_version_key(user_id))
        except Exception:
            pass


async def register_user(
    db: AsyncSession,
    email: str,
    password: str,
    role: UserRole = UserRole.USER,
) -> User:
    """註冊新使用者"""
    normalized_email = email.lower().strip()
    hashed = hash_password(password)
    role_value = role.value if isinstance(role, UserRole) else str(role)
    user = User(
        email=normalized_email,
        hashed_password=hashed,
        role=role_value,
        is_active=True,
        token_version=1,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def authenticate_user(
    db: AsyncSession,
    email: str,
    password: str,
) -> Optional[User]:
    """驗證信箱與密碼"""
    normalized_email = email.lower().strip()
    stmt = select(User).where(User.email == normalized_email)
    res = await db.execute(stmt)
    user = res.scalar_one_or_none()
    if not user:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


async def login_user(
    db: AsyncSession,
    redis: aioredis.Redis,
    user: User,
) -> Tuple[str, int]:
    """
    登入使用者並簽發 JWT
    同步將最新 token_version 寫入 Redis 快取供 O(1) 版本比對
    回傳 (access_token, expires_in_seconds)
    """
    try:
        await set_cached_token_version(redis, user.id, user.token_version)
    except Exception as e:
        logger.warning(f"Could not cache token_version in Redis on login for user {user.id}: {e}")

    expires_in = settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    token = create_access_token({
        "sub": str(user.id),
        "email": user.email,
        "role": user.role,
        "token_version": user.token_version,
    })
    return token, expires_in


async def revoke_user_tokens(
    db: AsyncSession,
    redis: aioredis.Redis,
    user: User,
) -> int:
    """
    使使用者所有既有 JWT 立即作廢 (Q5.1 核心機制)
    1. DB token_version 遞增 1
    2. 即時更新 Redis 快取值 (若更新失敗則強制刪除快取，逼退快取回源 DB 檢查)
    所有已簽發包含舊 token_version 的 JWT 將在下一次請求時立即被拒 (401)
    """
    user.token_version += 1
    await db.commit()
    await db.refresh(user)

    await _sync_or_invalidate_token_version(redis, user.id, user.token_version)

    logger.info(f"User id={user.id} tokens revoked. New token_version={user.token_version}")
    return user.token_version


async def update_user_role(
    db: AsyncSession,
    redis: aioredis.Redis,
    user: User,
    new_role: UserRole,
) -> User:
    """
    變更使用者角色並遞增 token_version 使既有憑證立即失效 (Q5.1)
    """
    role_value = new_role.value if isinstance(new_role, UserRole) else str(new_role)
    user.role = role_value
    user.token_version += 1
    await db.commit()
    await db.refresh(user)

    await _sync_or_invalidate_token_version(redis, user.id, user.token_version)

    logger.info(
        f"User id={user.id} role updated to {role_value}. New token_version={user.token_version}"
    )
    return user


async def update_user_status(
    db: AsyncSession,
    redis: aioredis.Redis,
    user: User,
    is_active: bool,
) -> User:
    """
    更新使用者啟用狀態；停用時遞增 token_version 撤銷既有憑證，並將 pending 匯入任務標記為 cancelled (Q5.1 / Q5.2)
    """
    cancelled_jobs = []
    user.is_active = is_active
    if not is_active:
        user.token_version += 1
        from app.events.import_service import (
            _publish_job_progress,
            cancel_pending_import_jobs_for_user,
        )

        cancelled_jobs = await cancel_pending_import_jobs_for_user(db, user.id)
    await db.commit()
    await db.refresh(user)

    if not is_active:
        await _sync_or_invalidate_token_version(redis, user.id, user.token_version)
        for job in cancelled_jobs:
            await _publish_job_progress(job, redis=redis)

    logger.info(
        f"User id={user.id} status updated to is_active={is_active}. token_version={user.token_version}"
    )
    return user


async def deactivate_user(
    db: AsyncSession,
    redis: aioredis.Redis,
    user: User,
) -> None:
    """
    停用使用者帳號並作廢憑證 (Q5.2 機制)
    """
    await update_user_status(db, redis, user, is_active=False)
