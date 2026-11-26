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


async def register_user(
    db: AsyncSession,
    email: str,
    password: str,
    role: UserRole = UserRole.USER,
) -> User:
    """註冊新使用者"""
    normalized_email = email.lower().strip()
    hashed = hash_password(password)
    user = User(
        email=normalized_email,
        hashed_password=hashed,
        role=role,
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
    同步將最新 token_version 快取入 Redis，確保極速 O(1) 驗證
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

    try:
        await set_cached_token_version(redis, user.id, user.token_version)
    except Exception as e:
        logger.warning(f"Failed to update token_version in Redis for user {user.id}: {e}")
        try:
            await redis.delete(get_token_version_key(user.id))
        except Exception:
            pass

    logger.info(f"User id={user.id} tokens revoked. New token_version={user.token_version}")
    return user.token_version


async def deactivate_user(
    db: AsyncSession,
    redis: aioredis.Redis,
    user: User,
) -> None:
    """
    停用使用者帳號並作廢憑證 (Q5.2 機制)
    """
    user.is_active = False
    user.token_version += 1
    await db.commit()
    await db.refresh(user)

    try:
        await set_cached_token_version(redis, user.id, user.token_version)
    except Exception as e:
        logger.warning(f"Failed to update token_version in Redis for user {user.id}: {e}")
        try:
            await redis.delete(get_token_version_key(user.id))
        except Exception:
            pass

    logger.info(f"User id={user.id} deactivated. All tokens revoked.")
