from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from app.db import get_db
from app.core.redis import get_redis
from app.models import User, UserRole
from app.auth.schemas import (
    UserRegisterRequest,
    UserLoginRequest,
    UserResponse,
    TokenResponse,
)
from app.auth.service import (
    register_user,
    authenticate_user,
    login_user,
    revoke_user_tokens,
)
from app.auth.dependencies import get_current_user

router = APIRouter(prefix="/api/v1/auth", tags=["Authentication"])


@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="使用者註冊",
)
async def register(
    req: UserRegisterRequest,
    db: AsyncSession = Depends(get_db),
):
    """使用者自主註冊，預設建立 User 角色"""
    normalized_email = req.email.lower().strip()
    stmt = select(User).where(User.email == normalized_email)
    res = await db.execute(stmt)
    if res.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email is already registered",
        )

    user = await register_user(db, normalized_email, req.password, role=UserRole.USER)
    return user


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="使用者登入",
)
async def login(
    req: UserLoginRequest,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    使用者登入
    驗證通過後簽發包含 token_version 的 JWT，並同步寫入 Redis 快取
    """
    user = await authenticate_user(db, req.email, req.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account has been deactivated",
        )

    token, expires_in = await login_user(db, redis, user)
    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=expires_in,
        user=UserResponse.model_validate(user),
    )


@router.post(
    "/logout",
    summary="使用者登出 (立即作廢憑證)",
)
async def logout(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    使用者登出 (Q5.1 關鍵機制)
    遞增 token_version 並即時同步 Redis，使該帳號所有既有 JWT 瞬間作廢
    """
    await revoke_user_tokens(db, redis, current_user)
    return {
        "status": "ok",
        "message": "Successfully logged out. All existing tokens have been revoked.",
    }


@router.get(
    "/me",
    response_model=UserResponse,
    summary="獲取當前登入使用者資訊",
)
async def get_me(
    current_user: User = Depends(get_current_user),
):
    """檢視當前已認證之使用者個人資料與角色權限"""
    return current_user
