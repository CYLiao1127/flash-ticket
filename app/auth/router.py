from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
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
    UserRoleUpdateRequest,
    UserStatusUpdateRequest,
)
from app.auth.service import (
    register_user,
    authenticate_user,
    login_user,
    revoke_user_tokens,
    update_user_role,
    update_user_status,
)
from app.auth.dependencies import get_current_user, public_endpoint, require_roles

router = APIRouter(prefix="/api/v1/auth", tags=["Authentication"])
admin_users_router = APIRouter(prefix="/api/v1/admin/users", tags=["Admin - Users"])


@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="使用者註冊",
)
@public_endpoint("Self-service user registration")
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

    try:
        user = await register_user(db, normalized_email, req.password, role=UserRole.USER)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email is already registered",
        )
    return user


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="使用者登入",
)
@public_endpoint("User credential login")
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


@admin_users_router.get(
    "",
    response_model=List[UserResponse],
    summary="取得所有使用者清單 (Admin)",
)
async def list_users(
    current_user: User = Depends(require_roles(UserRole.ADMIN)),
    db: AsyncSession = Depends(get_db),
):
    """僅限 Admin 查詢系統使用者清單"""
    stmt = select(User).order_by(User.id.asc())
    res = await db.execute(stmt)
    return res.scalars().all()


@admin_users_router.patch(
    "/{user_id}/role",
    response_model=UserResponse,
    summary="變更使用者角色並即時作廢既有憑證 (Admin)",
)
async def admin_update_user_role(
    user_id: int,
    req: UserRoleUpdateRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN)),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """僅限 Admin 修改使用者角色；變更後立即遞增 token_version 作廢舊憑證 (Q5.1)"""
    if user_id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot modify your own role",
        )

    stmt = select(User).where(User.id == user_id)
    res = await db.execute(stmt)
    target_user = res.scalar_one_or_none()
    if not target_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return await update_user_role(db, redis, target_user, req.role)


@admin_users_router.patch(
    "/{user_id}/status",
    response_model=UserResponse,
    summary="啟用或停用使用者帳號 (Admin)",
)
async def admin_update_user_status(
    user_id: int,
    req: UserStatusUpdateRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN)),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """僅限 Admin 停用或啟用帳號；停用時立即作廢既有憑證 (Q5.1 / Q5.2)"""
    if user_id == current_user.id and not req.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot deactivate your own account",
        )

    stmt = select(User).where(User.id == user_id)
    res = await db.execute(stmt)
    target_user = res.scalar_one_or_none()
    if not target_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return await update_user_status(db, redis, target_user, req.is_active)
