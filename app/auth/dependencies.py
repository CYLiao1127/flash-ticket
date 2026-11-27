from dataclasses import dataclass
from typing import Callable, FrozenSet, List, Optional
from fastapi import Depends, HTTPException, status
from fastapi.routing import APIRoute
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis
import jwt

from app.db import get_db
from app.core.redis import get_redis
from app.models import User, UserRole
from app.auth.security import decode_access_token
from app.auth.service import get_cached_token_version, set_cached_token_version

security_bearer = HTTPBearer(auto_error=False)


async def get_current_user(
    auth: Optional[HTTPAuthorizationCredentials] = Depends(security_bearer),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> User:
    """
    全域認證依賴項目 (Dependency)
    驗證流程：
    1. 驗證 Authorization: Bearer 標頭存在性
    2. 驗證 JWT 簽章與過期時戳 (pyjwt)
    3. 優先向 Redis 進行 O(1) token_version 比對 (Q5.1 無中心化瓶頸設計)
    4. 若 Redis Cache Miss，回源 DB 並回填 Redis 快取
    5. 檢查 token_version 一致性；不符立即判定憑證作廢 (401)
    6. 檢查使用者 is_active 狀態；停用帳號立即拒絕 (401, Q5.2)
    """
    if not auth or auth.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = auth.credentials

    try:
        payload = decode_access_token(token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    sub = payload.get("sub")
    token_version = payload.get("token_version")

    if not sub or token_version is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        user_id = int(sub)
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user identification in token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 1. 優先查詢 Redis 快取之 token_version (O(1))
    try:
        cached_version = await get_cached_token_version(redis, user_id)
    except Exception as e:
        # Redis 異常時優雅降級回源資料庫核對
        cached_version = None

    user: Optional[User] = None

    if cached_version is not None:
        # Cache Hit：優先比對 Redis 快取版本，不符即快速拒絕
        if cached_version != token_version:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has been revoked",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # 取得使用者資料實例，並以 DB 實體版本為最終真實來源防線
        stmt = select(User).where(User.id == user_id)
        res = await db.execute(stmt)
        user = res.scalar_one_or_none()
        if user and user.token_version != token_version:
            # 偵測到 Redis 快取陳舊，以 DB 為準同步修正快取並立即拒絕
            try:
                await set_cached_token_version(redis, user.id, user.token_version)
            except Exception:
                pass
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has been revoked",
                headers={"WWW-Authenticate": "Bearer"},
            )
    else:
        # Cache Miss 或 Redis 暫時故障：回源 DB 查詢並回填 Redis
        stmt = select(User).where(User.id == user_id)
        res = await db.execute(stmt)
        user = res.scalar_one_or_none()
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if user.token_version != token_version:
            try:
                await set_cached_token_version(redis, user.id, user.token_version)
            except Exception:
                pass
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has been revoked",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # 回填正確版本號
        try:
            await set_cached_token_version(redis, user.id, user.token_version)
        except Exception:
            pass

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account has been deactivated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


get_current_user.__requires_auth__ = True
get_current_user.__allowed_roles__ = frozenset(r.value for r in UserRole)


def public_endpoint(reason: str) -> Callable:
    """聲明公開端點（免認證），須附帶明確原因供授權稽核驗證 (Q5.3)"""
    if not reason or not reason.strip():
        raise ValueError("@public_endpoint requires a non-empty reason")

    def decorator(func: Callable) -> Callable:
        func.__is_public__ = True
        func.__public_reason__ = reason.strip()
        return func

    return decorator


def require_roles(*allowed_roles: UserRole) -> Callable:
    """
    角色權限守門員依賴項目 (Role-Based Access Control)
    """
    role_values = [r.value if isinstance(r, UserRole) else str(r) for r in allowed_roles]

    async def role_checker(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role not in role_values:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Operation not permitted: Insufficient permissions",
            )
        return current_user

    role_checker.__requires_auth__ = True
    role_checker.__allowed_roles__ = frozenset(role_values)
    return role_checker


@dataclass(frozen=True)
class RouteAuthPolicy:
    """路由授權政策宣告結構"""

    path: str
    methods: FrozenSet[str]
    is_public: bool
    public_reason: Optional[str]
    requires_auth: bool
    allowed_roles: FrozenSet[str]


def inspect_route_auth_policy(route: APIRoute) -> RouteAuthPolicy:
    """遍歷 APIRoute 依賴樹，檢查並解析宣告的授權政策 (Q5.3)"""
    is_public = bool(getattr(route.endpoint, "__is_public__", False))
    public_reason = getattr(route.endpoint, "__public_reason__", None)

    requires_auth = False
    role_sets: List[FrozenSet[str]] = []

    stack = list(route.dependant.dependencies)
    visited = set()
    while stack:
        dep = stack.pop()
        dep_id = id(dep)
        if dep_id in visited:
            continue
        visited.add(dep_id)

        call = getattr(dep, "call", None)
        if call is not None and getattr(call, "__requires_auth__", False):
            requires_auth = True
            allowed = getattr(call, "__allowed_roles__", frozenset())
            role_sets.append(frozenset(allowed))

        sub_deps = getattr(dep, "dependencies", None) or []
        stack.extend(sub_deps)

    effective_roles = frozenset.intersection(*role_sets) if role_sets else frozenset()

    if is_public and requires_auth:
        raise ValueError(
            f"Contradictory auth policy on {route.path}: marked @public_endpoint but also has auth dependencies"
        )

    methods = frozenset(
        m for m in (route.methods or set()) if m not in {"HEAD", "OPTIONS"}
    )
    return RouteAuthPolicy(
        path=route.path,
        methods=methods,
        is_public=is_public,
        public_reason=public_reason,
        requires_auth=requires_auth,
        allowed_roles=effective_roles,
    )
