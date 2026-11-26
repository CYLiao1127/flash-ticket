from datetime import datetime, timezone, timedelta
from typing import Optional, Any, Dict
import jwt
from passlib.context import CryptContext
from app.config import settings

# 密碼雜湊管理器 (固定相容之 bcrypt 實作)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    """計算純文字密碼之 Bcrypt 雜湊"""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """驗證密碼與雜湊是否吻合"""
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(
    data: Dict[str, Any],
    expires_delta: Optional[timedelta] = None,
) -> str:
    """
    簽發 JWT Access Token
    Payload 包含：
    - sub: 使用者 ID (str)
    - role: 角色名稱
    - token_version: 憑證版本號 (Q5.1 關鍵失效欄位)
    - exp: 過期時戳 (UTC)
    - iat: 簽發時戳 (UTC)
    """
    to_encode = data.copy()
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({
        "exp": expire,
        "iat": now,
    })

    encoded_jwt = jwt.encode(
        to_encode,
        settings.SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    return encoded_jwt


def decode_access_token(token: str) -> Dict[str, Any]:
    """
    解析並驗證 JWT Token 簽章與有效期限
    若無效或過期將拋出 jwt.PyJWTError
    """
    return jwt.decode(
        token,
        settings.SECRET_KEY,
        algorithms=[settings.JWT_ALGORITHM],
    )
