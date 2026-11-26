from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, field_validator
from app.models import UserRole


class UserRegisterRequest(BaseModel):
    email: str = Field(..., description="使用者信箱")
    password: str = Field(..., min_length=8, description="密碼 (至少 8 字元)")

    @field_validator("email")
    @classmethod
    def validate_email_format(cls, v: str) -> str:
        v = v.strip().lower()
        if "@" not in v or "." not in v.split("@")[-1] or len(v) < 5:
            raise ValueError("Invalid email format")
        return v


class UserLoginRequest(BaseModel):
    email: str = Field(..., description="使用者信箱")
    password: str = Field(..., description="密碼")

    @field_validator("email")
    @classmethod
    def validate_email_format(cls, v: str) -> str:
        v = v.strip().lower()
        if "@" not in v or "." not in v.split("@")[-1] or len(v) < 5:
            raise ValueError("Invalid email format")
        return v


class UserResponse(BaseModel):
    id: int
    email: str
    role: str
    is_active: bool
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user: UserResponse


class UserRoleUpdateRequest(BaseModel):
    role: UserRole = Field(..., description="新的使用者角色 (admin/user/viewer)")


class UserStatusUpdateRequest(BaseModel):
    is_active: bool = Field(..., description="帳號啟用狀態")
