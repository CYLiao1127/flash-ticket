"""資料庫種子帳號初始化腳本

負責在系統首次啟動或 Migration 完成後自動建立三大預設角色帳號：
1. Admin: admin@flashticket.com / AdminPass123! (最高管理權限)
2. User: user@flashticket.com / UserPass123! (一般搶票使用者)
3. Viewer: viewer@flashticket.com / ViewerPass123! (營運監控唯讀檢視者)
"""

import logging
from sqlalchemy import select
from app.db import SyncSessionLocal
from app.models import User, UserRole
from app.auth.security import hash_password

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flash_ticket.seed")

SEED_ACCOUNTS = [
    {
        "email": "admin@flashticket.com",
        "password": "AdminPass123!",
        "role": UserRole.ADMIN,
    },
    {
        "email": "user@flashticket.com",
        "password": "UserPass123!",
        "role": UserRole.USER,
    },
    {
        "email": "viewer@flashticket.com",
        "password": "ViewerPass123!",
        "role": UserRole.VIEWER,
    },
]


def seed_users() -> None:
    """
    冪等建立預設種子帳號
    若帳號已存在則跳過，絕不覆蓋既有密碼、角色或啟用狀態，保留管理者的修改記錄。
    """
    with SyncSessionLocal() as session:
        for account in SEED_ACCOUNTS:
            email = account["email"].lower().strip()
            stmt = select(User).where(User.email == email)
            existing_user = session.execute(stmt).scalar_one_or_none()

            if not existing_user:
                hashed = hash_password(account["password"])
                user = User(
                    email=email,
                    hashed_password=hashed,
                    role=account["role"],
                    is_active=True,
                    token_version=1,
                )
                session.add(user)
                logger.info(f"[Seed] Created default {account['role'].value} account: {email}")
            else:
                logger.info(f"[Seed] Account already exists, skipping overwrite: {email}")

        session.commit()
    logger.info("[Seed] All default seed accounts verified successfully.")


if __name__ == "__main__":
    seed_users()
