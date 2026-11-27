import pytest
from starlette.testclient import TestClient
from sqlalchemy import select
from app.models import User, UserRole
from app.auth.seed import seed_users
from app.auth.service import deactivate_user, get_token_version_key
from app.auth.security import create_access_token
from app.core.redis import get_redis


def test_seed_accounts_creation_and_login(client):
    """驗證系統種子帳號 (Admin, User, Viewer) 建立與登入功能"""
    seed_users()

    accounts = [
        ("admin@flashticket.com", "AdminPass123!", "admin"),
        ("user@flashticket.com", "UserPass123!", "user"),
        ("viewer@flashticket.com", "ViewerPass123!", "viewer"),
    ]

    for email, password, expected_role in accounts:
        res = client.post(
            "/api/v1/auth/login",
            json={"email": email, "password": password},
        )
        assert res.status_code == 200, f"Login failed for {email}: {res.text}"
        data = res.json()
        assert "access_token" in data
        assert data["user"]["role"] == expected_role
        assert data["user"]["email"] == email


def test_register_and_login_flow(client):
    """驗證一般使用者註冊、重複檢查、密碼驗證與 /me 查詢流程"""
    test_email = "new_shopper@flashticket.com"
    test_password = "SecurePassword123!"

    # 1. 註冊新帳號
    res = client.post(
        "/api/v1/auth/register",
        json={"email": test_email, "password": test_password},
    )
    assert res.status_code == 201
    user_data = res.json()
    assert user_data["email"] == test_email
    assert user_data["role"] == "user"

    # 2. 重複註冊相同 Email 必須被拒絕 (400)
    res_dup = client.post(
        "/api/v1/auth/register",
        json={"email": test_email, "password": test_password},
    )
    assert res_dup.status_code == 400
    assert "already registered" in res_dup.json()["detail"]

    # 3. 錯誤密碼登入必須被拒絕 (401)
    res_bad = client.post(
        "/api/v1/auth/login",
        json={"email": test_email, "password": "WrongPassword!"},
    )
    assert res_bad.status_code == 401

    # 4. 正確密碼登入取得 Token
    res_login = client.post(
        "/api/v1/auth/login",
        json={"email": test_email, "password": test_password},
    )
    assert res_login.status_code == 200
    token = res_login.json()["access_token"]

    # 5. 使用 Token 訪問 /me 端點
    res_me = client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert res_me.status_code == 200
    assert res_me.json()["email"] == test_email


def test_immediate_token_revocation_on_logout(client):
    """
    ★ 關鍵 Q5.1 驗證：使用者登出時，既有 JWT 必須『立即作廢』，不依賴過期時間
    """
    email = "logout_test@flashticket.com"
    password = "Password123!"

    # 註冊並登入
    client.post("/api/v1/auth/register", json={"email": email, "password": password})
    res_login = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    token = res_login.json()["access_token"]

    # 確認登出前 Token 有效
    res_pre = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert res_pre.status_code == 200

    # 呼叫登出端點 (觸發 token_version 遞增與 Redis 同步)
    res_logout = client.post("/api/v1/auth/logout", headers={"Authorization": f"Bearer {token}"})
    assert res_logout.status_code == 200

    # 再次使用原 Token 訪問 /me，必須立即被拒絕 (401 Token has been revoked)
    res_post = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert res_post.status_code == 401
    assert "revoked" in res_post.json()["detail"].lower()


@pytest.mark.asyncio
async def test_immediate_rejection_upon_user_deactivation(client, db_session):
    """
    ★ 關鍵 Q5.2 驗證：使用者帳號停用 (is_active=False) 時，已簽發之 JWT 必須立即被拒絕
    """
    email = "deactivate_test@flashticket.com"
    password = "Password123!"

    client.post("/api/v1/auth/register", json={"email": email, "password": password})
    res_login = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    token = res_login.json()["access_token"]

    # 確認當前 Token 有效
    res_valid = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert res_valid.status_code == 200

    # 後台管理員停用該使用者
    stmt = select(User).where(User.email == email)
    res = await db_session.execute(stmt)
    user = res.scalar_one()

    redis = get_redis()
    await deactivate_user(db_session, redis, user)

    # 停用後立即再發請求，必須被 401 拒絕
    res_deactivated = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert res_deactivated.status_code == 401
    assert "deactivated" in res_deactivated.json()["detail"].lower() or "revoked" in res_deactivated.json()["detail"].lower()


def test_invalid_and_tampered_token(client):
    """驗證無效或被篡改之 JWT 標頭處理"""
    # 1. 缺少 Authorization 標頭
    assert client.get("/api/v1/auth/me").status_code == 401

    # 2. 格式不正確 (非 Bearer)
    assert client.get("/api/v1/auth/me", headers={"Authorization": "Basic 123"}).status_code == 401

    # 3. 偽造或遭竄改的 JWT
    assert client.get("/api/v1/auth/me", headers={"Authorization": "Bearer fake.jwt.token"}).status_code == 401


@pytest.mark.asyncio
async def test_stale_redis_token_version_does_not_reject_current_token(
    client, db_session
):
    email = "stale_cache_test@flashticket.com"
    password = "Password123!"
    assert client.post(
        "/api/v1/auth/register", json={"email": email, "password": password}
    ).status_code == 201
    old_token = client.post(
        "/api/v1/auth/login", json={"email": email, "password": password}
    ).json()["access_token"]

    user = (await db_session.execute(select(User).where(User.email == email))).scalar_one()
    old_version = user.token_version
    user.token_version += 1
    await db_session.commit()

    redis = get_redis()
    await redis.set(get_token_version_key(user.id), str(old_version), ex=60)
    current_token = create_access_token(
        {
            "sub": str(user.id),
            "role": user.role,
            "token_version": user.token_version,
        }
    )

    res_current = client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {current_token}"},
    )
    assert res_current.status_code == 200
    assert await redis.get(get_token_version_key(user.id)) == str(user.token_version)

    res_old = client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {old_token}"},
    )
    assert res_old.status_code == 401
