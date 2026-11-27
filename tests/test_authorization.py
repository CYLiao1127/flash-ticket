"""動態路由授權覆蓋與角色權限矩陣自動化測試 (Q5.3)

核心防禦目標：
1. 遍歷 app.routes 中所有註冊的 APIRoute，強制每個端點必須明確宣告：
   - 認證與角色權限相依項 (`get_current_user` / `require_roles(...)`)，或
   - 公開端點裝飾器 (`@public_endpoint(reason=...)`) 且位於顯式白名單內。
   任何新增端點若遺漏授權宣告，CI 測試即自動失敗。
2. 以動態角色矩陣（匿名 / Admin / User / Viewer）實際打向所有受保護端點，驗證 401/403 守門行為。
3. 驗證 Admin 變更角色與停用帳號 API 會立即作廢既有憑證 (Q5.1 / Q5.2)。
"""

import re
from typing import Dict, Iterator, Set, Tuple
from fastapi import FastAPI
from fastapi.routing import APIRoute

from app.main import app
from app.auth.dependencies import (
    get_current_user,
    inspect_route_auth_policy,
    public_endpoint,
)
from app.auth.seed import SEED_ACCOUNTS, seed_users


FRAMEWORK_IGNORED_PATHS: Set[str] = {
    "/openapi.json",
    "/docs",
    "/docs/oauth2-redirect",
    "/redoc",
}

EXPECTED_PUBLIC_ROUTES: Set[Tuple[str, str]] = {
    ("GET", "/health"),
    ("POST", "/api/v1/auth/register"),
    ("POST", "/api/v1/auth/login"),
    ("POST", "/api/v1/payments/webhook"),  # HMAC-SHA256 signed gateway callback
}

ALL_ROLES: Tuple[str, ...] = ("admin", "user", "viewer")


def _iter_app_routes() -> Iterator[APIRoute]:
    """遍歷 FastAPI 應用中所有業務 API 路由（排除內建文件端點）"""
    for route in app.routes:
        if isinstance(route, APIRoute) and route.path not in FRAMEWORK_IGNORED_PATHS:
            yield route


def _materialize_path(path: str) -> str:
    """將路徑參數佔位符（例如 {user_id}）替換為合法整數識別碼"""
    return re.sub(r"\{[^}]+\}", "999999", path)


def _login_role(client, role: str) -> str:
    """使用種子帳號登入指定角色並回傳有效的 Bearer JWT"""
    account = next(acc for acc in SEED_ACCOUNTS if acc["role"].value == role)
    res = client.post(
        "/api/v1/auth/login",
        json={"email": account["email"], "password": account["password"]},
    )
    assert res.status_code == 200, f"Seed login failed for role={role}: {res.text}"
    return res.json()["access_token"]


def test_all_routes_declare_explicit_authorization_policy():
    """
    ★ 關鍵 Q5.3 守門員：掃描所有 app.routes，未宣告授權且未在公開白名單之端點一律失敗
    """
    discovered_public_routes: Set[Tuple[str, str]] = set()
    protected_count = 0

    for route in _iter_app_routes():
        policy = inspect_route_auth_policy(route)
        assert policy.methods, f"Route {route.path} has no HTTP methods"

        if policy.is_public:
            assert policy.public_reason, (
                f"Public route {route.path} must provide a non-empty reason"
            )
            for method in policy.methods:
                route_key = (method, route.path)
                assert route_key in EXPECTED_PUBLIC_ROUTES, (
                    f"Unexpected @public_endpoint detected: {route_key} "
                    f"(reason={policy.public_reason!r}). Add to whitelist only after security review."
                )
                discovered_public_routes.add(route_key)
        else:
            assert policy.requires_auth and len(policy.allowed_roles) > 0, (
                f"Unprotected route detected without auth dependency or @public_endpoint: "
                f"{sorted(policy.methods)} {route.path}"
            )
            protected_count += 1

    assert discovered_public_routes == EXPECTED_PUBLIC_ROUTES, (
        f"Public route whitelist mismatch. "
        f"Missing={EXPECTED_PUBLIC_ROUTES - discovered_public_routes}, "
        f"Extra={discovered_public_routes - EXPECTED_PUBLIC_ROUTES}"
    )
    assert protected_count >= 5, "Expected protected auth & admin endpoints to be covered"


def test_unprotected_route_detection_guard():
    """
    反向自證測試：驗證當開發者忘記掛載權限檢查或重複宣告衝突策略時，檢查器能精準抓出漏洞
    """
    dummy_app = FastAPI()

    from fastapi import Depends

    @dummy_app.get("/leaked-admin-endpoint")
    async def leaked_endpoint():
        return {"secret": "exposed"}

    @dummy_app.get("/real-conflict")
    @public_endpoint("Conflict test")
    async def real_conflict_endpoint(_=Depends(get_current_user)):
        return {"status": "conflict"}

    routes_by_path = {
        r.path: r for r in dummy_app.routes if isinstance(r, APIRoute)
    }

    leaked_policy = inspect_route_auth_policy(routes_by_path["/leaked-admin-endpoint"])
    assert leaked_policy.is_public is False
    assert leaked_policy.requires_auth is False
    assert leaked_policy.allowed_roles == frozenset()

    import pytest

    with pytest.raises(ValueError, match="Contradictory auth policy"):
        inspect_route_auth_policy(routes_by_path["/real-conflict"])


def test_public_routes_accessible_without_token(client):
    """驗證白名單內的所有公開端點可在未攜帶憑證的情況下存取（不回傳 401/403）"""
    for method, path in sorted(EXPECTED_PUBLIC_ROUTES):
        target_path = _materialize_path(path)
        kwargs = {"json": {}} if method in {"POST", "PUT", "PATCH"} else {}
        res = client.request(method, target_path, **kwargs)
        assert res.status_code not in (401, 403, 500), (
            f"Public route {method} {path} unexpectedly blocked or errored: "
            f"status={res.status_code}, body={res.text}"
        )


def test_dynamic_role_matrix_enforcement(client):
    """
    ★ 關鍵 Q5.3 矩陣驗證：針對所有受保護端點，逐一測試「匿名 / Admin / User / Viewer」四種身分
    註：此矩陣專注驗證「權限守門結果」（匿名 401、越權 403、授權角色不回 401/403/500）；
    各端點之具體業務邏輯與輸入驗證（如 200/400/404/422）另由各模組功能測試驗證。
    """
    seed_users()
    tokens: Dict[str, str] = {role: _login_role(client, role) for role in ALL_ROLES}

    for route in _iter_app_routes():
        policy = inspect_route_auth_policy(route)
        if policy.is_public:
            continue

        target_path = _materialize_path(route.path)

        for method in sorted(policy.methods):
            req_kwargs = {"json": {}} if method in {"POST", "PUT", "PATCH"} else {}
            if route.path.endswith("/alerts/stream"):
                req_kwargs["params"] = {"max_events": 0}

            # 1. 匿名存取必須被拒絕 (401 Unauthorized)
            anon_res = client.request(method, target_path, **req_kwargs)
            assert anon_res.status_code == 401, (
                f"Anonymous request to {method} {route.path} expected 401, "
                f"got {anon_res.status_code}: {anon_res.text}"
            )

            # 2. 逐一測試三種角色
            for role in ALL_ROLES:
                res = client.request(
                    method,
                    target_path,
                    headers={"Authorization": f"Bearer {tokens[role]}"},
                    **req_kwargs,
                )

                if role not in policy.allowed_roles:
                    assert res.status_code == 403, (
                        f"Role '{role}' on {method} {route.path} expected 403 Forbidden, "
                        f"got {res.status_code}: {res.text}"
                    )
                else:
                    assert res.status_code not in (401, 403, 500), (
                        f"Authorized role '{role}' on {method} {route.path} failed auth/server: "
                        f"status={res.status_code}, body={res.text}"
                    )

                # 若測試打到 /logout 成功撤銷了該角色的 Token，立即重新登入刷新以供後續路由使用
                if route.path == "/api/v1/auth/logout" and res.status_code == 200:
                    tokens[role] = _login_role(client, role)


def test_admin_role_change_immediately_revokes_old_token(client):
    """
    ★ 關鍵 Q5.1 驗證：Admin 透過 API 修改使用者角色後，舊角色 JWT 必須立即作廢 (401)
    """
    seed_users()
    target_email = "authz_role_target@flashticket.com"
    target_password = "TargetPass123!"

    # 1. 註冊並登入目標使用者 (預設 role=user)
    reg_res = client.post(
        "/api/v1/auth/register",
        json={"email": target_email, "password": target_password},
    )
    if reg_res.status_code == 201:
        target_id = reg_res.json()["id"]
    else:
        login_tmp = client.post(
            "/api/v1/auth/login",
            json={"email": target_email, "password": target_password},
        )
        assert login_tmp.status_code == 200
        target_id = login_tmp.json()["user"]["id"]

    login_res = client.post(
        "/api/v1/auth/login",
        json={"email": target_email, "password": target_password},
    )
    assert login_res.status_code == 200
    old_token = login_res.json()["access_token"]

    me_before = client.get(
        "/api/v1/auth/me", headers={"Authorization": f"Bearer {old_token}"}
    )
    assert me_before.status_code == 200
    assert me_before.json()["role"] == "user"

    # 2. Admin 呼叫 PATCH /api/v1/admin/users/{id}/role 將角色改為 viewer
    admin_token = _login_role(client, "admin")
    patch_res = client.patch(
        f"/api/v1/admin/users/{target_id}/role",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"role": "viewer"},
    )
    assert patch_res.status_code == 200
    assert patch_res.json()["role"] == "viewer"

    # 3. 舊 JWT（內含舊 role=user 與舊 token_version）必須立刻被拒絕
    me_revoked = client.get(
        "/api/v1/auth/me", headers={"Authorization": f"Bearer {old_token}"}
    )
    assert me_revoked.status_code == 401
    assert "revoked" in me_revoked.json()["detail"].lower()

    # 4. 重新登入取得最新角色 Token 後可正常存取
    relogin_res = client.post(
        "/api/v1/auth/login",
        json={"email": target_email, "password": target_password},
    )
    assert relogin_res.status_code == 200
    assert relogin_res.json()["user"]["role"] == "viewer"
    new_token = relogin_res.json()["access_token"]

    me_after = client.get(
        "/api/v1/auth/me", headers={"Authorization": f"Bearer {new_token}"}
    )
    assert me_after.status_code == 200
    assert me_after.json()["role"] == "viewer"


def test_admin_user_status_management_and_self_protection(client):
    """
    驗證 Admin 停用/啟用 API、非 Admin 禁止存取 (403) 與自我保護防呆 (400)
    """
    seed_users()
    target_email = "authz_status_target@flashticket.com"
    target_password = "StatusPass123!"

    reg_res = client.post(
        "/api/v1/auth/register",
        json={"email": target_email, "password": target_password},
    )
    assert reg_res.status_code == 201
    target_id = reg_res.json()["id"]

    target_login = client.post(
        "/api/v1/auth/login",
        json={"email": target_email, "password": target_password},
    )
    assert target_login.status_code == 200
    target_token = target_login.json()["access_token"]

    admin_token = _login_role(client, "admin")
    user_token = _login_role(client, "user")
    viewer_token = _login_role(client, "viewer")

    # 1. 非 Admin 嘗試停用帳號必須回傳 403
    for non_admin_token in (user_token, viewer_token):
        forbidden_res = client.patch(
            f"/api/v1/admin/users/{target_id}/status",
            headers={"Authorization": f"Bearer {non_admin_token}"},
            json={"is_active": False},
        )
        assert forbidden_res.status_code == 403

    # 2. Admin 停用目標帳號
    deactivate_res = client.patch(
        f"/api/v1/admin/users/{target_id}/status",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"is_active": False},
    )
    assert deactivate_res.status_code == 200
    assert deactivate_res.json()["is_active"] is False

    # 3. 目標帳號原有 Token 立刻失效，且禁止重新登入
    assert (
        client.get(
            "/api/v1/auth/me", headers={"Authorization": f"Bearer {target_token}"}
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/api/v1/auth/login",
            json={"email": target_email, "password": target_password},
        ).status_code
        == 401
    )

    # 4. Admin 重新啟用後可恢復登入
    reactivate_res = client.patch(
        f"/api/v1/admin/users/{target_id}/status",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"is_active": True},
    )
    assert reactivate_res.status_code == 200
    assert reactivate_res.json()["is_active"] is True
    assert (
        client.post(
            "/api/v1/auth/login",
            json={"email": target_email, "password": target_password},
        ).status_code
        == 200
    )

    # 5. Admin 不可透過管理端點停用自己或修改自己角色
    admin_me = client.get(
        "/api/v1/auth/me", headers={"Authorization": f"Bearer {admin_token}"}
    ).json()
    admin_id = admin_me["id"]

    assert (
        client.patch(
            f"/api/v1/admin/users/{admin_id}/status",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={"is_active": False},
        ).status_code
        == 400
    )
    assert (
        client.patch(
            f"/api/v1/admin/users/{admin_id}/role",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={"role": "viewer"},
        ).status_code
        == 400
    )


def test_admin_list_users_and_not_found_handling(client):
    """
    驗證 Admin 使用者管理端點業務行為：
    - GET /api/v1/admin/users 正確回傳種子帳號清單（非 Admin 回傳 403）
    - PATCH /role 與 PATCH /status 對不存在之 user_id 回傳 404
    """
    seed_users()
    admin_token = _login_role(client, "admin")
    user_token = _login_role(client, "user")
    viewer_token = _login_role(client, "viewer")

    for non_admin_token in (user_token, viewer_token):
        assert (
            client.get(
                "/api/v1/admin/users",
                headers={"Authorization": f"Bearer {non_admin_token}"},
            ).status_code
            == 403
        )

    list_res = client.get(
        "/api/v1/admin/users",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert list_res.status_code == 200
    emails = {u["email"] for u in list_res.json()}
    assert {
        "admin@flashticket.com",
        "user@flashticket.com",
        "viewer@flashticket.com",
    }.issubset(emails)

    assert (
        client.patch(
            "/api/v1/admin/users/999999/role",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={"role": "viewer"},
        ).status_code
        == 404
    )
    assert (
        client.patch(
            "/api/v1/admin/users/999999/status",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={"is_active": False},
        ).status_code
        == 404
    )
