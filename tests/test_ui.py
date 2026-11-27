"""前台即時選位介面與後台監控儀表板 Nginx 靜態託管整合測試 (Commit 21)"""

import httpx
import pytest


@pytest.mark.asyncio
async def test_nginx_serves_booking_and_admin_ui_and_proxies_api():
    """驗證 Nginx 正確提供靜態前端頁面與資源，且 API / Health 反向代理維持正常"""
    async with httpx.AsyncClient(base_url="http://nginx:80", timeout=5.0) as client:
        # 1. 前台即時座位選位頁
        res_index = await client.get("/")
        assert res_index.status_code == 200
        assert "text/html" in res_index.headers.get("content-type", "")
        assert 'id="seatGrid"' in res_index.text
        assert "booking.js" in res_index.text

        # 2. 後台即時監控儀表板
        res_admin = await client.get("/admin.html")
        assert res_admin.status_code == 200
        assert "text/html" in res_admin.headers.get("content-type", "")
        assert 'id="chartRps"' in res_admin.text
        assert 'id="alertsFeed"' in res_admin.text
        assert "admin.js" in res_admin.text

        # 3. 靜態資源 (CSS / JS)
        for asset in ("/styles.css", "/booking.js", "/admin.js"):
            res_asset = await client.get(asset)
            assert res_asset.status_code == 200, f"Failed to load {asset}"
            assert len(res_asset.text) > 100

        # 4. API 與 Health 代理未受影響
        res_health = await client.get("/health")
        assert res_health.status_code == 200
        assert res_health.json() == {"status": "ok"}

        res_docs = await client.get("/openapi.json")
        assert res_docs.status_code == 200
        assert "Flash Ticket API" in res_docs.text
