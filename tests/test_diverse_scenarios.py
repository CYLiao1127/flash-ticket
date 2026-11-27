"""多元測資與邊界情境整合測試 (Diverse Datasets & Edge-Case Verification)

涵蓋：
1. 多元座位匯入檔案（sample_data/seats_valid_multi_zone.csv、seats_mixed_errors.csv、
   seats_unicode_and_special.json）於 all_or_nothing 與 skip_invalid 模式之精確列號、錯誤原因與 UTF-8 驗證。
2. 奇數票價 5% 服務費四捨五入（ROUND_HALF_UP）、取消後重購原釋出座位、模擬金流 Webhook 付款，
   以及 Excel Summary 與 Details 金額數值型別與資料庫 SSOT 逐元對帳。
3. 活動生命週期邊界（草稿、未開賣、已結束皆不可訂票；有訂單禁刪、無訂單可刪；樂觀鎖 409；
   寫入中 Keyset 分頁不重不漏）與 Viewer 跨資源越權保護（禁建活動、禁訂票，可瀏覽與匯出報表）。
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import io
from pathlib import Path
from uuid import uuid4
import xml.etree.ElementTree as ET
import zipfile

from httpx import ASGITransport, AsyncClient
import pytest

from app.auth.seed import seed_users
from app.main import app

SAMPLE_DIR = Path(__file__).resolve().parents[1] / "sample_data"
XLSX_NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


async def _login(client: AsyncClient, email: str, password: str) -> str:
    seed_users()
    resp = await client.post(
        "/api/v1/auth/login",
        json={"email": email, "password": password},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


async def _register_and_login(
    client: AsyncClient, email: str, password: str = "TestPass123!"
) -> str:
    reg = await client.post(
        "/api/v1/auth/register",
        json={"email": email, "password": password, "full_name": email.split("@")[0]},
    )
    assert reg.status_code in (200, 201), reg.text
    return await _login(client, email, password)


async def _poll_import_job(client: AsyncClient, event_id: int, job_id: int, headers: dict) -> dict:
    for _ in range(40):
        j = (await client.get(f"/api/v1/events/{event_id}/imports/{job_id}", headers=headers)).json()
        if j["status"] in ("completed", "failed"):
            return j
    raise AssertionError(f"Import job {job_id} did not finish")


@pytest.mark.asyncio
async def test_diverse_seat_import_files_csv_json_and_error_modes():
    """測試全部 3 份多元 CSV / JSON 測資檔、精確錯誤列號與原因，以及 UTF-8 匯入"""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        admin_token = await _login(client, "admin@flashticket.com", "AdminPass123!")
        headers = {"Authorization": f"Bearer {admin_token}"}
        now = datetime.now(timezone.utc)

        zones_payload = [
            {"zone_code": "VIP", "name": "VIP 區", "price": "5800.00"},
            {"zone_code": "A", "name": "A 區", "price": "3200.00"},
            {"zone_code": "B", "name": "B 區", "price": "1800.00"},
            {"zone_code": "C", "name": "C 區", "price": "800.00"},
        ]

        # 活動 A：測試 seats_valid_multi_zone.csv（40 筆跨 4 區全數合法匯入）
        ev_valid = (
            await client.post(
                "/api/v1/events",
                headers=headers,
                json={
                    "title": "Multi-Zone Valid Import Event",
                    "category": "concert",
                    "venue": "Taipei Arena",
                    "start_time": (now - timedelta(hours=1)).isoformat(),
                    "end_time": (now + timedelta(days=2)).isoformat(),
                    "zones": zones_payload,
                },
            )
        ).json()
        valid_bytes = (SAMPLE_DIR / "seats_valid_multi_zone.csv").read_bytes()
        r_valid = await client.post(
            f"/api/v1/events/{ev_valid['id']}/seats/import?mode=all_or_nothing",
            headers=headers,
            files={"file": ("seats_valid_multi_zone.csv", valid_bytes, "text/csv")},
        )
        assert r_valid.status_code == 202, r_valid.text
        j_valid = await _poll_import_job(client, ev_valid["id"], r_valid.json()["id"], headers)
        assert j_valid["status"] == "completed"
        assert j_valid["imported_rows"] == 40
        assert j_valid["error_rows"] == 0
        snap_valid = (await client.get(f"/api/v1/events/{ev_valid['id']}/seats", headers=headers)).json()
        assert len(snap_valid["seats"]) == 40
        assert {s["zone_code"] for s in snap_valid["seats"]} == {"VIP", "A", "B", "C"}

        # 活動 B：測試 seats_mixed_errors.csv + seats_unicode_and_special.json
        ev_mixed = (
            await client.post(
                "/api/v1/events",
                headers=headers,
                json={
                    "title": "Diverse Mixed & Unicode Import Event",
                    "category": "concert",
                    "venue": "Kaohsiung Arena",
                    "start_time": (now - timedelta(hours=1)).isoformat(),
                    "end_time": (now + timedelta(days=2)).isoformat(),
                    "zones": zones_payload,
                },
            )
        ).json()
        event_id = ev_mixed["id"]

        # 1. all_or_nothing 模式應整批回滾（0 筆匯入）並精確核對列號 [4, 5, 6, 7] 與原因
        mixed_bytes = (SAMPLE_DIR / "seats_mixed_errors.csv").read_bytes()
        r_aon = await client.post(
            f"/api/v1/events/{event_id}/seats/import?mode=all_or_nothing",
            headers=headers,
            files={"file": ("seats_mixed_errors.csv", mixed_bytes, "text/csv")},
        )
        assert r_aon.status_code == 202, r_aon.text
        j_aon = await _poll_import_job(client, event_id, r_aon.json()["id"], headers)
        assert j_aon["status"] == "failed", j_aon
        assert j_aon["imported_rows"] == 0
        assert j_aon["error_rows"] == 4
        assert [e["row"] for e in j_aon["errors"]] == [4, 5, 6, 7]
        assert "Duplicate seat in file" in j_aon["errors"][0]["reason"]
        assert "Unknown zone_code" in j_aon["errors"][1]["reason"]
        assert "row_label" in j_aon["errors"][2]["reason"].lower() or "empty" in j_aon["errors"][2]["reason"].lower() or "missing" in j_aon["errors"][2]["reason"].lower()
        assert "seat_number" in j_aon["errors"][3]["reason"].lower() or "empty" in j_aon["errors"][3]["reason"].lower() or "missing" in j_aon["errors"][3]["reason"].lower()

        # 2. skip_invalid 模式應僅匯入 4 筆合法座位並保留精確列號 [4, 5, 6, 7]
        mixed_skip_bytes = mixed_bytes.replace(b"B,3,10", b"B,3,10 ")
        r_skip = await client.post(
            f"/api/v1/events/{event_id}/seats/import?mode=skip_invalid",
            headers=headers,
            files={"file": ("seats_mixed_errors_skip.csv", mixed_skip_bytes, "text/csv")},
        )
        assert r_skip.status_code == 202, r_skip.text
        j_skip = await _poll_import_job(client, event_id, r_skip.json()["id"], headers)
        assert j_skip["status"] == "completed", j_skip
        assert j_skip["imported_rows"] == 4
        assert j_skip["error_rows"] == 4
        assert [e["row"] for e in j_skip["errors"]] == [4, 5, 6, 7]

        # 3. UTF-8 中文與特殊字元 JSON 匯入
        json_bytes = (SAMPLE_DIR / "seats_unicode_and_special.json").read_bytes()
        r_json = await client.post(
            f"/api/v1/events/{event_id}/seats/import?mode=all_or_nothing",
            headers=headers,
            files={"file": ("seats_unicode_and_special.json", json_bytes, "application/json")},
        )
        assert r_json.status_code == 202, r_json.text
        j_json = await _poll_import_job(client, event_id, r_json.json()["id"], headers)
        assert j_json["status"] == "completed", j_json
        assert j_json["imported_rows"] == 6

        snap = (await client.get(f"/api/v1/events/{event_id}/seats", headers=headers)).json()
        assert len(snap["seats"]) == 10
        row_labels = {s["row_label"] for s in snap["seats"]}
        assert {"特一排", "B1-A", "二樓看台"}.issubset(row_labels)


@pytest.mark.asyncio
async def test_diverse_pricing_rounding_lifecycle_and_report_reconciliation():
    """測試奇數票價 5% 四捨五入、取消後重購原釋出座位、Webhook 付款與 Excel Summary/Details 逐元對帳"""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        admin_token = await _login(client, "admin@flashticket.com", "AdminPass123!")
        buyer_token = await _register_and_login(
            client, f"diverse_buyer_{uuid4().hex[:6]}@flashticket.com"
        )
        admin_h = {"Authorization": f"Bearer {admin_token}"}
        buyer_h = {"Authorization": f"Bearer {buyer_token}"}
        now = datetime.now(timezone.utc)

        ev = (
            await client.post(
                "/api/v1/events",
                headers=admin_h,
                json={
                    "title": "Odd Price Rounding Event",
                    "category": "sports",
                    "venue": "Taichung Stadium",
                    "start_time": (now - timedelta(hours=1)).isoformat(),
                    "end_time": (now + timedelta(days=2)).isoformat(),
                    "zones": [
                        {"zone_code": "ODD1", "name": "奇數區1", "price": "199.00"},
                        {"zone_code": "HALF", "name": "進位邊界區", "price": "110.00"},
                    ],
                },
            )
        ).json()
        event_id = ev["id"]

        csv_data = (
            "zone_code,row_label,seat_number\n"
            "ODD1,1,1\nODD1,1,2\nHALF,1,1\n"
        ).encode("utf-8")
        r_imp = await client.post(
            f"/api/v1/events/{event_id}/seats/import?mode=all_or_nothing",
            headers=admin_h,
            files={"file": ("odd_seats.csv", csv_data, "text/csv")},
        )
        assert r_imp.status_code == 202
        await _poll_import_job(client, event_id, r_imp.json()["id"], admin_h)
        assert (await client.post(f"/api/v1/events/{event_id}/publish", headers=admin_h)).status_code == 200

        run_tag = uuid4().hex[:8]
        # 1. 自動配位 2 張 ODD1 (199*2=398, 5%=19.9 -> 20, total=418)
        o1 = await client.post(
            f"/api/v1/events/{event_id}/tickets/allocate",
            headers={**buyer_h, "Idempotency-Key": f"diverse-o1-{event_id}-{run_tag}"},
            json={"zone_code": "ODD1", "quantity": 2},
        )
        assert o1.status_code == 201, o1.text
        d1 = o1.json()
        released_seat_ids = [item["seat_id"] for item in d1["items"]]
        assert Decimal(d1["total_ticket_amount"]) == Decimal("398.00")
        assert Decimal(d1["service_fee"]) == Decimal("20.00")
        assert Decimal(d1["total_amount"]) == Decimal("418.00")

        # 2. 取消第一筆訂單，並以指定選位「重購剛剛釋出的同兩張 ODD1 座位」
        assert (await client.post(f"/api/v1/orders/{d1['id']}/cancel", headers=buyer_h)).status_code == 200
        o1_rebuy = await client.post(
            f"/api/v1/events/{event_id}/tickets/reserve",
            headers={**buyer_h, "Idempotency-Key": f"diverse-rebuy-{event_id}-{run_tag}"},
            json={"seat_ids": released_seat_ids},
        )
        assert o1_rebuy.status_code == 201, o1_rebuy.text
        d_rebuy = o1_rebuy.json()
        assert sorted(i["seat_id"] for i in d_rebuy["items"]) == sorted(released_seat_ids)
        assert (
            await client.post(
                "/api/v1/payments/mock/pay",
                headers=buyer_h,
                json={"order_id": d_rebuy["id"], "outcome": "succeeded"},
            )
        ).status_code == 200

        # 3. 再購 HALF 1 張 (110, 5%=5.5 -> 6, total=116) 並完成付款
        o2 = await client.post(
            f"/api/v1/events/{event_id}/tickets/allocate",
            headers={**buyer_h, "Idempotency-Key": f"diverse-o2-{event_id}-{run_tag}"},
            json={"zone_code": "HALF", "quantity": 1},
        )
        assert o2.status_code == 201, o2.text
        d2 = o2.json()
        assert Decimal(d2["service_fee"]) == Decimal("6.00")
        assert Decimal(d2["total_amount"]) == Decimal("116.00")
        assert (
            await client.post(
                "/api/v1/payments/mock/pay",
                headers=buyer_h,
                json={"order_id": d2["id"], "outcome": "succeeded"},
            )
        ).status_code == 200
        assert (await client.get(f"/api/v1/orders/{d2['id']}", headers=buyer_h)).json()["status"] == "paid"

        # 4. 已有訂單的活動不可刪除 (409)
        assert (await client.delete(f"/api/v1/events/{event_id}", headers=admin_h)).status_code == 409

        # 5. 下載該活動 Excel 報表，並逐元核對 Summary 與 Details 數值儲存格
        #    已付款營收應為：ODD1(398 + 20 = 418) + HALF(110 + 6 = 116) = 票款 508、手續費 26、總營收 534
        #    明細總列數應為：5 筆 order_items（o1 cancelled 2 筆 + o1_rebuy paid 2 筆 + o2 paid 1 筆）
        rep = await client.get(f"/api/v1/reports/exports?event_id={event_id}", headers=admin_h)
        assert rep.status_code == 200
        assert int(rep.headers["X-Report-Rows"]) == 5

        with zipfile.ZipFile(io.BytesIO(rep.content), "r") as zf:
            summary_root = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
            details_root = ET.fromstring(zf.read("xl/worksheets/sheet2.xml"))

        # 核對 Summary 工作表的標籤與數值「逐項對應」（不以集合混查）
        summary_kv: dict[str, Decimal] = {}
        for row in summary_root.findall(".//s:sheetData/s:row", XLSX_NS):
            cells = row.findall("s:c", XLSX_NS)
            if len(cells) >= 2:
                c0, c1 = cells[0], cells[1]
                t_el = c0.find(".//s:t", XLSX_NS)
                v_el = c1.find("s:v", XLSX_NS)
                if t_el is not None and t_el.text and v_el is not None and v_el.text and c1.attrib.get("t") in (None, "n"):
                    summary_kv[t_el.text.strip()] = Decimal(v_el.text)
        assert summary_kv["已付款票價總額 (NT$)"] == Decimal("508")
        assert summary_kv["已付款 5% 服務費總額 (NT$)"] == Decimal("26")
        assert summary_kv["已付款實收總額 (NT$)"] == Decimal("534")

        # 核對 Details 工作表：1 標題列 + 5 資料列，且 K/L/M/N 金額欄皆為數值型別，單張票價(K欄)加總 = 199*4 + 110 = 906
        detail_rows = details_root.findall(".//s:sheetData/s:row", XLSX_NS)
        assert len(detail_rows) == 6
        seat_price_sum = Decimal("0")
        for row in detail_rows[1:]:
            for c in row.findall("s:c", XLSX_NS):
                ref = c.attrib.get("r", "")
                if ref.startswith(("K", "L", "M", "N")):
                    assert c.attrib.get("t") in (None, "n")
                if ref.startswith("K"):
                    seat_price_sum += Decimal(c.find("s:v", XLSX_NS).text)
        assert seat_price_sum == Decimal("906")


@pytest.mark.asyncio
async def test_event_boundaries_keyset_pagination_and_optimistic_lock():
    """測試草稿／未開賣／已結束禁訂、樂觀鎖 409、寫入中 Keyset 分頁不重不漏與 Viewer 越權保護"""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        admin_token = await _login(client, "admin@flashticket.com", "AdminPass123!")
        viewer_token = await _login(client, "viewer@flashticket.com", "ViewerPass123!")
        admin_h = {"Authorization": f"Bearer {admin_token}"}
        viewer_h = {"Authorization": f"Bearer {viewer_token}"}
        now = datetime.now(timezone.utc)

        async def _make_event(title: str, start_dt: datetime, end_dt: datetime, publish: bool) -> dict:
            ev = (
                await client.post(
                    "/api/v1/events",
                    headers=admin_h,
                    json={
                        "title": title,
                        "category": "exhibition",
                        "venue": "Songshan Cultural Park",
                        "start_time": start_dt.isoformat(),
                        "end_time": end_dt.isoformat(),
                        "zones": [{"zone_code": "GA", "name": "一般票", "price": "500.00"}],
                    },
                )
            ).json()
            if publish:
                csv_b = b"zone_code,row_label,seat_number\nGA,1,1\n"
                r = await client.post(
                    f"/api/v1/events/{ev['id']}/seats/import?mode=all_or_nothing",
                    headers=admin_h,
                    files={"file": ("s.csv", csv_b, "text/csv")},
                )
                await _poll_import_job(client, ev["id"], r.json()["id"], admin_h)
                assert (await client.post(f"/api/v1/events/{ev['id']}/publish", headers=admin_h)).status_code == 200
            return ev

        draft_ev = await _make_event("Draft Only Event", now - timedelta(hours=1), now + timedelta(days=2), publish=False)
        future_ev = await _make_event("Future Not Yet On Sale", now + timedelta(hours=2), now + timedelta(days=2), publish=True)
        ended_ev = await _make_event("Already Ended Event", now - timedelta(days=2), now - timedelta(hours=1), publish=True)
        active_ev = await _make_event("Active Live Event", now - timedelta(hours=1), now + timedelta(days=1), publish=True)

        # 1. 草稿、未開賣、已結束活動皆不可訂票
        for blocked_id in (draft_ev["id"], future_ev["id"], ended_ev["id"]):
            r_blk = await client.post(
                f"/api/v1/events/{blocked_id}/tickets/allocate",
                headers=admin_h,
                json={"zone_code": "GA", "quantity": 1},
            )
            assert r_blk.status_code in (400, 409), f"event {blocked_id} should reject booking: {r_blk.status_code}"

        # 2. 樂觀鎖版本衝突驗證 (409)
        v1 = draft_ev["version"]
        assert (
            await client.patch(
                f"/api/v1/events/{draft_ev['id']}",
                headers=admin_h,
                json={"title": "Draft Updated V2", "version": v1},
            )
        ).status_code == 200
        assert (
            await client.patch(
                f"/api/v1/events/{draft_ev['id']}",
                headers=admin_h,
                json={"title": "Stale Overwrite Attempt", "version": v1},
            )
        ).status_code == 409

        # 3. 寫入進行中之 Keyset 分頁穩定性（走完所有分頁，同時驗證零重複且原有 4 筆活動零遺漏）
        original_ids = {draft_ev["id"], future_ev["id"], ended_ev["id"], active_ev["id"]}
        seen_ids: list[int] = []
        cursor = None
        inserted_mid = False
        mid_ev = None
        for _ in range(20):
            url = "/api/v1/events?limit=2" + (f"&cursor={cursor}" if cursor else "")
            page = (await client.get(url, headers=admin_h)).json()
            seen_ids.extend(item["id"] for item in page["items"])
            if not inserted_mid:
                # 第 1 頁讀完後立刻插入新活動
                mid_ev = await _make_event("Inserted Mid Pagination", now, now + timedelta(days=3), publish=False)
                inserted_mid = True
            cursor = page["next_cursor"]
            if cursor is None:
                break
        assert len(seen_ids) == len(set(seen_ids)), f"Keyset pagination duplicated items: {seen_ids}"
        assert original_ids.issubset(set(seen_ids)), f"Keyset pagination missed original events: {original_ids - set(seen_ids)}"

        # 4. Viewer 不可建立活動亦不可直接訂票 (403)，但可檢視活動與下載全部報表 (200)
        assert (
            await client.post(
                "/api/v1/events",
                headers=viewer_h,
                json={
                    "title": "Viewer Forbidden",
                    "category": "concert",
                    "venue": "Nowhere",
                    "start_time": now.isoformat(),
                    "end_time": (now + timedelta(days=1)).isoformat(),
                    "zones": [{"zone_code": "A", "name": "A", "price": "100.00"}],
                },
            )
        ).status_code == 403
        assert (
            await client.post(
                f"/api/v1/events/{active_ev['id']}/tickets/allocate",
                headers=viewer_h,
                json={"zone_code": "GA", "quantity": 1},
            )
        ).status_code == 403
        assert (await client.get("/api/v1/events", headers=viewer_h)).status_code == 200
        assert (await client.get("/api/v1/reports/exports", headers=viewer_h)).status_code == 200

        # 5. 無訂單活動可正常刪除
        for clean_id in (draft_ev["id"], mid_ev["id"]):
            assert (await client.delete(f"/api/v1/events/{clean_id}", headers=admin_h)).status_code in (200, 204)
