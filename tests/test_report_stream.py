"""大數據串流 Excel 銷售報表匯出整合測試 (Commit 23)

驗證項目：
1. Excel 結構完整性：`Summary`（整體 KPI + 分區彙總）與 `Details`（正確關聯 order_items/seats/users 明細）。
2. 時區與型別規範：表頭標示 `Asia/Taipei`、日期轉換為 `+0800`；金額以 Excel 數值型別儲存（非字串）。
3. 角色授權校驗：User 僅能匯出自己建立的活動（他人活動回 403）；Admin 與 Viewer 可匯出全部活動。
4. 低記憶體串流驗證：匯出 10,000 筆明細時，Python 峰值記憶體增量嚴格小於 100 MB。
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import io
import tracemalloc
import xml.etree.ElementTree as ET
import zipfile

import pytest
from sqlalchemy import select, text

from app.auth.seed import SEED_ACCOUNTS, seed_users
from app.models import Event, EventStatus, Seat, SeatStatus, User, Zone

NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _login_seed(client, role: str) -> str:
    acc = next(a for a in SEED_ACCOUNTS if a["role"].value == role)
    res = client.post(
        "/api/v1/auth/login",
        json={"email": acc["email"], "password": acc["password"]},
    )
    assert res.status_code == 200, res.text
    return res.json()["access_token"]


def _parse_xlsx_sheets(xlsx_bytes: bytes):
    """使用標準庫 zipfile + ElementTree 解析 xlsx 工作表名稱、儲存格值與型別 (numeric vs string)"""
    with zipfile.ZipFile(io.BytesIO(xlsx_bytes), "r") as zf:
        shared_strings = []
        if "xl/sharedStrings.xml" in zf.namelist():
            ss_root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in ss_root.findall("s:si", NS):
                shared_strings.append(
                    "".join(t.text or "" for t in si.findall(".//s:t", NS))
                )

        wb_root = ET.fromstring(zf.read("xl/workbook.xml"))
        sheet_names = [
            el.attrib["name"] for el in wb_root.findall(".//s:sheets/s:sheet", NS)
        ]

        sheets = {}
        for idx, name in enumerate(sheet_names, start=1):
            xml_path = f"xl/worksheets/sheet{idx}.xml"
            root = ET.fromstring(zf.read(xml_path))
            rows_parsed = []
            for row_el in root.findall(".//s:sheetData/s:row", NS):
                row_cells = []
                for c_el in row_el.findall("s:c", NS):
                    ctype = c_el.attrib.get("t", "n")
                    v_el = c_el.find("s:v", NS)
                    is_el = c_el.find("s:is", NS)
                    if ctype == "s" and v_el is not None and v_el.text is not None:
                        val = shared_strings[int(v_el.text)]
                    elif ctype == "inlineStr" and is_el is not None:
                        val = "".join(t.text or "" for t in is_el.findall(".//s:t", NS))
                    elif v_el is not None and v_el.text is not None:
                        val = float(v_el.text)
                    else:
                        val = ""
                    row_cells.append((ctype, val))
                rows_parsed.append(row_cells)
            sheets[name] = rows_parsed
        return sheet_names, sheets


@pytest.mark.asyncio
async def test_report_export_summary_zone_aggregation_taipei_tz_and_numeric_money(
    client, db_session
):
    """驗證 Summary 摘要、Zone 分區彙總、Details 關聯欄位、Asia/Taipei 時區與金額數值型別"""
    seed_users()
    admin_token = _login_seed(client, "admin")
    user_token = _login_seed(client, "user")

    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    now = datetime.now(timezone.utc)
    event = Event(
        creator_id=admin_user.id,
        title="Excel Report Concert",
        category="concert",
        venue="Taipei Dome",
        start_time=now - timedelta(hours=1),
        end_time=now + timedelta(days=1),
        status=EventStatus.PUBLISHED.value,
    )
    db_session.add(event)
    await db_session.flush()
    db_session.add_all(
        [
            Zone(event_id=event.id, zone_code="A", name="搖滾 A 區", price=Decimal("1000.00")),
            Zone(event_id=event.id, zone_code="B", name="看台 B 區", price=Decimal("2500.00")),
        ]
    )
    await db_session.flush()
    s1 = Seat(event_id=event.id, zone_code="A", row_label="1", seat_number="1", status=SeatStatus.AVAILABLE.value)
    s2 = Seat(event_id=event.id, zone_code="B", row_label="2", seat_number="5", status=SeatStatus.AVAILABLE.value)
    db_session.add_all([s1, s2])
    await db_session.commit()
    await db_session.refresh(s1)
    await db_session.refresh(s2)

    # 建立一筆已付款訂單（A 區 NT$1,000 + 5% 手續費 NT$50 = NT$1,050）與一筆待付款訂單（B 區 NT$2,500）
    r1 = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [s1.id]},
    )
    assert r1.status_code == 201, r1.text
    o1_id = r1.json()["id"]
    p1 = client.post(
        "/api/v1/payments/mock/pay",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"order_id": o1_id, "outcome": "succeeded"},
    )
    assert p1.status_code == 200, p1.text

    r2 = client.post(
        f"/api/v1/events/{event.id}/tickets/reserve",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"seat_ids": [s2.id]},
    )
    assert r2.status_code == 201, r2.text

    # Admin 匯出報表
    exp_res = client.get(
        f"/api/v1/reports/exports?event_id={event.id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert exp_res.status_code == 200, exp_res.text
    assert "spreadsheetml.sheet" in exp_res.headers.get("content-type", "")
    assert exp_res.headers.get("x-report-timezone") == "Asia/Taipei"
    assert exp_res.headers.get("x-report-rows") == "2"

    sheet_names, sheets = _parse_xlsx_sheets(exp_res.content)
    assert sheet_names == ["Summary", "Details"]

    # 驗證 Summary 工作表含 KPI 與 A/B 兩區彙總，且金額為數值型別 (t == "n")
    summary_rows = sheets["Summary"]
    flat_summary = [[val for _, val in r] for r in summary_rows]
    assert any("Asia/Taipei" in str(cell) for row in flat_summary for cell in row)
    # 檢查已付款實收總額 = 1050.0 (數值型別)
    paid_total_row = next(r for r in summary_rows if r and r[0][1] == "已付款實收總額 (NT$)")
    assert paid_total_row[1][0] == "n"
    assert paid_total_row[1][1] == 1050.0
    # 檢查分區彙總含 A 與 B 區
    zone_codes_in_summary = {r[2][1] for r in summary_rows if len(r) >= 9 and r[2][1] in ("A", "B")}
    assert zone_codes_in_summary == {"A", "B"}

    # 驗證 Details 表頭標註 Asia/Taipei，資料列包含關聯欄位且金額皆為數值型別
    details_rows = sheets["Details"]
    header_vals = [val for _, val in details_rows[0]]
    assert "Created At (Asia/Taipei)" in header_vals
    assert "Expires At (Asia/Taipei)" in header_vals
    assert "Seat Price (NT$)" in header_vals
    assert len(details_rows) == 3  # 1 header + 2 detail items

    first_detail = details_rows[1]
    # Col 10=Seat Price, 11=Order Ticket Amt, 12=Service Fee, 13=Order Total -> 全數為 Excel 數值型別
    for col_idx in (10, 11, 12, 13):
        assert first_detail[col_idx][0] == "n", f"Column {col_idx} must be numeric"
    assert first_detail[10][1] == 1000.0
    assert first_detail[12][1] == 50.0
    assert first_detail[13][1] == 1050.0
    assert "+0800" in str(first_detail[14][1])


@pytest.mark.asyncio
async def test_report_export_rbac_user_own_event_only_vs_admin_and_viewer(
    client, db_session
):
    """驗證角色授權：User 僅能匯出自己建立的活動（他人活動 403），Viewer/Admin 可匯出全部"""
    seed_users()
    admin_token = _login_seed(client, "admin")
    user_token = _login_seed(client, "user")
    viewer_token = _login_seed(client, "viewer")

    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()
    normal_user = (
        await db_session.execute(select(User).where(User.email == "user@flashticket.com"))
    ).scalar_one()

    now = datetime.now(timezone.utc)
    ev_admin = Event(
        creator_id=admin_user.id,
        title="Admin Owned Event",
        category="concert",
        venue="Arena",
        start_time=now,
        end_time=now + timedelta(days=1),
        status=EventStatus.PUBLISHED.value,
    )
    ev_user = Event(
        creator_id=normal_user.id,
        title="User Owned Event",
        category="concert",
        venue="Arena",
        start_time=now,
        end_time=now + timedelta(days=1),
        status=EventStatus.PUBLISHED.value,
    )
    db_session.add_all([ev_admin, ev_user])
    await db_session.commit()
    await db_session.refresh(ev_admin)
    await db_session.refresh(ev_user)

    # 1. User 匯出他人（Admin）活動 -> 403 Forbidden
    r_forbid = client.get(
        f"/api/v1/reports/exports?event_id={ev_admin.id}",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert r_forbid.status_code == 403

    # 2. User 匯出自己建立的活動 -> 200 OK
    r_own = client.get(
        f"/api/v1/reports/exports?event_id={ev_user.id}",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert r_own.status_code == 200

    # 3. Viewer 與 Admin 可匯出任意活動與全站報表 -> 200 OK
    for tok in (viewer_token, admin_token):
        assert (
            client.get(
                f"/api/v1/reports/exports?event_id={ev_admin.id}",
                headers={"Authorization": f"Bearer {tok}"},
            ).status_code
            == 200
        )
        assert (
            client.get(
                "/api/v1/reports/exports",
                headers={"Authorization": f"Bearer {tok}"},
            ).status_code
            == 200
        )


@pytest.mark.asyncio
async def test_report_export_10k_rows_constant_memory_under_100mb(client, db_session):
    """驗證 10,000 筆訂單明細串流匯出時，Python 峰值記憶體增量嚴格低於 100 MB"""
    seed_users()
    admin_token = _login_seed(client, "admin")
    admin_user = (
        await db_session.execute(select(User).where(User.email == "admin@flashticket.com"))
    ).scalar_one()

    now = datetime.now(timezone.utc)
    ev = Event(
        creator_id=admin_user.id,
        title="10K Streaming Memory Event",
        category="concert",
        venue="Taipei Dome",
        start_time=now - timedelta(hours=1),
        end_time=now + timedelta(days=1),
        status=EventStatus.PUBLISHED.value,
    )
    db_session.add(ev)
    await db_session.flush()
    db_session.add(Zone(event_id=ev.id, zone_code="A", name="A 區", price=Decimal("1000.00")))
    await db_session.flush()
    seat = Seat(event_id=ev.id, zone_code="A", row_label="1", seat_number="1", status=SeatStatus.AVAILABLE.value)
    db_session.add(seat)
    await db_session.flush()

    # 快速以 SQL generate_series 建立 10,000 筆訂單與明細
    await db_session.execute(
        text(
            """
            WITH ins_orders AS (
                INSERT INTO orders (
                    user_id, event_id, total_ticket_amount, service_fee, total_amount,
                    status, expires_at
                )
                SELECT :uid, :eid, 1000.00, 50.00, 1050.00, 'cancelled', :exp
                FROM generate_series(1, 10000)
                RETURNING id
            )
            INSERT INTO order_items (order_id, seat_id, price, status)
            SELECT id, :sid, 1000.00, 'cancelled' FROM ins_orders
            """
        ),
        {
            "uid": admin_user.id,
            "eid": ev.id,
            "sid": seat.id,
            "exp": now + timedelta(minutes=10),
        },
    )
    await db_session.commit()

    tracemalloc.start()
    with client.stream(
        "GET",
        f"/api/v1/reports/exports?event_id={ev.id}&batch_size=1000",
        headers={"Authorization": f"Bearer {admin_token}"},
    ) as res:
        assert res.status_code == 200
        assert res.headers.get("x-report-rows") == "10000"
        total_bytes = sum(len(chunk) for chunk in res.iter_bytes(chunk_size=65536))
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_mb = peak_bytes / (1024 * 1024)
    assert total_bytes > 100_000
    assert peak_mb < 100.0, f"Peak memory {peak_mb:.2f} MB exceeded 100 MB budget"
