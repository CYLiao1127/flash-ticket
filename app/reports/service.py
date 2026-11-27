"""大數據低記憶體串流 Excel 銷售報表引擎 (Commit 23)

核心記憶體與效能設計（50 萬筆訂單 < 512MB RAM）：
1. 摘要與分區彙總先以 PostgreSQL SQL 聚合完成，避免將所有明細拉回 Python 記憶體統計。
2. 明細查詢使用 PostgreSQL Server-Side Named Cursor (`DECLARE ... CURSOR` + `itersize=batch_size`)，
   每次僅於記憶體駐留 1,000 筆列資料。
3. 使用磁碟暫存檔搭配 `xlsxwriter.Workbook(tmp_path, {'constant_memory': True})` 逐列寫入
   （刻意不使用 `BytesIO` / `in_memory=True`，避免覆蓋 `constant_memory` 導致整份檔案滯留記憶體）。
4. 所有時間戳轉換為 `Asia/Taipei` (UTC+8) 並標註於表頭；所有金額皆以 Excel 數值型別 (`write_number`) 儲存。
5. 匯出檔案以 64KB chunk 串流輸出，完成或中斷後立即刪除磁碟暫存檔。
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import os
import tempfile
from typing import AsyncIterator, List, Optional, Tuple
from zoneinfo import ZoneInfo

from fastapi import HTTPException, status
import psycopg2
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import xlsxwriter

from app.config import settings
from app.models import Event, User, UserRole

TAIPEI_TZ = ZoneInfo("Asia/Taipei")
DEFAULT_CURSOR_BATCH_SIZE = 1000
STREAM_CHUNK_SIZE = 65536


@dataclass
class ReportScope:
    event_id: Optional[int]
    creator_id_filter: Optional[int]
    event_title: Optional[str]


@dataclass
class GeneratedReportResult:
    file_path: str
    filename: str
    detail_rows: int
    total_orders: int
    zone_rows: int


async def resolve_report_scope(
    db: AsyncSession,
    current_user: User,
    event_id: Optional[int],
) -> ReportScope:
    """
    角色權限校驗：
    - Admin / Viewer：可匯出指定活動或全站所有活動報表。
    - User：指定 event_id 時必須為該活動建立者（否則 403）；未指定時僅限匯出自己建立的活動。
    """
    is_privileged = current_user.role in (UserRole.ADMIN.value, UserRole.VIEWER.value)

    if event_id is not None:
        event = (
            await db.execute(select(Event).where(Event.id == event_id))
        ).scalar_one_or_none()
        if event is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Event not found",
            )
        if not is_privileged and event.creator_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Users can only export reports for events they created",
            )
        return ReportScope(
            event_id=event.id,
            creator_id_filter=None,
            event_title=event.title,
        )

    return ReportScope(
        event_id=None,
        creator_id_filter=None if is_privileged else current_user.id,
        event_title=None,
    )


def _format_taipei(dt: Optional[datetime]) -> str:
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M:%S %z")


def _build_where_clause(
    scope: ReportScope,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
) -> Tuple[str, List[object]]:
    conditions = ["1=1"]
    params: List[object] = []

    if scope.event_id is not None:
        conditions.append("o.event_id = %s")
        params.append(scope.event_id)
    if scope.creator_id_filter is not None:
        conditions.append("e.creator_id = %s")
        params.append(scope.creator_id_filter)
    if date_from is not None:
        conditions.append("o.created_at >= %s")
        params.append(date_from)
    if date_to is not None:
        conditions.append("o.created_at <= %s")
        params.append(date_to)

    return " AND ".join(conditions), params


def _generate_excel_sync(
    *,
    sync_db_url: str,
    scope: ReportScope,
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    batch_size: int = DEFAULT_CURSOR_BATCH_SIZE,
) -> GeneratedReportResult:
    """
    在背景執行緒中以 PostgreSQL Server-Side Cursor + xlsxwriter(constant_memory=True)
    將報表寫入磁碟暫存檔，確保即使 50 萬筆明細記憶體仍維持在數十 MB 內。
    """
    where_sql, params = _build_where_clause(scope, date_from, date_to)

    fd, tmp_path = tempfile.mkstemp(prefix="flash_ticket_report_", suffix=".xlsx")
    os.close(fd)

    conn = psycopg2.connect(sync_db_url)
    try:
        # 1. 摘要 SQL 聚合（訂單面）
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(o.id) AS total_orders,
                    COUNT(o.id) FILTER (WHERE o.status = 'paid') AS paid_orders,
                    COUNT(o.id) FILTER (WHERE o.status = 'pending') AS pending_orders,
                    COUNT(o.id) FILTER (WHERE o.status = 'cancelled') AS cancelled_orders,
                    COUNT(o.id) FILTER (WHERE o.status = 'cancelled_refund_required') AS refund_orders,
                    COALESCE(SUM(o.total_ticket_amount) FILTER (WHERE o.status = 'paid'), 0) AS paid_ticket_sum,
                    COALESCE(SUM(o.service_fee) FILTER (WHERE o.status = 'paid'), 0) AS paid_fee_sum,
                    COALESCE(SUM(o.total_amount) FILTER (WHERE o.status = 'paid'), 0) AS paid_total_sum,
                    COALESCE(SUM(o.total_amount), 0) AS all_orders_total_sum
                FROM orders o
                JOIN events e ON e.id = o.event_id
                WHERE {where_sql}
                """,
                params,
            )
            summary_row = cur.fetchone()

            # 2. 區域彙總 SQL 聚合（GROUP BY event_id, zone_code）
            cur.execute(
                f"""
                SELECT
                    e.id AS event_id,
                    e.title AS event_title,
                    s.zone_code AS zone_code,
                    z.name AS zone_name,
                    z.price AS unit_price,
                    COUNT(oi.id) FILTER (WHERE oi.status = 'paid') AS paid_tickets,
                    COUNT(oi.id) FILTER (WHERE oi.status = 'held') AS held_tickets,
                    COUNT(oi.id) FILTER (WHERE oi.status = 'cancelled') AS cancelled_tickets,
                    COALESCE(SUM(oi.price) FILTER (WHERE oi.status = 'paid'), 0) AS paid_revenue
                FROM order_items oi
                JOIN orders o ON o.id = oi.order_id
                JOIN events e ON e.id = o.event_id
                JOIN seats s ON s.id = oi.seat_id
                JOIN zones z ON z.event_id = s.event_id AND z.zone_code = s.zone_code
                WHERE {where_sql}
                GROUP BY e.id, e.title, s.zone_code, z.name, z.price
                ORDER BY e.id ASC, s.zone_code ASC
                """,
                params,
            )
            zone_rows = cur.fetchall()

        # 3. 建立真正磁碟備援的 constant_memory Workbook（不傳 BytesIO / 不設 in_memory=True）
        workbook = xlsxwriter.Workbook(
            tmp_path,
            {
                "constant_memory": True,
                "strings_to_urls": False,
            },
        )
        try:
            header_fmt = workbook.add_format({"bold": True, "bg_color": "#E8EDF5", "border": 1})
            title_fmt = workbook.add_format({"bold": True, "font_size": 12})
            money_fmt = workbook.add_format({"num_format": "#,##0.00"})
            int_fmt = workbook.add_format({"num_format": "#,##0"})

            # --- Sheet 1: Summary（銷售摘要 + 區域彙總） ---
            ws_summary = workbook.add_worksheet("Summary")
            now_taipei = _format_taipei(datetime.now(timezone.utc))

            ws_summary.write(0, 0, "Flash Ticket 銷售摘要與區域彙總報表", title_fmt)
            ws_summary.write(1, 0, "報表產出時間 (Asia/Taipei)")
            ws_summary.write(1, 1, now_taipei)
            ws_summary.write(2, 0, "時區基準")
            ws_summary.write(2, 1, "Asia/Taipei (UTC+08:00)")
            ws_summary.write(3, 0, "篩選活動")
            ws_summary.write(
                3,
                1,
                f"#{scope.event_id} {scope.event_title}" if scope.event_id else "全部授權活動",
            )

            total_orders = int(summary_row[0] or 0)
            paid_orders = int(summary_row[1] or 0)
            pending_orders = int(summary_row[2] or 0)
            cancelled_orders = int(summary_row[3] or 0)
            refund_orders = int(summary_row[4] or 0)
            paid_ticket_sum = Decimal(str(summary_row[5] or 0))
            paid_fee_sum = Decimal(str(summary_row[6] or 0))
            paid_total_sum = Decimal(str(summary_row[7] or 0))
            all_orders_total_sum = Decimal(str(summary_row[8] or 0))

            ws_summary.write(5, 0, "指標名稱", header_fmt)
            ws_summary.write(5, 1, "數值", header_fmt)
            kpi_int_rows = [
                ("總訂單數", total_orders),
                ("已付款訂單數 (paid)", paid_orders),
                ("待付款訂單數 (pending)", pending_orders),
                ("已取消訂單數 (cancelled)", cancelled_orders),
                ("需退款訂單數 (cancelled_refund_required)", refund_orders),
            ]
            row_idx = 6
            for label, val in kpi_int_rows:
                ws_summary.write(row_idx, 0, label)
                ws_summary.write_number(row_idx, 1, val, int_fmt)
                row_idx += 1

            kpi_money_rows = [
                ("已付款票價總額 (NT$)", paid_ticket_sum),
                ("已付款 5% 服務費總額 (NT$)", paid_fee_sum),
                ("已付款實收總額 (NT$)", paid_total_sum),
                ("全訂單金額合計 (NT$)", all_orders_total_sum),
            ]
            for label, val in kpi_money_rows:
                ws_summary.write(row_idx, 0, label)
                ws_summary.write_number(row_idx, 1, float(val), money_fmt)
                row_idx += 1

            row_idx += 1
            ws_summary.write(row_idx, 0, "分區銷售彙總 (Zone Aggregation)", title_fmt)
            row_idx += 1
            zone_headers = [
                "Event ID",
                "Event Title",
                "Zone Code",
                "Zone Name",
                "Unit Price (NT$)",
                "Paid Tickets",
                "Held Tickets",
                "Cancelled Tickets",
                "Paid Revenue (NT$)",
            ]
            for col_i, h_text in enumerate(zone_headers):
                ws_summary.write(row_idx, col_i, h_text, header_fmt)
            row_idx += 1

            for z_row in zone_rows:
                ws_summary.write_number(row_idx, 0, int(z_row[0]), int_fmt)
                ws_summary.write(row_idx, 1, str(z_row[1]))
                ws_summary.write(row_idx, 2, str(z_row[2]))
                ws_summary.write(row_idx, 3, str(z_row[3]))
                ws_summary.write_number(row_idx, 4, float(Decimal(str(z_row[4]))), money_fmt)
                ws_summary.write_number(row_idx, 5, int(z_row[5] or 0), int_fmt)
                ws_summary.write_number(row_idx, 6, int(z_row[6] or 0), int_fmt)
                ws_summary.write_number(row_idx, 7, int(z_row[7] or 0), int_fmt)
                ws_summary.write_number(row_idx, 8, float(Decimal(str(z_row[8] or 0))), money_fmt)
                row_idx += 1

            # --- Sheet 2: Details（Server-Side Cursor 分批串流明細） ---
            ws_details = workbook.add_worksheet("Details")
            detail_headers = [
                "Order ID",
                "Item ID",
                "Event ID",
                "Event Title",
                "Buyer Email",
                "Zone Code",
                "Row",
                "Seat Number",
                "Order Status",
                "Item Status",
                "Seat Price (NT$)",
                "Order Ticket Amount (NT$)",
                "Service Fee 5% (NT$)",
                "Order Total Amount (NT$)",
                "Created At (Asia/Taipei)",
                "Expires At (Asia/Taipei)",
            ]
            for col_i, h_text in enumerate(detail_headers):
                ws_details.write(0, col_i, h_text, header_fmt)

            detail_count = 0
            detail_sql = f"""
                SELECT
                    o.id AS order_id,
                    oi.id AS item_id,
                    o.event_id AS event_id,
                    e.title AS event_title,
                    u.email AS buyer_email,
                    s.zone_code AS zone_code,
                    s.row_label AS row_label,
                    s.seat_number AS seat_number,
                    o.status AS order_status,
                    oi.status AS item_status,
                    oi.price AS seat_price,
                    o.total_ticket_amount AS order_ticket_amount,
                    o.service_fee AS service_fee,
                    o.total_amount AS order_total_amount,
                    o.created_at AS created_at,
                    o.expires_at AS expires_at
                FROM order_items oi
                JOIN orders o ON o.id = oi.order_id
                JOIN events e ON e.id = o.event_id
                JOIN users u ON u.id = o.user_id
                JOIN seats s ON s.id = oi.seat_id
                WHERE {where_sql}
                ORDER BY o.id ASC, oi.id ASC
            """
            # 使用 PostgreSQL 伺服器端命名游標分批讀取（每批 batch_size 筆）
            with conn.cursor(name="report_details_server_cursor") as server_cur:
                server_cur.itersize = int(batch_size)
                server_cur.execute(detail_sql, params)
                excel_row = 1
                while True:
                    chunk = server_cur.fetchmany(int(batch_size))
                    if not chunk:
                        break
                    for rec in chunk:
                        ws_details.write_number(excel_row, 0, int(rec[0]), int_fmt)
                        ws_details.write_number(excel_row, 1, int(rec[1]), int_fmt)
                        ws_details.write_number(excel_row, 2, int(rec[2]), int_fmt)
                        ws_details.write(excel_row, 3, str(rec[3]))
                        ws_details.write(excel_row, 4, str(rec[4]))
                        ws_details.write(excel_row, 5, str(rec[5]))
                        ws_details.write(excel_row, 6, str(rec[6]))
                        ws_details.write(excel_row, 7, str(rec[7]))
                        ws_details.write(excel_row, 8, str(rec[8]))
                        ws_details.write(excel_row, 9, str(rec[9]))
                        ws_details.write_number(excel_row, 10, float(Decimal(str(rec[10]))), money_fmt)
                        ws_details.write_number(excel_row, 11, float(Decimal(str(rec[11]))), money_fmt)
                        ws_details.write_number(excel_row, 12, float(Decimal(str(rec[12]))), money_fmt)
                        ws_details.write_number(excel_row, 13, float(Decimal(str(rec[13]))), money_fmt)
                        ws_details.write(excel_row, 14, _format_taipei(rec[14]))
                        ws_details.write(excel_row, 15, _format_taipei(rec[15]))
                        excel_row += 1
                        detail_count += 1
        finally:
            workbook.close()

        ts_label = datetime.now(TAIPEI_TZ).strftime("%Y%m%d_%H%M%S")
        suffix = f"event_{scope.event_id}" if scope.event_id else "all_events"
        filename = f"sales_report_{suffix}_{ts_label}.xlsx"

        return GeneratedReportResult(
            file_path=tmp_path,
            filename=filename,
            detail_rows=detail_count,
            total_orders=total_orders,
            zone_rows=len(zone_rows),
        )
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise
    finally:
        conn.close()


async def generate_sales_report_xlsx(
    *,
    scope: ReportScope,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    batch_size: int = DEFAULT_CURSOR_BATCH_SIZE,
    sync_db_url: Optional[str] = None,
) -> GeneratedReportResult:
    """於背景執行緒非阻塞產生低記憶體 Excel 暫存檔"""
    return await asyncio.to_thread(
        _generate_excel_sync,
        sync_db_url=sync_db_url or settings.SYNC_DATABASE_URL,
        scope=scope,
        date_from=date_from,
        date_to=date_to,
        batch_size=batch_size,
    )


async def stream_and_cleanup_file(
    file_path: str,
    chunk_size: int = STREAM_CHUNK_SIZE,
) -> AsyncIterator[bytes]:
    """以固定 64KB chunk 串流讀取磁碟 Excel 暫存檔，串流結束或中斷後自動清理暫存檔"""
    try:
        with open(file_path, "rb") as fh:
            while True:
                data = fh.read(chunk_size)
                if not data:
                    break
                yield data
    finally:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass
