"""大數據低記憶體串流 Excel 銷售報表匯出 API 路由 (Commit 23)"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_current_user
from app.db import get_db
from app.models import User
from app.reports.service import (
    DEFAULT_CURSOR_BATCH_SIZE,
    generate_sales_report_xlsx,
    resolve_report_scope,
    stream_and_cleanup_file,
)

reports_router = APIRouter(prefix="/api/v1/reports", tags=["Reports"])


@reports_router.get(
    "/exports",
    summary="串流匯出活動銷售 Excel 報表（Summary 摘要與分區彙總 + Details 訂單明細）",
)
async def export_sales_report_endpoint(
    event_id: Optional[int] = Query(None, ge=1, description="選填：指定活動 ID"),
    date_from: Optional[datetime] = Query(
        None, description="選填：訂單建立起始時間 (ISO-8601，含時區)"
    ),
    date_to: Optional[datetime] = Query(
        None, description="選填：訂單建立結束時間 (ISO-8601，含時區)"
    ),
    batch_size: int = Query(
        DEFAULT_CURSOR_BATCH_SIZE,
        ge=100,
        le=10000,
        description="Server-side Cursor 每批讀取筆數",
    ),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    串流匯出 Excel 銷售報表（`xlsxwriter(constant_memory=True)` + PostgreSQL Server-Side Cursor）：
    - **權限控管**：User 僅限匯出自己建立的活動（存取他人活動回 403）；Admin / Viewer 可匯出全部活動。
    - **記憶體上限**：採磁碟暫存檔搭配 64KB chunk 串流輸出，50 萬筆訂單亦維持在 512MB 記憶體限制內。
    - **格式規範**：日期統一轉換為 `Asia/Taipei` 並標註於表頭，金額以 Excel 數值型別儲存。
    """
    if date_from is not None and date_to is not None and date_to < date_from:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="date_to must be greater than or equal to date_from",
        )

    scope = await resolve_report_scope(db, current_user, event_id=event_id)
    report = await generate_sales_report_xlsx(
        scope=scope,
        date_from=date_from,
        date_to=date_to,
        batch_size=batch_size,
    )

    headers = {
        "Content-Disposition": f'attachment; filename="{report.filename}"',
        "X-Report-Timezone": "Asia/Taipei",
        "X-Report-Orders": str(report.total_orders),
        "X-Report-Rows": str(report.detail_rows),
        "X-Report-Zones": str(report.zone_rows),
    }
    return StreamingResponse(
        stream_and_cleanup_file(report.file_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
