from typing import Literal, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_current_user, require_roles
from app.db import get_db
from app.events.schemas import (
    EventCreateRequest,
    EventListResponse,
    EventResponse,
    EventSeatsSnapshotResponse,
    EventUpdateRequest,
)
from app.events.service import (
    create_event,
    delete_event,
    get_event_by_id,
    get_event_seats_snapshot,
    list_events_keyset,
    publish_event,
    update_event,
)
from app.models import EventStatus, SeatStatus, User, UserRole

router = APIRouter(prefix="/api/v1/events", tags=["Events"])


@router.post(
    "",
    response_model=EventResponse,
    status_code=status.HTTP_201_CREATED,
    summary="建立活動與票價區域",
)
async def create_event_endpoint(
    req: EventCreateRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限 Admin 與 User 建立活動（預設為 draft 草稿狀態）"""
    return await create_event(db, current_user, req)


@router.get(
    "",
    response_model=EventListResponse,
    summary="活動列表查詢 (Keyset 游標分頁)",
)
async def list_events_endpoint(
    limit: int = Query(20, ge=1, le=100, description="每頁筆數"),
    cursor: Optional[str] = Query(None, description="Keyset 分頁游標"),
    category: Optional[str] = Query(None, description="依分類篩選"),
    status_param: Optional[EventStatus] = Query(
        None, alias="status", description="依狀態篩選 (draft/published)"
    ),
    search: Optional[str] = Query(None, description="關鍵字搜尋標題或場地"),
    creator_id: Optional[int] = Query(None, description="依建立者篩選"),
    sort_by: Literal["created_at", "start_time", "title"] = Query(
        "created_at", description="排序欄位"
    ),
    sort_order: Literal["asc", "desc"] = Query("desc", description="排序方向"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """支援全角色檢視；採用 Keyset 分頁避免資料持續寫入時翻頁重複或遺漏"""
    items, next_cursor, has_more = await list_events_keyset(
        db,
        limit=limit,
        cursor=cursor,
        category=category,
        status_filter=status_param,
        search=search,
        creator_id=creator_id,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    return EventListResponse(
        items=[EventResponse.model_validate(item) for item in items],
        next_cursor=next_cursor,
        has_more=has_more,
    )


@router.get(
    "/{event_id}",
    response_model=EventResponse,
    summary="取得單一活動詳情",
)
async def get_event_endpoint(
    event_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """全角色皆可瀏覽活動詳情與分區票價"""
    event = await get_event_by_id(db, event_id)
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event not found",
        )
    return event


@router.patch(
    "/{event_id}",
    response_model=EventResponse,
    summary="更新活動資訊與區域 (樂觀鎖保護)",
)
async def update_event_endpoint(
    event_id: int,
    req: EventUpdateRequest,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限建立者或 Admin 更新；須攜帶當前 version，版本不符時回傳 409 Conflict"""
    return await update_event(db, event_id, current_user, req)


@router.delete(
    "/{event_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="刪除活動 (已有訂單禁止刪除)",
)
async def delete_event_endpoint(
    event_id: int,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限建立者或 Admin 刪除；若已有訂單關聯則拒絕並回傳 409 Conflict"""
    await delete_event(db, event_id, current_user)
    return None


@router.post(
    "/{event_id}/publish",
    response_model=EventResponse,
    summary="發布活動 (draft -> published)",
)
async def publish_event_endpoint(
    event_id: int,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限建立者或 Admin 發布活動；已發布者重複發布回傳 409 Conflict"""
    return await publish_event(db, event_id, current_user)


@router.get(
    "/{event_id}/seats",
    response_model=EventSeatsSnapshotResponse,
    summary="取得活動座位狀態快照",
)
async def get_event_seats_snapshot_endpoint(
    event_id: int,
    zone_code: Optional[str] = Query(None, description="依區域代碼篩選"),
    status_param: Optional[SeatStatus] = Query(
        None, alias="status", description="依座位狀態篩選 (available/held/sold)"
    ),
    after_id: Optional[int] = Query(None, ge=0, description="分頁起點座位 ID"),
    limit: int = Query(500, ge=1, le=5000, description="每次回傳最大座位筆數"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """全角色皆可讀取同一資料庫快照下的活動 last_seq、各分區座位統計與座位清單"""
    return await get_event_seats_snapshot(
        db,
        event_id,
        zone_code=zone_code,
        status_filter=status_param,
        after_id=after_id,
        limit=limit,
    )
