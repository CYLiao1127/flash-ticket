import json
from typing import List, Literal, Optional
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    HTTPException,
    Query,
    UploadFile,
    status,
)
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.auth.dependencies import get_current_user, require_roles
from app.core.redis import get_redis
from app.db import get_db
from app.events.import_service import (
    get_import_job,
    list_import_jobs,
    process_import_job,
    serialize_import_progress,
    submit_seat_import,
)
from app.events.schemas import (
    EventCreateRequest,
    EventListResponse,
    EventResponse,
    EventSeatsSnapshotResponse,
    EventUpdateRequest,
    ImportJobResponse,
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
from app.models import (
    EventStatus,
    ImportJob,
    ImportJobMode,
    ImportJobStatus,
    SeatStatus,
    User,
    UserRole,
)

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


@router.post(
    "/{event_id}/seats/import",
    response_model=ImportJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="批量匯入活動座位 (CSV / JSON，支援 SHA-256 去重與背景處理)",
)
async def import_seats_endpoint(
    event_id: int,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="CSV 或 JSON 座位檔案 (上限 20MB / 100,000 列)"),
    mode: ImportJobMode = Query(
        ImportJobMode.ALL_OR_NOTHING,
        description="匯入模式：all_or_nothing (預設) 或 skip_invalid",
    ),
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限活動建立者或 Admin 於草稿狀態匯入座位；重複上傳同內容檔案將回傳既有 Job"""
    job, is_dup, parsed_rows = await submit_seat_import(
        db, event_id, current_user, file, mode
    )
    if not is_dup and parsed_rows is not None:
        session_factory = async_sessionmaker(
            bind=db.bind,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )
        background_tasks.add_task(
            process_import_job, job.id, parsed_rows, session_factory
        )

    resp = ImportJobResponse.model_validate(job)
    resp.is_duplicate = is_dup
    return resp


@router.get(
    "/{event_id}/imports",
    response_model=List[ImportJobResponse],
    summary="查詢活動之所有座位匯入任務清單",
)
async def list_import_jobs_endpoint(
    event_id: int,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限活動建立者或 Admin 查詢匯入任務清單"""
    return await list_import_jobs(db, event_id, current_user)


@router.get(
    "/{event_id}/imports/{job_id}",
    response_model=ImportJobResponse,
    summary="查詢單一座位匯入任務進度與錯誤報告",
)
async def get_import_job_endpoint(
    event_id: int,
    job_id: int,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限活動建立者或 Admin 查詢匯入進度與原列號錯誤明細"""
    return await get_import_job(db, event_id, job_id, current_user)


@router.get(
    "/{event_id}/imports/{job_id}/stream",
    summary="SSE 即時推播單一座位匯入任務進度",
)
async def stream_import_job_progress_endpoint(
    event_id: int,
    job_id: int,
    current_user: User = Depends(require_roles(UserRole.ADMIN, UserRole.USER)),
    db: AsyncSession = Depends(get_db),
):
    """僅限活動建立者或 Admin 訂閱，透過 Server-Sent Events 即時推送批次匯入進度"""
    initial_job = await get_import_job(db, event_id, job_id, current_user)
    session_factory = async_sessionmaker(
        bind=db.bind,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async def event_stream():
        initial_payload = serialize_import_progress(initial_job)
        if initial_job.status in (
            ImportJobStatus.COMPLETED.value,
            ImportJobStatus.FAILED.value,
        ):
            yield f"data: {json.dumps(initial_payload)}\n\n"
            return

        redis = get_redis()
        pubsub = redis.pubsub()
        channel = f"import_job:{job_id}:progress"
        # 先訂閱再取快照與送出首幀，避免訂閱與快照間遺漏任何剛送出的推播
        await pubsub.subscribe(channel)
        try:
            async with session_factory() as snap_sess:
                fresh_job = (
                    await snap_sess.execute(
                        select(ImportJob).where(ImportJob.id == job_id)
                    )
                ).scalar_one_or_none()
            snap_payload = serialize_import_progress(fresh_job or initial_job)
            yield f"data: {json.dumps(snap_payload)}\n\n"
            if snap_payload["status"] in (
                ImportJobStatus.COMPLETED.value,
                ImportJobStatus.FAILED.value,
            ):
                return

            last_sent = snap_payload
            while True:
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if msg is not None:
                    data_str = msg["data"]
                    if isinstance(data_str, bytes):
                        data_str = data_str.decode("utf-8")
                    yield f"data: {data_str}\n\n"
                    parsed = json.loads(data_str)
                    last_sent = parsed
                    if parsed.get("status") in (
                        ImportJobStatus.COMPLETED.value,
                        ImportJobStatus.FAILED.value,
                    ):
                        break
                else:
                    async with session_factory() as poll_sess:
                        latest = (
                            await poll_sess.execute(
                                select(ImportJob).where(ImportJob.id == job_id)
                            )
                        ).scalar_one_or_none()
                        if latest is None:
                            break
                        current_payload = serialize_import_progress(latest)
                        if current_payload != last_sent:
                            yield f"data: {json.dumps(current_payload)}\n\n"
                            last_sent = current_payload
                        if latest.status in (
                            ImportJobStatus.COMPLETED.value,
                            ImportJobStatus.FAILED.value,
                        ):
                            break
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
