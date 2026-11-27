import base64
import binascii
import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, status
from sqlalchemy import BigInteger, Integer, String, bindparam, delete, func, or_, select, text, tuple_, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.events.schemas import (
    EventCreateRequest,
    EventSeatsSnapshotResponse,
    EventUpdateRequest,
    SeatSnapshotItem,
    ZoneSeatSummary,
)
from app.models import (
    Event,
    EventStatus,
    Order,
    Seat,
    SeatStatus,
    User,
    UserRole,
    Zone,
)

SORTABLE_COLUMNS = {
    "created_at": Event.created_at,
    "start_time": Event.start_time,
    "title": Event.title,
}


def encode_event_cursor(
    sort_by: str,
    sort_order: str,
    sort_val: Any,
    event_id: int,
) -> str:
    """將 Keyset 分頁游標（排序欄位值 + 主鍵 ID）編碼為 URL-safe Base64 字串"""
    serialized_val = (
        sort_val.isoformat() if isinstance(sort_val, datetime) else str(sort_val)
    )
    payload = {
        "sort_by": sort_by,
        "sort_order": sort_order,
        "val": serialized_val,
        "id": event_id,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_event_cursor(
    cursor: str,
    expected_sort_by: str,
    expected_sort_order: str,
) -> Tuple[Any, int]:
    """解碼並嚴格驗證 Keyset 分頁游標，避免跨排序條件誤用導致跳頁或錯誤"""
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Cursor payload must be a JSON object")
        if (
            data.get("sort_by") != expected_sort_by
            or data.get("sort_order") != expected_sort_order
        ):
            raise ValueError("Cursor sort parameters do not match query")
        event_id = data.get("id")
        if not isinstance(event_id, int) or isinstance(event_id, bool):
            raise ValueError("Cursor id must be an integer")
        raw_val = data.get("val")
        if raw_val is None:
            raise ValueError("Cursor value is missing")

        if expected_sort_by in ("created_at", "start_time"):
            parsed_val: Any = datetime.fromisoformat(str(raw_val))
            if parsed_val.tzinfo is None or parsed_val.tzinfo.utcoffset(parsed_val) is None:
                raise ValueError("Cursor datetime must include timezone")
        else:
            parsed_val = str(raw_val)
        return parsed_val, event_id
    except (ValueError, TypeError, json.JSONDecodeError, binascii.Error, UnicodeDecodeError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid pagination cursor",
        )


def _assert_can_manage_event(event: Event, current_user: User) -> None:
    """驗證操作者是否為 Admin 或活動建立者 (第四節權限規範)"""
    if current_user.role == UserRole.ADMIN.value:
        return
    if event.creator_id == current_user.id:
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Operation not permitted: Only the event creator or Admin can manage this event",
    )


async def get_event_by_id(db: AsyncSession, event_id: int) -> Optional[Event]:
    """依 ID 查詢活動與區域，使用 populate_existing 確保 Core UPDATE 後載入最新狀態"""
    stmt = (
        select(Event)
        .options(selectinload(Event.zones))
        .where(Event.id == event_id)
        .execution_options(populate_existing=True)
    )
    res = await db.execute(stmt)
    event = res.scalar_one_or_none()
    if event is not None:
        event.zones.sort(key=lambda z: z.id)
    return event


async def create_event(
    db: AsyncSession,
    current_user: User,
    req: EventCreateRequest,
) -> Event:
    """建立草稿活動與所屬區域票價設定"""
    event = Event(
        creator_id=current_user.id,
        title=req.title,
        description=req.description,
        category=req.category,
        venue=req.venue,
        start_time=req.start_time,
        end_time=req.end_time,
        status=EventStatus.DRAFT.value,
        last_seq=0,
        version=1,
    )
    for z in req.zones:
        event.zones.append(
            Zone(
                zone_code=z.zone_code,
                name=z.name,
                price=z.price,
            )
        )
    db.add(event)
    await db.commit()
    created = await get_event_by_id(db, event.id)
    assert created is not None
    return created


async def list_events_keyset(
    db: AsyncSession,
    *,
    limit: int = 20,
    cursor: Optional[str] = None,
    category: Optional[str] = None,
    status_filter: Optional[EventStatus] = None,
    search: Optional[str] = None,
    creator_id: Optional[int] = None,
    sort_by: str = "created_at",
    sort_order: str = "desc",
) -> Tuple[List[Event], Optional[str], bool]:
    """
    使用 Keyset 游標分頁查詢活動列表，以 (sort_col, id) 複合排序確保資料持續寫入時不重複、不遺漏
    """
    sort_col = SORTABLE_COLUMNS[sort_by]
    stmt = select(Event).options(selectinload(Event.zones))

    if category:
        stmt = stmt.where(Event.category == category)
    if status_filter is not None:
        stmt = stmt.where(Event.status == status_filter.value)
    if creator_id is not None:
        stmt = stmt.where(Event.creator_id == creator_id)
    if search:
        pattern = f"%{search.strip()}%"
        stmt = stmt.where(or_(Event.title.ilike(pattern), Event.venue.ilike(pattern)))

    if cursor:
        cursor_val, cursor_id = decode_event_cursor(cursor, sort_by, sort_order)
        if sort_order == "desc":
            stmt = stmt.where(tuple_(sort_col, Event.id) < (cursor_val, cursor_id))
        else:
            stmt = stmt.where(tuple_(sort_col, Event.id) > (cursor_val, cursor_id))

    if sort_order == "desc":
        stmt = stmt.order_by(sort_col.desc(), Event.id.desc())
    else:
        stmt = stmt.order_by(sort_col.asc(), Event.id.asc())

    stmt = stmt.limit(limit + 1)
    res = await db.execute(stmt)
    rows = list(res.scalars().all())

    has_more = len(rows) > limit
    items = rows[:limit]
    for ev in items:
        ev.zones.sort(key=lambda z: z.id)

    next_cursor: Optional[str] = None
    if has_more and items:
        last_item = items[-1]
        last_val = getattr(last_item, sort_by)
        next_cursor = encode_event_cursor(sort_by, sort_order, last_val, last_item.id)

    return items, next_cursor, has_more


async def update_event(
    db: AsyncSession,
    event_id: int,
    current_user: User,
    req: EventUpdateRequest,
) -> Event:
    """
    更新活動與區域設定，以資料庫條件式 UPDATE (WHERE version = :expected) 落實樂觀鎖防並行覆蓋
    """
    event = await get_event_by_id(db, event_id)
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event not found",
        )
    _assert_can_manage_event(event, current_user)

    effective_start = req.start_time if req.start_time is not None else event.start_time
    effective_end = req.end_time if req.end_time is not None else event.end_time
    if effective_end <= effective_start:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="end_time must be after start_time",
        )

    update_values: Dict[str, Any] = {}
    if req.title is not None:
        update_values["title"] = req.title
    if "description" in req.model_fields_set:
        update_values["description"] = req.description
    if req.category is not None:
        update_values["category"] = req.category
    if req.venue is not None:
        update_values["venue"] = req.venue
    if req.start_time is not None:
        update_values["start_time"] = req.start_time
    if req.end_time is not None:
        update_values["end_time"] = req.end_time

    # 原子性樂觀鎖條件更新
    update_stmt = (
        update(Event)
        .where(Event.id == event_id, Event.version == req.version)
        .values(
            **update_values,
            version=Event.version + 1,
            updated_at=func.now(),
        )
    )
    result = await db.execute(update_stmt)
    if result.rowcount == 0:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Event version conflict: resource was modified concurrently",
        )

    # 若請求中包含 zones，先對活動既有區域取得 FOR UPDATE 行鎖（與並發新增座位之 FOR KEY SHARE 互斥）
    if req.zones is not None:
        zone_res = await db.execute(
            select(Zone)
            .where(Zone.event_id == event_id)
            .order_by(Zone.id.asc())
            .with_for_update()
        )
        existing_zones = {z.zone_code: z for z in zone_res.scalars().all()}
        incoming_codes = {z.zone_code for z in req.zones}
        removed_codes = set(existing_zones.keys()) - incoming_codes

        if removed_codes:
            seat_exists_res = await db.execute(
                select(Seat.id)
                .where(Seat.event_id == event_id, Seat.zone_code.in_(removed_codes))
                .limit(1)
            )
            if seat_exists_res.scalar_one_or_none() is not None:
                await db.rollback()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Cannot remove zone with existing seats",
                )
            for code in removed_codes:
                await db.delete(existing_zones[code])

        for z_req in req.zones:
            if z_req.zone_code in existing_zones:
                existing_zone = existing_zones[z_req.zone_code]
                existing_zone.name = z_req.name
                existing_zone.price = z_req.price
            else:
                db.add(
                    Zone(
                        event_id=event_id,
                        zone_code=z_req.zone_code,
                        name=z_req.name,
                        price=z_req.price,
                    )
                )

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot remove zone with existing seats",
        )
    updated = await get_event_by_id(db, event_id)
    assert updated is not None
    return updated


async def publish_event(
    db: AsyncSession,
    event_id: int,
    current_user: User,
) -> Event:
    """將草稿活動轉換為已發布狀態 (draft -> published)"""
    event = await get_event_by_id(db, event_id)
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event not found",
        )
    _assert_can_manage_event(event, current_user)

    stmt = (
        update(Event)
        .where(Event.id == event_id, Event.status == EventStatus.DRAFT.value)
        .values(
            status=EventStatus.PUBLISHED.value,
            version=Event.version + 1,
            updated_at=func.now(),
        )
    )
    res = await db.execute(stmt)
    if res.rowcount == 0:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Event is already published",
        )

    await db.commit()
    published = await get_event_by_id(db, event_id)
    assert published is not None
    return published


async def delete_event(
    db: AsyncSession,
    event_id: int,
    current_user: User,
) -> None:
    """刪除活動；若已有訂單則禁止刪除並回傳 409 Conflict"""
    res = await db.execute(
        select(Event).where(Event.id == event_id).with_for_update()
    )
    event = res.scalar_one_or_none()
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event not found",
        )
    _assert_can_manage_event(event, current_user)

    order_exists = await db.execute(
        select(Order.id).where(Order.event_id == event_id).limit(1)
    )
    if order_exists.scalar_one_or_none() is not None:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot delete event with existing orders",
        )

    try:
        # 因 fk_seats_zones_event_zone 為 ON DELETE RESTRICT，須先刪座位再刪區域與活動
        await db.execute(delete(Seat).where(Seat.event_id == event_id))
        await db.execute(delete(Zone).where(Zone.event_id == event_id))
        await db.execute(delete(Event).where(Event.id == event_id))
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot delete event with existing orders",
        )


async def get_event_seats_snapshot(
    db: AsyncSession,
    event_id: int,
    *,
    zone_code: Optional[str] = None,
    status_filter: Optional[SeatStatus] = None,
    after_id: Optional[int] = None,
    limit: int = 500,
) -> EventSeatsSnapshotResponse:
    """
    以單一 PostgreSQL CTE 查詢同時取得活動 last_seq、分區座位統計與分頁座位清單，
    確保回傳欄位來自同一個資料庫 MVCC 快照。
    """
    snapshot_sql = text(
        """
        WITH target_event AS (
            SELECT id, status, last_seq
            FROM events
            WHERE id = :event_id
        ),
        seat_counts AS (
            SELECT
                zone_code,
                COUNT(*)::int AS total_seats,
                COUNT(*) FILTER (WHERE status = 'available')::int AS available_seats,
                COUNT(*) FILTER (WHERE status = 'held')::int AS held_seats,
                COUNT(*) FILTER (WHERE status = 'sold')::int AS sold_seats
            FROM seats
            WHERE event_id = :event_id
            GROUP BY zone_code
        ),
        zone_summary AS (
            SELECT
                z.id AS zone_id,
                z.zone_code,
                z.name,
                z.price::text AS price,
                COALESCE(sc.total_seats, 0) AS total_seats,
                COALESCE(sc.available_seats, 0) AS available_seats,
                COALESCE(sc.held_seats, 0) AS held_seats,
                COALESCE(sc.sold_seats, 0) AS sold_seats
            FROM zones z
            LEFT JOIN seat_counts sc ON sc.zone_code = z.zone_code
            WHERE z.event_id = :event_id
            ORDER BY z.id ASC
        ),
        filtered_seats AS (
            SELECT s.id, s.zone_code, s.row_label, s.seat_number, s.status, s.version
            FROM seats s
            WHERE s.event_id = :event_id
              AND (CAST(:zone_code AS VARCHAR) IS NULL OR s.zone_code = CAST(:zone_code AS VARCHAR))
              AND (CAST(:seat_status AS VARCHAR) IS NULL OR s.status = CAST(:seat_status AS VARCHAR))
              AND (CAST(:after_id AS BIGINT) IS NULL OR s.id > CAST(:after_id AS BIGINT))
            ORDER BY s.id ASC
            LIMIT :limit_plus_one
        )
        SELECT
            te.id AS event_id,
            te.status AS event_status,
            te.last_seq AS last_seq,
            COALESCE(
                (SELECT SUM(zs.total_seats)::int FROM zone_summary zs),
                0
            ) AS total_seats,
            COALESCE(
                (SELECT json_agg(
                    json_build_object(
                        'zone_code', zs.zone_code,
                        'name', zs.name,
                        'price', zs.price,
                        'total_seats', zs.total_seats,
                        'available_seats', zs.available_seats,
                        'held_seats', zs.held_seats,
                        'sold_seats', zs.sold_seats
                    )
                    ORDER BY zs.zone_id ASC
                ) FROM zone_summary zs),
                '[]'::json
            ) AS zones_json,
            COALESCE(
                (SELECT json_agg(
                    json_build_object(
                        'id', fs.id,
                        'zone_code', fs.zone_code,
                        'row_label', fs.row_label,
                        'seat_number', fs.seat_number,
                        'status', fs.status,
                        'version', fs.version
                    )
                    ORDER BY fs.id ASC
                ) FROM filtered_seats fs),
                '[]'::json
            ) AS seats_json
        FROM target_event te
        """
    ).bindparams(
        bindparam("event_id", type_=Integer),
        bindparam("zone_code", type_=String),
        bindparam("seat_status", type_=String),
        bindparam("after_id", type_=BigInteger),
        bindparam("limit_plus_one", type_=Integer),
    )

    res = await db.execute(
        snapshot_sql,
        {
            "event_id": event_id,
            "zone_code": zone_code,
            "seat_status": status_filter.value if status_filter is not None else None,
            "after_id": after_id,
            "limit_plus_one": limit + 1,
        },
    )
    row = res.mappings().one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event not found",
        )

    raw_zones = row["zones_json"]
    if isinstance(raw_zones, str):
        raw_zones = json.loads(raw_zones)
    zone_summaries = [
        ZoneSeatSummary(
            zone_code=z["zone_code"],
            name=z["name"],
            price=Decimal(str(z["price"])),
            total_seats=z["total_seats"],
            available_seats=z["available_seats"],
            held_seats=z["held_seats"],
            sold_seats=z["sold_seats"],
        )
        for z in raw_zones
    ]

    raw_seats = row["seats_json"]
    if isinstance(raw_seats, str):
        raw_seats = json.loads(raw_seats)
    has_more = len(raw_seats) > limit
    seat_items = [SeatSnapshotItem(**s) for s in raw_seats[:limit]]
    next_after_id = seat_items[-1].id if has_more and seat_items else None

    return EventSeatsSnapshotResponse(
        event_id=row["event_id"],
        status=row["event_status"],
        last_seq=row["last_seq"],
        total_seats=row["total_seats"],
        zones=zone_summaries,
        seats=seat_items,
        next_after_id=next_after_id,
        has_more=has_more,
    )
