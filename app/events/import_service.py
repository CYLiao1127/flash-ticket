"""大量座位批量匯入服務

支援功能：
1. CSV / JSON 格式解析與 20MB / 100,000 列上限檢核
2. (event_id, file_sha256) 檔案指紋去重，重複上傳直接回傳既有 Job
3. 雙模式驗證：all_or_nothing（任一錯誤整批取消）與 skip_invalid（跳過錯誤列並記錄原始列號與原因）
4. PostgreSQL COPY 分批寫入、Redis Pub/Sub 進度推播與失敗／重啟卡死任務恢復
"""

import csv
import hashlib
import io
import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Awaitable, Callable, Dict, List, Optional, Set, Tuple

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.redis import get_redis
from app.events.service import _assert_can_manage_event, get_event_by_id
from app.models import (
    EventStatus,
    ImportJob,
    ImportJobMode,
    ImportJobStatus,
    Seat,
    SeatStatus,
    User,
    Zone,
)

logger = logging.getLogger("flash_ticket.import")

MAX_IMPORT_FILE_BYTES = 20 * 1024 * 1024  # 20 MB
MAX_IMPORT_ROWS = 100_000
COPY_BATCH_SIZE = 10_000


@dataclass(frozen=True)
class ParsedSeatRow:
    row_num: int
    zone_code: str
    row_label: str
    seat_number: str
    parse_error: Optional[str] = None


def serialize_import_progress(job: ImportJob) -> Dict[str, object]:
    """序列化匯入任務進度快照供 Redis Pub/Sub 與 SSE 推播使用"""
    return {
        "job_id": job.id,
        "event_id": job.event_id,
        "status": job.status,
        "total_rows": job.total_rows,
        "imported_rows": job.imported_rows,
        "error_rows": job.error_rows,
    }


async def _publish_job_progress(job: ImportJob) -> None:
    """發布匯入進度至 Redis Pub/Sub 頻道（盡力通知，不因推播異常中斷匯入流程）"""
    try:
        redis = get_redis()
        channel = f"import_job:{job.id}:progress"
        await redis.publish(channel, json.dumps(serialize_import_progress(job)))
    except Exception as exc:
        logger.warning(f"Failed to publish progress for job_id={job.id}: {exc}")


async def _mark_job_failed(
    session_factory: async_sessionmaker[AsyncSession],
    job_id: int,
    errors: List[Dict[str, object]],
) -> None:
    """使用全新 Session 將發生未預期錯誤的匯入任務標記為 FAILED，避免卡在 processing"""
    async with session_factory() as fail_session:
        job = (
            await fail_session.execute(
                select(ImportJob).where(ImportJob.id == job_id)
            )
        ).scalar_one_or_none()
        if job is None:
            return
        job.status = ImportJobStatus.FAILED.value
        job.imported_rows = 0
        job.errors = errors
        job.error_rows = len(errors)
        job.updated_at = func.now()
        await fail_session.commit()
        await fail_session.refresh(job)
        await _publish_job_progress(job)


def _detect_format(filename: str, content_type: Optional[str]) -> str:
    lower_name = (filename or "").lower().strip()
    if lower_name.endswith(".csv"):
        return "csv"
    if lower_name.endswith(".json"):
        return "json"
    if content_type:
        lower_ct = content_type.lower()
        if "csv" in lower_ct:
            return "csv"
        if "json" in lower_ct:
            return "json"
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Unsupported file format. Only CSV and JSON are supported",
    )


def _extract_json_field(obj: Dict, key: str) -> Tuple[str, Optional[str]]:
    if key not in obj or obj[key] is None:
        return "", f"Missing required field '{key}'"
    val = obj[key]
    if isinstance(val, bool) or not isinstance(val, (str, int)):
        return "", f"Field '{key}' must be a string or integer"
    return str(val).strip(), None


def parse_seat_file(
    raw_bytes: bytes,
    filename: str,
    content_type: Optional[str] = None,
) -> List[ParsedSeatRow]:
    """解析 CSV 或 JSON 座位檔案並檢核格式與 100,000 列上限"""
    if not raw_bytes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Import file is empty",
        )
    if len(raw_bytes) > MAX_IMPORT_FILE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File size exceeds maximum limit of {MAX_IMPORT_FILE_BYTES} bytes (20MB)",
        )

    fmt = _detect_format(filename, content_type)
    try:
        text_content = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be UTF-8 encoded",
        )

    parsed_rows: List[ParsedSeatRow] = []

    if fmt == "csv":
        reader = csv.reader(io.StringIO(text_content))
        try:
            header = next(reader)
        except StopIteration:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="CSV file is empty",
            )
        normalized_header = [col.strip() for col in header]
        if normalized_header != ["zone_code", "row_label", "seat_number"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="CSV header must be: zone_code,row_label,seat_number",
            )
        for row_num, cols in enumerate(reader, start=2):
            if not cols or (len(cols) == 1 and not cols[0].strip()):
                parsed_rows.append(
                    ParsedSeatRow(row_num, "", "", "", "Empty row is not allowed")
                )
                continue
            if len(cols) != 3:
                parsed_rows.append(
                    ParsedSeatRow(
                        row_num, "", "", "", f"Expected 3 columns, got {len(cols)}"
                    )
                )
                continue
            parsed_rows.append(
                ParsedSeatRow(
                    row_num=row_num,
                    zone_code=cols[0].strip(),
                    row_label=cols[1].strip(),
                    seat_number=cols[2].strip(),
                )
            )
    else:
        try:
            data = json.loads(text_content)
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid JSON syntax: {e.msg}",
            )
        if isinstance(data, dict) and isinstance(data.get("seats"), list):
            items = data["seats"]
        elif isinstance(data, list):
            items = data
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="JSON payload must be an array of seat objects or {'seats': [...]}",
            )
        for row_num, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                parsed_rows.append(
                    ParsedSeatRow(row_num, "", "", "", "Row must be a JSON object")
                )
                continue
            z_code, z_err = _extract_json_field(item, "zone_code")
            r_label, r_err = _extract_json_field(item, "row_label")
            s_num, s_err = _extract_json_field(item, "seat_number")
            err = z_err or r_err or s_err
            parsed_rows.append(
                ParsedSeatRow(
                    row_num=row_num,
                    zone_code=z_code,
                    row_label=r_label,
                    seat_number=s_num,
                    parse_error=err,
                )
            )

    if len(parsed_rows) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Import file contains no seat rows",
        )
    if len(parsed_rows) > MAX_IMPORT_ROWS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Row count ({len(parsed_rows)}) exceeds maximum limit of {MAX_IMPORT_ROWS} rows",
        )

    return parsed_rows


async def _read_upload_bounded(file: UploadFile) -> bytes:
    chunks: List[bytes] = []
    total = 0
    while True:
        chunk = await file.read(1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_IMPORT_FILE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File size exceeds maximum limit of {MAX_IMPORT_FILE_BYTES} bytes (20MB)",
            )
        chunks.append(chunk)
    raw = b"".join(chunks)
    if not raw:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Import file is empty",
        )
    return raw


async def submit_seat_import(
    db: AsyncSession,
    event_id: int,
    current_user: User,
    file: UploadFile,
    mode: ImportJobMode,
) -> Tuple[ImportJob, bool, Optional[List[ParsedSeatRow]]]:
    """
    驗證權限、檔案上限與 SHA-256 去重後建立 ImportJob。
    回傳 (job, is_duplicate, parsed_rows)。
    """
    event = await get_event_by_id(db, event_id)
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event not found",
        )
    _assert_can_manage_event(event, current_user)

    raw_bytes = await _read_upload_bounded(file)
    file_sha256 = hashlib.sha256(raw_bytes).hexdigest()

    # 1. 同一活動上傳相同內容之檔案時，直接回傳既有 Job
    existing_stmt = select(ImportJob).where(
        ImportJob.event_id == event_id,
        ImportJob.file_sha256 == file_sha256,
    )
    existing_job = (await db.execute(existing_stmt)).scalar_one_or_none()
    if existing_job is not None:
        return existing_job, True, None

    # 2. 僅允許於活動 draft 草稿狀態匯入座位
    if event.status != EventStatus.DRAFT.value:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Seats can only be imported while the event is in draft status",
        )

    parsed_rows = parse_seat_file(
        raw_bytes,
        filename=file.filename or "",
        content_type=file.content_type,
    )

    new_job = ImportJob(
        event_id=event_id,
        user_id=current_user.id,
        file_sha256=file_sha256,
        filename=file.filename or "upload",
        total_rows=len(parsed_rows),
        imported_rows=0,
        error_rows=0,
        mode=mode.value,
        status=ImportJobStatus.PENDING.value,
        errors=[],
    )
    db.add(new_job)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        existing_job = (await db.execute(existing_stmt)).scalar_one()
        return existing_job, True, None

    await db.refresh(new_job)
    return new_job, False, parsed_rows


async def process_import_job(
    job_id: int,
    parsed_rows: List[ParsedSeatRow],
    session_factory: async_sessionmaker[AsyncSession],
    on_batch_progress: Optional[Callable[[int, int], Awaitable[None]]] = None,
) -> None:
    """
    背景執行整段座位驗證、PostgreSQL COPY 寫入與 Redis Pub/Sub 進度推播；
    任一階段發生例外皆確保任務轉換為 FAILED，避免停留在 processing。
    """
    errors: List[Dict[str, object]] = []
    try:
        async with session_factory() as progress_session:
            job = (
                await progress_session.execute(
                    select(ImportJob).where(ImportJob.id == job_id)
                )
            ).scalar_one_or_none()
            if job is None:
                return

            job.status = ImportJobStatus.PROCESSING.value
            job.updated_at = func.now()
            await progress_session.commit()
            await _publish_job_progress(job)

            # 載入活動合法區域與既有座位
            zone_rows = await progress_session.execute(
                select(Zone.zone_code).where(Zone.event_id == job.event_id)
            )
            valid_zones: Set[str] = {r[0] for r in zone_rows.all()}

            existing_seat_rows = await progress_session.execute(
                select(Seat.zone_code, Seat.row_label, Seat.seat_number).where(
                    Seat.event_id == job.event_id
                )
            )
            existing_seats: Set[Tuple[str, str, str]] = {
                (r[0], r[1], r[2]) for r in existing_seat_rows.all()
            }

            seen_in_file: Set[Tuple[str, str, str]] = set()
            valid_records: List[Tuple[int, str, str, str, str, int]] = []

            for row in parsed_rows:
                if row.parse_error:
                    errors.append({"row": row.row_num, "reason": row.parse_error})
                    continue
                if not row.zone_code or not row.row_label or not row.seat_number:
                    errors.append(
                        {
                            "row": row.row_num,
                            "reason": "zone_code, row_label, and seat_number must be non-empty",
                        }
                    )
                    continue
                if (
                    len(row.zone_code) > 50
                    or len(row.row_label) > 50
                    or len(row.seat_number) > 50
                ):
                    errors.append(
                        {
                            "row": row.row_num,
                            "reason": "Field length exceeds 50 characters",
                        }
                    )
                    continue
                if row.zone_code not in valid_zones:
                    errors.append(
                        {
                            "row": row.row_num,
                            "reason": f"Unknown zone_code: {row.zone_code}",
                        }
                    )
                    continue

                coord = (row.zone_code, row.row_label, row.seat_number)
                if coord in seen_in_file:
                    errors.append(
                        {
                            "row": row.row_num,
                            "reason": f"Duplicate seat in file: {row.zone_code}-{row.row_label}-{row.seat_number}",
                        }
                    )
                    continue
                if coord in existing_seats:
                    errors.append(
                        {
                            "row": row.row_num,
                            "reason": f"Seat already exists in event: {row.zone_code}-{row.row_label}-{row.seat_number}",
                        }
                    )
                    continue

                seen_in_file.add(coord)
                valid_records.append(
                    (
                        job.event_id,
                        row.zone_code,
                        row.row_label,
                        row.seat_number,
                        SeatStatus.AVAILABLE.value,
                        1,
                    )
                )

            if job.mode == ImportJobMode.ALL_OR_NOTHING.value and errors:
                job.status = ImportJobStatus.FAILED.value
                job.imported_rows = 0
                job.error_rows = len(errors)
                job.errors = errors
                job.updated_at = func.now()
                await progress_session.commit()
                await _publish_job_progress(job)
                return

            async with session_factory() as seat_session:
                try:
                    if valid_records:
                        sa_conn = await seat_session.connection()
                        raw_conn = await sa_conn.get_raw_connection()
                        asyncpg_conn = raw_conn.driver_connection

                        for offset in range(0, len(valid_records), COPY_BATCH_SIZE):
                            batch = valid_records[offset : offset + COPY_BATCH_SIZE]
                            await asyncpg_conn.copy_records_to_table(
                                "seats",
                                records=batch,
                                columns=[
                                    "event_id",
                                    "zone_code",
                                    "row_label",
                                    "seat_number",
                                    "status",
                                    "version",
                                ],
                            )
                            job.imported_rows = offset + len(batch)
                            job.error_rows = len(errors)
                            job.updated_at = func.now()
                            await progress_session.commit()
                            await _publish_job_progress(job)
                            if on_batch_progress is not None:
                                await on_batch_progress(job.imported_rows, job.total_rows)

                    await seat_session.commit()
                except Exception:
                    await seat_session.rollback()
                    raise

            job.status = ImportJobStatus.COMPLETED.value
            job.imported_rows = len(valid_records)
            job.error_rows = len(errors)
            job.errors = errors
            job.updated_at = func.now()
            await progress_session.commit()
            await _publish_job_progress(job)
    except Exception as exc:
        logger.error(
            f"Bulk seat import failed for job_id={job_id}: {exc}",
            exc_info=True,
        )
        failure_errors = list(errors)
        failure_errors.append({"row": 0, "reason": f"Import execution error: {exc}"})
        try:
            await _mark_job_failed(session_factory, job_id, failure_errors)
        except Exception as mark_exc:
            logger.error(
                f"Failed to mark import job_id={job_id} as FAILED: {mark_exc}",
                exc_info=True,
            )


async def recover_stale_import_jobs(
    session: AsyncSession,
    stale_seconds: int = 120,
) -> int:
    """
    回收因程序重啟或逾時而停留在 pending / processing 的過期匯入任務。
    使用 FOR UPDATE SKIP LOCKED 確保多 Worker 不重複處理，並基於資料庫時鐘判斷逾時。
    """
    stmt = (
        select(ImportJob)
        .where(
            ImportJob.status.in_(
                [ImportJobStatus.PENDING.value, ImportJobStatus.PROCESSING.value]
            ),
            ImportJob.updated_at <= func.now() - timedelta(seconds=stale_seconds),
        )
        .order_by(ImportJob.id.asc())
        .limit(50)
        .with_for_update(skip_locked=True)
    )
    jobs = list((await session.execute(stmt)).scalars().all())
    if not jobs:
        return 0

    for job in jobs:
        job.status = ImportJobStatus.FAILED.value
        job.imported_rows = 0
        existing_errors = list(job.errors or [])
        existing_errors.append(
            {
                "row": 0,
                "reason": "Import job interrupted by process restart or timeout",
            }
        )
        job.errors = existing_errors
        job.error_rows = len(existing_errors)
        job.updated_at = func.now()

    await session.commit()
    for job in jobs:
        await _publish_job_progress(job)
    return len(jobs)


async def list_import_jobs(
    db: AsyncSession,
    event_id: int,
    current_user: User,
) -> List[ImportJob]:
    """查詢活動所有座位匯入任務清單（僅建立者或 Admin）"""
    event = await get_event_by_id(db, event_id)
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event not found",
        )
    _assert_can_manage_event(event, current_user)

    res = await db.execute(
        select(ImportJob)
        .where(ImportJob.event_id == event_id)
        .order_by(ImportJob.id.desc())
    )
    return list(res.scalars().all())


async def get_import_job(
    db: AsyncSession,
    event_id: int,
    job_id: int,
    current_user: User,
) -> ImportJob:
    """查詢單一匯入任務進度與錯誤明細（僅建立者或 Admin）"""
    event = await get_event_by_id(db, event_id)
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event not found",
        )
    _assert_can_manage_event(event, current_user)

    res = await db.execute(
        select(ImportJob).where(
            ImportJob.event_id == event_id,
            ImportJob.id == job_id,
        )
    )
    job = res.scalar_one_or_none()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Import job not found",
        )
    return job
