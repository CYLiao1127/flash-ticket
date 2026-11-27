"""大量座位匯入測試：CSV/JSON、SHA-256 去重、雙模式驗證、上限檢查與 10 萬筆 COPY 效能驗證"""

import asyncio
import io
import json
import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple
import httpx
import pytest
from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.auth.seed import SEED_ACCOUNTS, seed_users
from app.config import settings
from app.core.redis import get_redis
from app.events.import_service import (
    MAX_IMPORT_FILE_BYTES,
    ParsedSeatRow,
    process_import_job,
    recover_stale_import_jobs,
)
from app.events.router import stream_import_job_progress_endpoint
from app.models import Event, ImportJob, ImportJobMode, ImportJobStatus, Seat, SeatStatus, User
from tests.conftest import TestSessionLocal


def _login(client, email: str, password: str) -> str:
    res = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert res.status_code == 200, res.text
    return res.json()["access_token"]


def _login_seed(client, role: str) -> str:
    acc = next(a for a in SEED_ACCOUNTS if a["role"].value == role)
    return _login(client, acc["email"], acc["password"])


def _create_draft_event(client, token: str, title: str = "Bulk Import Concert") -> int:
    now = datetime.now(timezone.utc)
    res = client.post(
        "/api/v1/events",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": title,
            "description": "Import target event",
            "category": "concert",
            "venue": "Taipei Arena",
            "start_time": (now + timedelta(days=5)).isoformat(),
            "end_time": (now + timedelta(days=5, hours=3)).isoformat(),
            "zones": [
                {"zone_code": "VIP", "name": "VIP 特區", "price": "5800.00"},
                {"zone_code": "A", "name": "看台 A 區", "price": "3200.00"},
                {"zone_code": "B", "name": "看台 B 區", "price": "1800.00"},
            ],
        },
    )
    assert res.status_code == 201, res.text
    return res.json()["id"]


@pytest.mark.asyncio
async def test_csv_and_json_import_and_sha256_deduplication(client, db_session):
    """驗證 CSV / JSON 匯入成功，且同內容檔案重複上傳時依 SHA-256 回傳既有 Job 不重複匯入"""
    seed_users()
    user_token = _login_seed(client, "user")
    event_id = _create_draft_event(client, user_token)

    csv_content = (
        "zone_code,row_label,seat_number\n"
        "VIP,1,1\n"
        "VIP,1,2\n"
        "A,2,1\n"
        "B,3,1\n"
    ).encode("utf-8")

    # 1. 初次上傳 CSV
    res1 = client.post(
        f"/api/v1/events/{event_id}/seats/import",
        headers={"Authorization": f"Bearer {user_token}"},
        files={"file": ("seats.csv", io.BytesIO(csv_content), "text/csv")},
    )
    assert res1.status_code == 202, res1.text
    job1_init = res1.json()
    assert job1_init["is_duplicate"] is False
    job1_id = job1_init["id"]

    job1_detail = client.get(
        f"/api/v1/events/{event_id}/imports/{job1_id}",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert job1_detail.status_code == 200
    d1 = job1_detail.json()
    assert d1["status"] == "completed"
    assert d1["total_rows"] == 4
    assert d1["imported_rows"] == 4
    assert d1["error_rows"] == 0

    # 2. 重複上傳完全相同內容之 CSV -> 必須回傳同一個 job_id 且 is_duplicate=True
    res_dup = client.post(
        f"/api/v1/events/{event_id}/seats/import",
        headers={"Authorization": f"Bearer {user_token}"},
        files={"file": ("seats_renamed.csv", io.BytesIO(csv_content), "text/csv")},
    )
    assert res_dup.status_code == 202
    dup_data = res_dup.json()
    assert dup_data["is_duplicate"] is True
    assert dup_data["id"] == job1_id
    assert dup_data["file_sha256"] == d1["file_sha256"]

    # 3. 上傳 JSON 格式新增 2 個座位
    json_bytes = json.dumps(
        [
            {"zone_code": "A", "row_label": "2", "seat_number": "2"},
            {"zone_code": "B", "row_label": "3", "seat_number": "2"},
        ]
    ).encode("utf-8")
    res_json = client.post(
        f"/api/v1/events/{event_id}/seats/import",
        headers={"Authorization": f"Bearer {user_token}"},
        files={"file": ("more_seats.json", io.BytesIO(json_bytes), "application/json")},
    )
    assert res_json.status_code == 202
    job2_id = res_json.json()["id"]
    assert job2_id != job1_id

    job2_detail = client.get(
        f"/api/v1/events/{event_id}/imports/{job2_id}",
        headers={"Authorization": f"Bearer {user_token}"},
    ).json()
    assert job2_detail["status"] == "completed"
    assert job2_detail["imported_rows"] == 2

    # 4. 確認列表 API 回傳兩筆任務，且資料庫總座位數恰好為 6（未因重複上傳增加）
    list_res = client.get(
        f"/api/v1/events/{event_id}/imports",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert list_res.status_code == 200
    assert len(list_res.json()) == 2

    seat_count = (
        await db_session.execute(
            select(func.count(Seat.id)).where(Seat.event_id == event_id)
        )
    ).scalar_one()
    assert seat_count == 6


@pytest.mark.asyncio
async def test_import_all_or_nothing_vs_skip_invalid_and_error_reporting(client, db_session):
    """驗證 all_or_nothing（全撤）與 skip_invalid（跳過錯列），並核對原始列號與錯誤原因"""
    seed_users()
    user_token = _login_seed(client, "user")
    event_id = _create_draft_event(client, user_token, title="Mixed Validity Event")

    # 預先在資料庫植入 1 個既有座位 VIP-1-1
    db_session.add(
        Seat(
            event_id=event_id,
            zone_code="VIP",
            row_label="1",
            seat_number="1",
            status=SeatStatus.AVAILABLE.value,
        )
    )
    await db_session.commit()

    # Row 1: Header
    # Row 2: VIP,1,2       (合法)
    # Row 3: VIP,1,2       (檔案內重複)
    # Row 4: VIP,1,1       (與既有 DB 座位重複)
    # Row 5: UNKNOWN,1,1   (不存在之 zone_code)
    # Row 6: A,2,1         (合法)
    bad_csv_a = (
        "zone_code,row_label,seat_number\n"
        "VIP,1,2\n"
        "VIP,1,2\n"
        "VIP,1,1\n"
        "UNKNOWN,1,1\n"
        "A,2,1\n"
    ).encode("utf-8")

    # 1. all_or_nothing（預設）-> 失敗、0 筆匯入、保留 3 筆原列號錯誤
    res_aon = client.post(
        f"/api/v1/events/{event_id}/seats/import",
        headers={"Authorization": f"Bearer {user_token}"},
        files={"file": ("aon.csv", io.BytesIO(bad_csv_a), "text/csv")},
    )
    assert res_aon.status_code == 202
    aon_id = res_aon.json()["id"]

    aon_detail = client.get(
        f"/api/v1/events/{event_id}/imports/{aon_id}",
        headers={"Authorization": f"Bearer {user_token}"},
    ).json()
    assert aon_detail["status"] == "failed"
    assert aon_detail["total_rows"] == 5
    assert aon_detail["imported_rows"] == 0
    assert aon_detail["error_rows"] == 3
    assert [e["row"] for e in aon_detail["errors"]] == [3, 4, 5]
    assert "Duplicate seat in file" in aon_detail["errors"][0]["reason"]
    assert "already exists" in aon_detail["errors"][1]["reason"]
    assert "Unknown zone_code" in aon_detail["errors"][2]["reason"]

    # DB 依然只有原本那 1 筆預植座位
    cnt_after_aon = (
        await db_session.execute(
            select(func.count(Seat.id)).where(Seat.event_id == event_id)
        )
    ).scalar_one()
    assert cnt_after_aon == 1

    # 2. skip_invalid -> 略過第 3,4,5 列，成功匯入第 2、6 列共 2 筆（略微調整空白以取得新 sha256）
    bad_csv_skip = (
        "zone_code,row_label,seat_number\n"
        "VIP,1,2\n"
        "VIP,1,2\n"
        "VIP,1,1\n"
        "UNKNOWN,1,1\n"
        "A,2,1 \n"
    ).encode("utf-8")
    res_skip = client.post(
        f"/api/v1/events/{event_id}/seats/import?mode=skip_invalid",
        headers={"Authorization": f"Bearer {user_token}"},
        files={"file": ("skip.csv", io.BytesIO(bad_csv_skip), "text/csv")},
    )
    assert res_skip.status_code == 202
    skip_id = res_skip.json()["id"]

    skip_detail = client.get(
        f"/api/v1/events/{event_id}/imports/{skip_id}",
        headers={"Authorization": f"Bearer {user_token}"},
    ).json()
    assert skip_detail["status"] == "completed"
    assert skip_detail["total_rows"] == 5
    assert skip_detail["imported_rows"] == 2
    assert skip_detail["error_rows"] == 3
    assert [e["row"] for e in skip_detail["errors"]] == [3, 4, 5]

    cnt_after_skip = (
        await db_session.execute(
            select(func.count(Seat.id)).where(Seat.event_id == event_id)
        )
    ).scalar_one()
    assert cnt_after_skip == 3


@pytest.mark.asyncio
async def test_import_validation_limits_and_permissions(client, db_session):
    """驗證權限保護、活動狀態限制、表頭驗證、100,000 列與 20MB 檔案上限"""
    seed_users()
    user_token = _login_seed(client, "user")
    viewer_token = _login_seed(client, "viewer")
    event_id = _create_draft_event(client, user_token, title="Validation Limits Event")

    valid_csv = b"zone_code,row_label,seat_number\nVIP,1,1\n"

    # 1. Viewer 與非建立者無權匯入或查詢任務 (403)
    assert (
        client.post(
            f"/api/v1/events/{event_id}/seats/import",
            headers={"Authorization": f"Bearer {viewer_token}"},
            files={"file": ("s.csv", io.BytesIO(valid_csv), "text/csv")},
        ).status_code
        == 403
    )
    assert (
        client.get(
            f"/api/v1/events/{event_id}/imports",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ).status_code
        == 403
    )

    # 2. CSV 表頭錯誤 -> 400
    assert (
        client.post(
            f"/api/v1/events/{event_id}/seats/import",
            headers={"Authorization": f"Bearer {user_token}"},
            files={"file": ("bad_hdr.csv", io.BytesIO(b"a,b,c\nVIP,1,1\n"), "text/csv")},
        ).status_code
        == 400
    )

    # 3. 超過 100,000 列 (100,001 列) -> 400
    lines = ["zone_code,row_label,seat_number"]
    lines.extend(f"A,{i // 100},{i % 100}" for i in range(100_001))
    over_rows_csv = "\n".join(lines).encode("utf-8")
    assert len(over_rows_csv) <= MAX_IMPORT_FILE_BYTES
    res_over_rows = client.post(
        f"/api/v1/events/{event_id}/seats/import",
        headers={"Authorization": f"Bearer {user_token}"},
        files={"file": ("over_rows.csv", io.BytesIO(over_rows_csv), "text/csv")},
    )
    assert res_over_rows.status_code == 400

    # 4. 超過 20MB -> 413
    oversized_bytes = b"zone_code,row_label,seat_number\n" + (b"x" * (MAX_IMPORT_FILE_BYTES + 128))
    res_oversized = client.post(
        f"/api/v1/events/{event_id}/seats/import",
        headers={"Authorization": f"Bearer {user_token}"},
        files={"file": ("too_big.csv", io.BytesIO(oversized_bytes), "text/csv")},
    )
    assert res_oversized.status_code == 413

    # 5. 活動已發布後不可匯入新座位 -> 409 Conflict
    assert (
        client.post(
            f"/api/v1/events/{event_id}/publish",
            headers={"Authorization": f"Bearer {user_token}"},
        ).status_code
        == 200
    )
    res_pub = client.post(
        f"/api/v1/events/{event_id}/seats/import",
        headers={"Authorization": f"Bearer {user_token}"},
        files={"file": ("after_pub.csv", io.BytesIO(valid_csv), "text/csv")},
    )
    assert res_pub.status_code == 409


@pytest.mark.asyncio
async def test_bulk_import_100k_rows_performance_and_progress_tracking(client, db_session):
    """
    驗證 PostgreSQL COPY 批次匯入效能 (100,000 列 < 60 秒) 與過程中進度分批推進
    """
    seed_users()
    user_token = _login_seed(client, "user")
    event_id_100k = _create_draft_event(client, user_token, title="100K Perf Event")

    # 1. 產生 100,000 列合法 CSV
    buf = io.StringIO()
    buf.write("zone_code,row_label,seat_number\n")
    for idx in range(100_000):
        zone = "VIP" if idx < 20_000 else ("A" if idx < 60_000 else "B")
        row_num = (idx // 100) + 1
        seat_num = (idx % 100) + 1
        buf.write(f"{zone},R{row_num},{seat_num}\n")
    csv_100k = buf.getvalue().encode("utf-8")

    t0 = time.perf_counter()
    res = client.post(
        f"/api/v1/events/{event_id_100k}/seats/import",
        headers={"Authorization": f"Bearer {user_token}"},
        files={"file": ("seats_100k.csv", io.BytesIO(csv_100k), "text/csv")},
    )
    elapsed_sec = time.perf_counter() - t0
    assert res.status_code == 202, res.text
    job_id = res.json()["id"]

    job_detail = client.get(
        f"/api/v1/events/{event_id_100k}/imports/{job_id}",
        headers={"Authorization": f"Bearer {user_token}"},
    ).json()
    assert job_detail["status"] == "completed"
    assert job_detail["total_rows"] == 100_000
    assert job_detail["imported_rows"] == 100_000
    assert job_detail["error_rows"] == 0
    assert elapsed_sec < 60.0, f"100k import took too long: {elapsed_sec:.2f}s"
    print(f"[BENCHMARK] 100,000 rows COPY import elapsed={elapsed_sec:.2f}s")

    db_count = (
        await db_session.execute(
            select(func.count(Seat.id)).where(Seat.event_id == event_id_100k)
        )
    ).scalar_one()
    assert db_count == 100_000

    # 2. 驗證分批進度回報：執行 25,000 列匯入，在每批之間由外部獨立 Session 觀測 processing 與累計筆數
    event_id_prog = _create_draft_event(client, user_token, title="Batch Progress Event")
    user_obj = (
        await db_session.execute(select(User).where(User.email == "user@flashticket.com"))
    ).scalar_one()
    prog_job = ImportJob(
        event_id=event_id_prog,
        user_id=user_obj.id,
        file_sha256="progress-test-sha256",
        filename="prog.csv",
        total_rows=25_000,
        imported_rows=0,
        error_rows=0,
        mode=ImportJobMode.ALL_OR_NOTHING.value,
        status=ImportJobStatus.PENDING.value,
        errors=[],
    )
    db_session.add(prog_job)
    await db_session.commit()
    prog_job_id = prog_job.id

    prog_rows: List[ParsedSeatRow] = [
        ParsedSeatRow(
            row_num=i + 2,
            zone_code="A",
            row_label=f"P{i // 100}",
            seat_number=str(i % 100),
        )
        for i in range(25_000)
    ]

    observed_progress: List[Tuple[str, int]] = []

    async def capture_progress(_imported: int, _total: int) -> None:
        async with TestSessionLocal() as observer_sess:
            snap = (
                await observer_sess.execute(
                    select(ImportJob).where(ImportJob.id == prog_job_id)
                )
            ).scalar_one()
            observed_progress.append((snap.status, snap.imported_rows))

    await process_import_job(
        prog_job_id, prog_rows, TestSessionLocal, on_batch_progress=capture_progress
    )

    assert observed_progress == [
        ("processing", 10_000),
        ("processing", 20_000),
        ("processing", 25_000),
    ]
    await db_session.refresh(prog_job)
    assert prog_job.status == "completed"
    assert prog_job.imported_rows == 25_000


@pytest.mark.asyncio
async def test_import_exception_handling_stale_recovery_and_sse_stream(client, db_session):
    """
    驗證三項關鍵可靠性與即時推播需求：
    1. COPY 前（例如查 Zone 時）發生異常，Job 必須收斂為 failed 而非卡在 processing
    2. 模擬程序重啟留下的過期 processing 任務，recover_stale_import_jobs 會自動標記為 failed
    3. Redis Pub/Sub 進度推播頻道與 GET .../imports/{job_id}/stream (SSE) 正確推送進度並保護權限
    """
    seed_users()
    user_token = _login_seed(client, "user")
    viewer_token = _login_seed(client, "viewer")
    event_id = _create_draft_event(client, user_token, title="Recovery & SSE Event")

    user_obj = (
        await db_session.execute(select(User).where(User.email == "user@flashticket.com"))
    ).scalar_one()

    # 1. Pre-COPY 異常捕捉測試：透過包裝 session 在查詢 Zone 時拋錯，驗證任務不會停在 processing
    fail_job = ImportJob(
        event_id=event_id,
        user_id=user_obj.id,
        file_sha256="pre-copy-fail-sha256",
        filename="fail_before_copy.csv",
        total_rows=1,
        imported_rows=0,
        error_rows=0,
        mode=ImportJobMode.ALL_OR_NOTHING.value,
        status=ImportJobStatus.PENDING.value,
        errors=[],
    )
    db_session.add(fail_job)
    await db_session.commit()
    fail_job_id = fail_job.id

    call_idx = {"n": 0}

    class BrokenZoneSessionWrapper:
        def __init__(self, inner):
            self._inner = inner
            self._exec_count = 0

        async def __aenter__(self):
            await self._inner.__aenter__()
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return await self._inner.__aexit__(exc_type, exc, tb)

        async def execute(self, stmt, *args, **kwargs):
            self._exec_count += 1
            # 第 1 次是查 ImportJob，第 2 次查 Zone 時模擬資料庫中斷
            if self._exec_count == 2:
                raise RuntimeError("Simulated DB error before COPY")
            return await self._inner.execute(stmt, *args, **kwargs)

        async def commit(self):
            return await self._inner.commit()

    def broken_on_first_factory():
        call_idx["n"] += 1
        real_sess = TestSessionLocal()
        if call_idx["n"] == 1:
            return BrokenZoneSessionWrapper(real_sess)
        return real_sess

    await process_import_job(
        fail_job_id,
        [ParsedSeatRow(2, "VIP", "1", "1")],
        broken_on_first_factory,  # type: ignore[arg-type]
    )
    await db_session.refresh(fail_job)
    assert fail_job.status == "failed"
    assert fail_job.imported_rows == 0
    assert "Simulated DB error before COPY" in fail_job.errors[0]["reason"]

    # 2. 遺留卡住任務恢復：建立一個 5 分鐘前更新但仍停在 processing 的任務
    stale_job = ImportJob(
        event_id=event_id,
        user_id=user_obj.id,
        file_sha256="stale-orphan-sha256",
        filename="orphan.csv",
        total_rows=50000,
        imported_rows=10000,
        error_rows=0,
        mode=ImportJobMode.ALL_OR_NOTHING.value,
        status=ImportJobStatus.PROCESSING.value,
        errors=[],
    )
    db_session.add(stale_job)
    await db_session.commit()
    stale_job_id = stale_job.id

    await db_session.execute(
        text(
            "UPDATE import_jobs SET updated_at = NOW() - INTERVAL '5 minutes' WHERE id = :jid"
        ),
        {"jid": stale_job_id},
    )
    await db_session.commit()

    recovered = await recover_stale_import_jobs(db_session, stale_seconds=60)
    assert recovered == 1
    await db_session.refresh(stale_job)
    assert stale_job.status == "failed"
    assert stale_job.imported_rows == 0
    assert "interrupted by process restart or timeout" in stale_job.errors[-1]["reason"]

    # 3. 驗證 Redis Pub/Sub 即時進度頻道收到推播訊息 + SSE /stream 端點回應
    sse_job = ImportJob(
        event_id=event_id,
        user_id=user_obj.id,
        file_sha256="sse-stream-sha256",
        filename="sse.csv",
        total_rows=2,
        imported_rows=0,
        error_rows=0,
        mode=ImportJobMode.ALL_OR_NOTHING.value,
        status=ImportJobStatus.PENDING.value,
        errors=[],
    )
    db_session.add(sse_job)
    await db_session.commit()
    sse_job_id = sse_job.id

    redis = get_redis()
    pubsub = redis.pubsub()
    channel = f"import_job:{sse_job_id}:progress"
    await pubsub.subscribe(channel)
    try:
        await pubsub.get_message(ignore_subscribe_messages=False, timeout=1.0)
        await process_import_job(
            sse_job_id,
            [
                ParsedSeatRow(2, "VIP", "9", "1"),
                ParsedSeatRow(3, "VIP", "9", "2"),
            ],
            TestSessionLocal,
        )
        received_statuses = []
        for _ in range(5):
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg is None:
                break
            received_statuses.append(json.loads(msg["data"])["status"])
        assert "processing" in received_statuses
        assert "completed" in received_statuses
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()

    # SSE 端點權限檢查 (Viewer -> 403) 與正常串流輸出 (200 text/event-stream)
    assert (
        client.get(
            f"/api/v1/events/{event_id}/imports/{sse_job_id}/stream",
            headers={"Authorization": f"Bearer {viewer_token}"},
        ).status_code
        == 403
    )

    sse_res = client.get(
        f"/api/v1/events/{event_id}/imports/{sse_job_id}/stream",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert sse_res.status_code == 200
    assert "text/event-stream" in sse_res.headers["content-type"]
    assert sse_res.headers.get("x-accel-buffering") == "no"
    assert "no-cache" in sse_res.headers.get("cache-control", "")
    data_lines = [
        line for line in sse_res.text.splitlines() if line.startswith("data: ")
    ]
    assert len(data_lines) >= 1
    first_frame = json.loads(data_lines[0][len("data: ") :])
    assert first_frame["job_id"] == sse_job_id
    assert first_frame["status"] == "completed"
    assert first_frame["imported_rows"] == 2

    # 3b. 多幀即時 SSE 串流測試：從 pending 開始訂閱，於串流開啟期間陸續發布 processing 與 completed
    live_job = ImportJob(
        event_id=event_id,
        user_id=user_obj.id,
        file_sha256="live-multiframe-sse-sha256",
        filename="live_sse.csv",
        total_rows=20_000,
        imported_rows=0,
        error_rows=0,
        mode=ImportJobMode.ALL_OR_NOTHING.value,
        status=ImportJobStatus.PENDING.value,
        errors=[],
    )
    db_session.add(live_job)
    await db_session.commit()
    live_job_id = live_job.id

    live_redis = get_redis()
    live_channel = f"import_job:{live_job_id}:progress"
    sse_resp = await stream_import_job_progress_endpoint(
        event_id=event_id,
        job_id=live_job_id,
        current_user=user_obj,
        db=db_session,
    )
    assert sse_resp.headers.get("x-accel-buffering") == "no"
    assert "no-cache" in sse_resp.headers.get("cache-control", "")

    def _parse_frame(chunk) -> dict:
        text_chunk = chunk.decode("utf-8") if isinstance(chunk, bytes) else str(chunk)
        for ln in text_chunk.splitlines():
            if ln.startswith("data: "):
                return json.loads(ln[len("data: ") :])
        raise AssertionError(f"Missing SSE data line in chunk: {text_chunk!r}")

    body_iter = sse_resp.body_iterator

    # 1. 在尚未發布任何更新前，先逐幀取得第一幀 pending（此時後端已完成 subscribe）
    f1 = _parse_frame(await body_iter.__anext__())
    t1 = time.monotonic()
    assert f1["status"] == "pending"
    assert f1["imported_rows"] == 0

    # 2. 發布 processing (10,000) 並在尚未發布 completed 前，立刻收到第 2 幀
    t_pub_proc = time.monotonic()
    assert t1 <= t_pub_proc
    await live_redis.publish(
        live_channel,
        json.dumps(
            {
                "job_id": live_job_id,
                "event_id": event_id,
                "status": "processing",
                "total_rows": 20_000,
                "imported_rows": 10_000,
                "error_rows": 0,
            }
        ),
    )
    f2 = _parse_frame(await body_iter.__anext__())
    t2 = time.monotonic()
    assert f2["status"] == "processing"
    assert f2["imported_rows"] == 10_000
    assert t_pub_proc <= t2

    # 3. 間隔 150ms 後再發布 completed (20,000)，確保第 2 幀早於 completed 發布時刻抵達
    await asyncio.sleep(0.15)
    t_pub_comp = time.monotonic()
    assert t2 < t_pub_comp
    await live_redis.publish(
        live_channel,
        json.dumps(
            {
                "job_id": live_job_id,
                "event_id": event_id,
                "status": "completed",
                "total_rows": 20_000,
                "imported_rows": 20_000,
                "error_rows": 0,
            }
        ),
    )
    f3 = _parse_frame(await body_iter.__anext__())
    t3 = time.monotonic()
    assert f3["status"] == "completed"
    assert f3["imported_rows"] == 20_000
    assert t_pub_comp <= t3

    with pytest.raises(StopAsyncIteration):
        await body_iter.__anext__()

    # 4. 長時間執行但持續有批次進度的 Job，每次進度提交必須刷新 updated_at，不可被誤判為過期回收
    active_long_job = ImportJob(
        event_id=event_id,
        user_id=user_obj.id,
        file_sha256="long-running-active-sha256",
        filename="long_active.csv",
        total_rows=25_000,
        imported_rows=0,
        error_rows=0,
        mode=ImportJobMode.ALL_OR_NOTHING.value,
        status=ImportJobStatus.PENDING.value,
        errors=[],
    )
    db_session.add(active_long_job)
    await db_session.commit()
    long_job_id = active_long_job.id

    # 模擬該任務 5 分鐘前就已建立
    await db_session.execute(
        text(
            "UPDATE import_jobs "
            "SET created_at = NOW() - INTERVAL '5 minutes', "
            "    updated_at = NOW() - INTERVAL '5 minutes' "
            "WHERE id = :jid"
        ),
        {"jid": long_job_id},
    )
    await db_session.commit()

    long_rows: List[ParsedSeatRow] = [
        ParsedSeatRow(
            row_num=i + 2,
            zone_code="B",
            row_label=f"L{i // 100}",
            seat_number=str(i % 100),
        )
        for i in range(25_000)
    ]

    recovery_checks: List[Tuple[int, str, bool]] = []

    async def check_not_reclaimed(imported: int, _total: int) -> None:
        if imported == 10_000:
            async with TestSessionLocal() as worker_sess:
                recovered_cnt = await recover_stale_import_jobs(
                    worker_sess, stale_seconds=60
                )
                snap = (
                    await worker_sess.execute(
                        select(ImportJob).where(ImportJob.id == long_job_id)
                    )
                ).scalar_one()
                recovery_checks.append(
                    (
                        recovered_cnt,
                        snap.status,
                        snap.updated_at > snap.created_at,
                    )
                )

    await process_import_job(
        long_job_id,
        long_rows,
        TestSessionLocal,
        on_batch_progress=check_not_reclaimed,
    )

    assert recovery_checks == [(0, "processing", True)]
    await db_session.refresh(active_long_job)
    assert active_long_job.status == "completed"
    assert active_long_job.imported_rows == 25_000


@pytest.mark.asyncio
async def test_sse_progress_stream_unbuffered_through_nginx():
    """
    經由 Docker Compose 真實 Nginx 反向代理 (http://nginx) 實測逐幀無緩衝 SSE 推播：
    確保在第 2 幀發出並於 300ms 後才發出第 3 幀的條件下，第 2 幀仍在第 3 幀發出前送達客戶端。
    """
    main_db_url = (
        f"postgresql+asyncpg://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@"
        f"{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/flash_ticket"
    )
    main_engine = create_async_engine(main_db_url, poolclass=NullPool)
    MainSessionLocal = async_sessionmaker(
        bind=main_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    created_event_id = None
    redis = get_redis()
    try:
        async with httpx.AsyncClient(base_url="http://nginx", timeout=10.0) as ng_client:
            login = await ng_client.post(
                "/api/v1/auth/login",
                json={"email": "user@flashticket.com", "password": "UserPass123!"},
            )
            assert login.status_code == 200, login.text
            token = login.json()["access_token"]
            uid = login.json()["user"]["id"]
            headers = {"Authorization": f"Bearer {token}"}

            now = datetime.now(timezone.utc)
            ev_res = await ng_client.post(
                "/api/v1/events",
                headers=headers,
                json={
                    "title": "Nginx Unbuffered SSE Verification",
                    "category": "concert",
                    "venue": "Taipei Arena",
                    "start_time": (now + timedelta(days=1)).isoformat(),
                    "end_time": (now + timedelta(days=1, hours=2)).isoformat(),
                    "zones": [{"zone_code": "A", "name": "A", "price": "1000.00"}],
                },
            )
            assert ev_res.status_code == 201, ev_res.text
            created_event_id = ev_res.json()["id"]

            async with MainSessionLocal() as sess:
                job = ImportJob(
                    event_id=created_event_id,
                    user_id=uid,
                    file_sha256=f"nginx-ci-sse-{int(time.time() * 1000)}",
                    filename="nginx_sse.csv",
                    total_rows=20_000,
                    imported_rows=0,
                    error_rows=0,
                    mode=ImportJobMode.ALL_OR_NOTHING.value,
                    status=ImportJobStatus.PENDING.value,
                    errors=[],
                )
                sess.add(job)
                await sess.commit()
                await sess.refresh(job)
                jid = job.id

            channel = f"import_job:{jid}:progress"
            async with ng_client.stream(
                "GET",
                f"/api/v1/events/{created_event_id}/imports/{jid}/stream",
                headers=headers,
            ) as resp:
                assert resp.status_code == 200
                assert "text/event-stream" in resp.headers.get("content-type", "")
                assert "no-cache" in resp.headers.get("cache-control", "")
                lines = resp.aiter_lines()

                async def next_frame():
                    async for ln in lines:
                        if ln.startswith("data: "):
                            return (
                                json.loads(ln[len("data: ") :]),
                                time.monotonic(),
                                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                            )
                    raise AssertionError("Nginx SSE stream ended before expected frame")

                f1, t1, u1 = await next_frame()
                assert f1["status"] == "pending"

                for _ in range(50):
                    ns = await redis.pubsub_numsub(channel)
                    if ns and ns[0][1] >= 1:
                        break
                    await asyncio.sleep(0.02)

                t_pub2 = time.monotonic()
                u_pub2 = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                assert t1 <= t_pub2
                await redis.publish(
                    channel,
                    json.dumps(
                        {
                            "job_id": jid,
                            "event_id": created_event_id,
                            "status": "processing",
                            "total_rows": 20_000,
                            "imported_rows": 10_000,
                            "error_rows": 0,
                        }
                    ),
                )
                f2, t2, u2 = await next_frame()
                assert f2["status"] == "processing" and f2["imported_rows"] == 10_000
                assert t_pub2 <= t2

                await asyncio.sleep(0.30)
                t_pub3 = time.monotonic()
                u_pub3 = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                # 關鍵證明：第 2 幀在第 3 幀發布前即已穿過 Nginx 送達客戶端
                assert t2 < t_pub3
                await redis.publish(
                    channel,
                    json.dumps(
                        {
                            "job_id": jid,
                            "event_id": created_event_id,
                            "status": "completed",
                            "total_rows": 20_000,
                            "imported_rows": 20_000,
                            "error_rows": 0,
                        }
                    ),
                )
                f3, t3, u3 = await next_frame()
                assert f3["status"] == "completed" and f3["imported_rows"] == 20_000
                assert t_pub3 <= t3

                print(
                    f"[NGINX_SSE] f1=pending@{u1} | "
                    f"f2=processing(pub={u_pub2}->arr={u2}) | "
                    f"f3=completed(pub={u_pub3}->arr={u3})"
                )
    finally:
        if created_event_id is not None:
            async with MainSessionLocal() as clean_sess:
                await clean_sess.execute(
                    delete(Event).where(Event.id == created_event_id)
                )
                await clean_sess.commit()
        await main_engine.dispose()


@pytest.mark.asyncio
async def test_import_job_cancelled_upon_user_deactivation_between_batches(client, db_session):
    """
    ★ 關鍵 Q5.2 驗證：使用者帳號遭停用 (is_active=False) 時：
    1. 進行中的匯入任務必須在下一批次開始前偵測停用並主動取消 (cancelled)、回滾未提交之 COPY 批次（不遺留半套座位），並於 SSE 推播 cancelled 後正常結束串流。
    2. 建立者既有 JWT 必須立即失效 (401)。
    3. 尚未開始執行的 pending 任務於停用當下立即轉為 cancelled，即使後續背景執行亦不寫入座位。
    """
    seed_users()
    admin_token = _login_seed(client, "admin")

    owner_email = "import_owner_q52@flashticket.com"
    owner_password = "Password123!"
    reg_res = client.post(
        "/api/v1/auth/register",
        json={"email": owner_email, "password": owner_password},
    )
    assert reg_res.status_code == 201, reg_res.text
    owner_token = _login(client, owner_email, owner_password)
    event_id = _create_draft_event(client, owner_token, title="Deactivation Cancel Event")

    owner_obj = (
        await db_session.execute(select(User).where(User.email == owner_email))
    ).scalar_one()
    admin_obj = (
        await db_session.execute(
            select(User).where(User.email == "admin@flashticket.com")
        )
    ).scalar_one()

    def _parse_frame(chunk) -> dict:
        text_chunk = chunk.decode("utf-8") if isinstance(chunk, bytes) else str(chunk)
        for ln in text_chunk.splitlines():
            if ln.startswith("data: "):
                return json.loads(ln[len("data: ") :])
        raise AssertionError(f"Missing SSE data line in chunk: {text_chunk!r}")

    # --- A. 進行中任務於第 1 批 (10,000 列) 完成後停用建立者帳號 ---
    job = ImportJob(
        event_id=event_id,
        user_id=owner_obj.id,
        file_sha256="cancel-mid-batch-sha256",
        filename="cancel_mid_batch.csv",
        total_rows=25_000,
        imported_rows=0,
        error_rows=0,
        mode=ImportJobMode.ALL_OR_NOTHING.value,
        status=ImportJobStatus.PENDING.value,
        errors=[],
    )
    db_session.add(job)
    await db_session.commit()
    job_id = job.id

    sse_resp = await stream_import_job_progress_endpoint(
        event_id=event_id,
        job_id=job_id,
        current_user=admin_obj,
        db=db_session,
    )
    body_iter = sse_resp.body_iterator
    f_init = _parse_frame(await body_iter.__anext__())
    assert f_init["status"] == "pending"

    rows_25k: List[ParsedSeatRow] = [
        ParsedSeatRow(
            row_num=i + 2,
            zone_code="VIP",
            row_label=f"C{i // 100}",
            seat_number=str(i % 100),
        )
        for i in range(25_000)
    ]

    batches_seen: List[int] = []

    async def deactivate_after_first_batch(imported: int, _total: int) -> None:
        batches_seen.append(imported)
        if imported == 10_000:
            deact_res = client.patch(
                f"/api/v1/admin/users/{owner_obj.id}/status",
                headers={"Authorization": f"Bearer {admin_token}"},
                json={"is_active": False},
            )
            assert deact_res.status_code == 200, deact_res.text
            assert deact_res.json()["is_active"] is False

            # 建立者既有 JWT 必須立即失效
            revoked_res = client.get(
                f"/api/v1/events/{event_id}/imports/{job_id}",
                headers={"Authorization": f"Bearer {owner_token}"},
            )
            assert revoked_res.status_code == 401

    await process_import_job(
        job_id,
        rows_25k,
        TestSessionLocal,
        on_batch_progress=deactivate_after_first_batch,
    )

    assert batches_seen == [10_000], f"Expected halt after first batch, got {batches_seen}"

    # 驗證 SSE 依序收到 processing (10,000) 與 cancelled (0) 並正常結束串流
    frames = []
    async for chunk in body_iter:
        frames.append(_parse_frame(chunk))
    statuses = [f["status"] for f in frames]
    assert "processing" in statuses
    assert statuses[-1] == "cancelled"
    assert frames[-1]["imported_rows"] == 0

    # 驗證 DB 與 Admin API 查詢 Job 狀態為 cancelled，且第 1 批座位已完全回滾
    job_res = client.get(
        f"/api/v1/events/{event_id}/imports/{job_id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert job_res.status_code == 200
    job_data = job_res.json()
    assert job_data["status"] == "cancelled"
    assert job_data["imported_rows"] == 0
    assert job_data["error_rows"] == 1
    assert "deactivated" in job_data["errors"][0]["reason"].lower()

    snap_res = client.get(
        f"/api/v1/events/{event_id}/seats",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert snap_res.status_code == 200
    snap_data = snap_res.json()
    assert snap_data["total_seats"] == 0
    assert snap_data["seats"] == []

    # --- B. 尚未開始執行的 pending 任務於使用者停用當下立即標記為 cancelled ---
    pending_email = "pending_owner_q52@flashticket.com"
    assert (
        client.post(
            "/api/v1/auth/register",
            json={"email": pending_email, "password": owner_password},
        ).status_code
        == 201
    )
    pending_token = _login(client, pending_email, owner_password)
    pending_event_id = _create_draft_event(
        client, pending_token, title="Pending Cancel Event"
    )
    pending_owner = (
        await db_session.execute(select(User).where(User.email == pending_email))
    ).scalar_one()

    pending_job = ImportJob(
        event_id=pending_event_id,
        user_id=pending_owner.id,
        file_sha256="cancel-pending-sha256",
        filename="cancel_pending.csv",
        total_rows=2,
        imported_rows=0,
        error_rows=0,
        mode=ImportJobMode.ALL_OR_NOTHING.value,
        status=ImportJobStatus.PENDING.value,
        errors=[],
    )
    db_session.add(pending_job)
    await db_session.commit()
    pending_job_id = pending_job.id

    pending_sse = await stream_import_job_progress_endpoint(
        event_id=pending_event_id,
        job_id=pending_job_id,
        current_user=admin_obj,
        db=db_session,
    )
    pending_iter = pending_sse.body_iterator
    assert _parse_frame(await pending_iter.__anext__())["status"] == "pending"

    deact_pending_res = client.patch(
        f"/api/v1/admin/users/{pending_owner.id}/status",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"is_active": False},
    )
    assert deact_pending_res.status_code == 200

    f_cancel = _parse_frame(await pending_iter.__anext__())
    assert f_cancel["status"] == "cancelled"
    assert f_cancel["imported_rows"] == 0
    with pytest.raises(StopAsyncIteration):
        await pending_iter.__anext__()

    # 即使背景排程隨後觸發 process_import_job，也不得執行匯入
    await process_import_job(
        pending_job_id,
        [ParsedSeatRow(2, "VIP", "1", "1"), ParsedSeatRow(3, "VIP", "1", "2")],
        TestSessionLocal,
    )
    await db_session.refresh(pending_job)
    assert pending_job.status == "cancelled"
    assert pending_job.imported_rows == 0

    pending_snap = client.get(
        f"/api/v1/events/{pending_event_id}/seats",
        headers={"Authorization": f"Bearer {admin_token}"},
    ).json()
    assert pending_snap["total_seats"] == 0


@pytest.mark.asyncio
async def test_import_commit_and_user_deactivation_serialized_by_row_lock(client, db_session):
    """
    ★ 關鍵 Q5.2 邊界競態驗證：最後一次檢查建立者啟用狀態與 COPY 交易提交必須以 PostgreSQL FOR SHARE 列鎖原子協調：
    - 情境 A（匯入交易先取得 FOR SHARE）：在 on_before_commit 期間發起停用交易，停用交易必須阻塞直到 tx.commit() 完成後才能提交，絕不出現「停用已成功回傳後，座位仍延遲提交」。
    - 情境 B（停用交易先鎖定 users 列但尚未提交）：匯入交易執行 SELECT ... FOR SHARE 時必須等待停用提交，讀到 is_active=False 後完整回滾 COPY 交易，寫入 0 筆座位並標記為 cancelled。
    """
    from app.auth.service import update_user_status

    seed_users()
    admin_token = _login_seed(client, "admin")
    password = "Password123!"

    # --- 情境 A：匯入交易先取得 FOR SHARE 鎖，停用必須等 tx.commit() 完成後才提交 ---
    email_a = "race_commit_first@flashticket.com"
    assert (
        client.post(
            "/api/v1/auth/register",
            json={"email": email_a, "password": password},
        ).status_code
        == 201
    )
    token_a = _login(client, email_a, password)
    event_a_id = _create_draft_event(client, token_a, title="Race Commit First Event")
    user_a = (
        await db_session.execute(select(User).where(User.email == email_a))
    ).scalar_one()

    job_a = ImportJob(
        event_id=event_a_id,
        user_id=user_a.id,
        file_sha256="race-commit-first-sha256",
        filename="race_a.csv",
        total_rows=5,
        imported_rows=0,
        error_rows=0,
        mode=ImportJobMode.ALL_OR_NOTHING.value,
        status=ImportJobStatus.PENDING.value,
        errors=[],
    )
    db_session.add(job_a)
    await db_session.commit()
    job_a_id = job_a.id

    rows_a = [
        ParsedSeatRow(row_num=i + 2, zone_code="VIP", row_label="A", seat_number=str(i + 1))
        for i in range(5)
    ]

    deactivate_task_a: asyncio.Task | None = None
    blocked_while_holding_lock = False

    async def deactivate_user_a_concurrently() -> None:
        async with TestSessionLocal() as deact_sess:
            target = (
                await deact_sess.execute(select(User).where(User.id == user_a.id))
            ).scalar_one()
            await update_user_status(deact_sess, get_redis(), target, is_active=False)

    async def hold_before_commit_a() -> None:
        nonlocal deactivate_task_a, blocked_while_holding_lock
        deactivate_task_a = asyncio.create_task(deactivate_user_a_concurrently())
        # 停用交易嘗試 UPDATE users 會撞上 tx 持有的 FOR SHARE 鎖，150ms 內絕不可提前完成
        await asyncio.sleep(0.15)
        blocked_while_holding_lock = not deactivate_task_a.done()

    await process_import_job(
        job_a_id,
        rows_a,
        TestSessionLocal,
        on_before_commit=hold_before_commit_a,
    )
    assert deactivate_task_a is not None
    assert blocked_while_holding_lock is True, (
        "Deactivation committed while import transaction still held FOR SHARE before tx.commit()"
    )
    await asyncio.wait_for(deactivate_task_a, timeout=5.0)

    await db_session.refresh(job_a)
    await db_session.refresh(user_a)
    assert job_a.status == "completed"
    assert job_a.imported_rows == 5
    assert user_a.is_active is False

    snap_a = client.get(
        f"/api/v1/events/{event_a_id}/seats",
        headers={"Authorization": f"Bearer {admin_token}"},
    ).json()
    assert snap_a["total_seats"] == 5

    # --- 情境 B：停用交易先鎖定 users 列並於 150ms 後提交，匯入交易等待後必讀到 False 並回滾 ---
    email_b = "race_deactivate_first@flashticket.com"
    assert (
        client.post(
            "/api/v1/auth/register",
            json={"email": email_b, "password": password},
        ).status_code
        == 201
    )
    token_b = _login(client, email_b, password)
    event_b_id = _create_draft_event(client, token_b, title="Race Deactivate First Event")
    user_b = (
        await db_session.execute(select(User).where(User.email == email_b))
    ).scalar_one()

    job_b = ImportJob(
        event_id=event_b_id,
        user_id=user_b.id,
        file_sha256="race-deactivate-first-sha256",
        filename="race_b.csv",
        total_rows=5,
        imported_rows=0,
        error_rows=0,
        mode=ImportJobMode.ALL_OR_NOTHING.value,
        status=ImportJobStatus.PENDING.value,
        errors=[],
    )
    db_session.add(job_b)
    await db_session.commit()
    job_b_id = job_b.id

    rows_b = [
        ParsedSeatRow(row_num=i + 2, zone_code="VIP", row_label="B", seat_number=str(i + 1))
        for i in range(5)
    ]

    delayed_commit_task: asyncio.Task | None = None
    on_before_commit_called = False

    async def start_deactivation_before_final_lock(_imported: int, _total: int) -> None:
        nonlocal delayed_commit_task
        lock_acquired = asyncio.Event()

        async def lock_then_commit_deactivation() -> None:
            async with TestSessionLocal() as deact_sess:
                # 先取得 users 列寫鎖（未提交），讓匯入交易的 FOR SHARE 必須等待本停用交易結果
                await deact_sess.execute(
                    text(
                        "UPDATE users SET is_active = FALSE, token_version = token_version + 1 "
                        "WHERE id = :uid"
                    ),
                    {"uid": user_b.id},
                )
                lock_acquired.set()
                await asyncio.sleep(0.15)
                await deact_sess.commit()

        delayed_commit_task = asyncio.create_task(lock_then_commit_deactivation())
        await lock_acquired.wait()

    async def should_never_reach_commit() -> None:
        nonlocal on_before_commit_called
        on_before_commit_called = True

    await process_import_job(
        job_b_id,
        rows_b,
        TestSessionLocal,
        on_batch_progress=start_deactivation_before_final_lock,
        on_before_commit=should_never_reach_commit,
    )
    assert delayed_commit_task is not None
    await asyncio.wait_for(delayed_commit_task, timeout=5.0)

    assert on_before_commit_called is False
    await db_session.refresh(job_b)
    assert job_b.status == "cancelled"
    assert job_b.imported_rows == 0
    assert job_b.error_rows == 1
    assert "deactivated" in job_b.errors[0]["reason"].lower()

    snap_b = client.get(
        f"/api/v1/events/{event_b_id}/seats",
        headers={"Authorization": f"Bearer {admin_token}"},
    ).json()
    assert snap_b["total_seats"] == 0
    assert snap_b["seats"] == []
