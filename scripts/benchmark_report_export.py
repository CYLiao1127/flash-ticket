"""50 萬筆串流 Excel 報表匯出記憶體上限基準量測腳本 (Commit 23 / 512MB RAM 驗證)

使用方式：
  docker compose exec -T api-1 python scripts/benchmark_report_export.py --rows 500000
"""

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import io
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import tracemalloc
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx
import psycopg2
from sqlalchemy import select

from app.auth.security import create_access_token
from app.config import settings
from app.db import SyncSessionLocal
from app.models import Event, EventStatus, User, Zone
from app.reports.service import ReportScope, _generate_excel_sync


def _read_cgroup_memory() -> dict[str, int | float | None]:
    """讀取容器 cgroup v2 memory.max / memory.current / memory.peak / anon / file / oom_kill"""
    def _read_int(path: str) -> int | None:
        try:
            raw = Path(path).read_text(encoding="utf-8").strip()
            return int(raw)
        except (OSError, ValueError):
            return None

    stat_map: dict[str, int] = {}
    try:
        for line in Path("/sys/fs/cgroup/memory.stat").read_text(encoding="utf-8").splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0] in ("anon", "file"):
                stat_map[parts[0]] = int(parts[1])
    except OSError:
        pass

    oom_kill: int | None = None
    try:
        for line in Path("/sys/fs/cgroup/memory.events").read_text(encoding="utf-8").splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0] == "oom_kill":
                oom_kill = int(parts[1])
    except OSError:
        pass

    mem_max = _read_int("/sys/fs/cgroup/memory.max")
    mem_cur = _read_int("/sys/fs/cgroup/memory.current")
    mem_peak = _read_int("/sys/fs/cgroup/memory.peak")
    mib = 1024 * 1024
    return {
        "memory_max_bytes": mem_max,
        "memory_max_mib": round(mem_max / mib, 2) if mem_max is not None else None,
        "current_mib": round(mem_cur / mib, 2) if mem_cur is not None else None,
        "peak_mib": round(mem_peak / mib, 2) if mem_peak is not None else None,
        "anon_mib": round(stat_map["anon"] / mib, 2) if "anon" in stat_map else None,
        "file_mib": round(stat_map["file"] / mib, 2) if "file" in stat_map else None,
        "oom_kill": oom_kill,
    }


def _count_sheet_rows_streaming(xlsx_path: str, sheet_member: str = "xl/worksheets/sheet2.xml") -> int:
    """以固定 64KB chunk 串流計算工作表 XML 內的 `<row ` 標籤數量，不載入整份 XML"""
    count = 0
    carry = b""
    needle = b"<row "
    with zipfile.ZipFile(xlsx_path, "r") as zf:
        with zf.open(sheet_member, "r") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                buf = carry + chunk
                count += buf.count(needle)
                carry = buf[-(len(needle) - 1):]
    return count


def _read_linux_rss_mb() -> tuple[float, float]:
    """讀取 /proc/self/status 的 VmRSS（當前）與 VmHWM（行程歷史峰值）"""
    rss_kb = 0.0
    hwm_kb = 0.0
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss_kb = float(line.split()[1])
                elif line.startswith("VmHWM:"):
                    hwm_kb = float(line.split()[1])
    except OSError:
        pass
    return rss_kb / 1024.0, hwm_kb / 1024.0


def run_benchmark(
    num_rows: int = 100000,
    batch_size: int = 1000,
    use_tracemalloc: bool = False,
    via_http: bool = False,
    base_url: str = "http://127.0.0.1:8000",
) -> dict:
    now = datetime.now(timezone.utc)
    with SyncSessionLocal() as db:
        admin = db.execute(
            select(User).where(User.email == "admin@flashticket.com")
        ).scalar_one()
        ev = Event(
            creator_id=admin.id,
            title=f"Report Memory Benchmark {num_rows} Rows",
            category="concert",
            venue="Taipei Dome Benchmark",
            start_time=now - timedelta(hours=1),
            end_time=now + timedelta(days=2),
            status=EventStatus.PUBLISHED.value,
        )
        db.add(ev)
        db.flush()
        db.add_all(
            [
                Zone(event_id=ev.id, zone_code="A", name="A 區", price=Decimal("1000.00")),
                Zone(event_id=ev.id, zone_code="B", name="B 區", price=Decimal("2000.00")),
            ]
        )
        db.commit()
        event_id = ev.id
        event_title = ev.title
        admin_id = admin.id
        admin_role = admin.role
        admin_tv = admin.token_version

    # 以 PostgreSQL COPY 快速建置大量測試資料（1 筆代表座位 + N 筆 cancelled order_items 以避開 uq_active_seat_order）
    t_seed = time.perf_counter()
    conn = psycopg2.connect(settings.SYNC_DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO seats (event_id, zone_code, row_label, seat_number, status, version) "
                "VALUES (%s, 'A', '1', '1', 'available', 1), (%s, 'B', '1', '1', 'available', 1) RETURNING id",
                (event_id, event_id),
            )
            seat_a_id, seat_b_id = [r[0] for r in cur.fetchall()]

            chunk_size = 50000
            inserted = 0
            expires_iso = (now + timedelta(minutes=10)).isoformat()
            while inserted < num_rows:
                n = min(chunk_size, num_rows - inserted)
                cur.execute(
                    """
                    WITH gen AS (
                        SELECT gs FROM generate_series(1, %s) AS gs
                    ),
                    ins_orders AS (
                        INSERT INTO orders (
                            user_id, event_id, total_ticket_amount, service_fee, total_amount,
                            status, expires_at
                        )
                        SELECT
                            %s, %s,
                            CASE WHEN (gs %% 2 = 0) THEN 1000.00 ELSE 2000.00 END,
                            CASE WHEN (gs %% 2 = 0) THEN 50.00 ELSE 100.00 END,
                            CASE WHEN (gs %% 2 = 0) THEN 1050.00 ELSE 2100.00 END,
                            'cancelled', %s
                        FROM gen
                        RETURNING id, total_ticket_amount
                    )
                    INSERT INTO order_items (order_id, seat_id, price, status)
                    SELECT
                        id,
                        CASE WHEN total_ticket_amount = 1000.00 THEN %s ELSE %s END,
                        total_ticket_amount,
                        'cancelled'
                    FROM ins_orders
                    """,
                    (n, admin_id, event_id, expires_iso, seat_a_id, seat_b_id),
                )
                inserted += n
        conn.commit()
    finally:
        conn.close()
    seed_elapsed = time.perf_counter() - t_seed

    rss_before, _ = _read_linux_rss_mb()
    cg_before = _read_cgroup_memory()
    if use_tracemalloc:
        tracemalloc.start()

    # 背景取樣容器實際不可回收匿名記憶體（cgroup memory.stat anon）峰值
    peak_anon_mib = cg_before.get("anon_mib") or 0.0
    stop_sampler = threading.Event()

    def _sample_anon() -> None:
        nonlocal peak_anon_mib
        while not stop_sampler.is_set():
            snap = _read_cgroup_memory()
            val = snap.get("anon_mib")
            if isinstance(val, (int, float)) and val > peak_anon_mib:
                peak_anon_mib = val
            stop_sampler.wait(0.2)

    sampler_thread = threading.Thread(target=_sample_anon, daemon=True)
    sampler_thread.start()

    t_export = time.perf_counter()

    http_status = None
    x_report_rows = None
    xml_details_data_rows = None
    zip_entries: list[str] = []
    try:
        if via_http:
            token = create_access_token(
                {"sub": str(admin_id), "role": admin_role, "token_version": admin_tv}
            )
            fd, dl_path = tempfile.mkstemp(prefix="flash_ticket_http_bench_", suffix=".xlsx")
            os.close(fd)
            try:
                with httpx.Client(timeout=httpx.Timeout(300.0, connect=10.0)) as client:
                    with client.stream(
                        "GET",
                        f"{base_url}/api/v1/reports/exports",
                        params={"event_id": event_id, "batch_size": batch_size},
                        headers={"Authorization": f"Bearer {token}"},
                    ) as resp:
                        http_status = resp.status_code
                        if resp.status_code != 200:
                            raise RuntimeError(
                                f"HTTP {resp.status_code}: {resp.read().decode('utf-8', 'ignore')}"
                            )
                        raw_rows_hdr = resp.headers.get("x-report-rows")
                        if raw_rows_hdr is None:
                            raise RuntimeError("Missing X-Report-Rows response header")
                        x_report_rows = int(raw_rows_hdr)
                        with open(dl_path, "wb") as out_f:
                            for chunk in resp.iter_bytes(chunk_size=65536):
                                out_f.write(chunk)
                file_size_mb = os.path.getsize(dl_path) / (1024 * 1024)
                with zipfile.ZipFile(dl_path, "r") as zf:
                    zip_entries = zf.namelist()
                # 獨立串流掃描下載後的 Details 工作表 XML，扣除第 1 列標題列
                xml_details_data_rows = _count_sheet_rows_streaming(dl_path, "xl/worksheets/sheet2.xml") - 1
                if x_report_rows != num_rows:
                    raise RuntimeError(f"X-Report-Rows mismatch: {x_report_rows} != {num_rows}")
                if xml_details_data_rows != num_rows:
                    raise RuntimeError(f"XML Details row count mismatch: {xml_details_data_rows} != {num_rows}")
                rows_exported = xml_details_data_rows
            finally:
                if os.path.exists(dl_path):
                    os.remove(dl_path)
        else:
            res = _generate_excel_sync(
                sync_db_url=settings.SYNC_DATABASE_URL,
                scope=ReportScope(event_id=event_id, creator_id_filter=None, event_title=event_title),
                date_from=None,
                date_to=None,
                batch_size=batch_size,
            )
            file_size_mb = os.path.getsize(res.file_path) / (1024 * 1024)
            xml_details_data_rows = _count_sheet_rows_streaming(res.file_path, "xl/worksheets/sheet2.xml") - 1
            rows_exported = res.detail_rows
            os.remove(res.file_path)

        export_elapsed = time.perf_counter() - t_export
        stop_sampler.set()
        sampler_thread.join(timeout=2.0)
        peak_py = 0
        if use_tracemalloc:
            _, peak_py = tracemalloc.get_traced_memory()
            tracemalloc.stop()
        rss_after, hwm_mb = _read_linux_rss_mb()
        cg_after = _read_cgroup_memory()
    finally:
        stop_sampler.set()
        # 清理壓測活動資料
        conn = psycopg2.connect(settings.SYNC_DATABASE_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE event_id = %s)", (event_id,))
                cur.execute("DELETE FROM orders WHERE event_id = %s", (event_id,))
                cur.execute("DELETE FROM seats WHERE event_id = %s", (event_id,))
                cur.execute("DELETE FROM zones WHERE event_id = %s", (event_id,))
                cur.execute("DELETE FROM events WHERE id = %s", (event_id,))
            conn.commit()
        finally:
            conn.close()

    stats = {
        "mode": "http_endpoint" if via_http else "in_process",
        "rows_exported": rows_exported,
        "xml_details_data_rows": xml_details_data_rows,
        "batch_size": batch_size,
        "seed_seconds": round(seed_elapsed, 2),
        "export_seconds": round(export_elapsed, 2),
        "xlsx_size_mb": round(file_size_mb, 2),
        "rss_before_mib": round(rss_before, 2),
        "rss_after_mib": round(rss_after, 2),
        "kernel_vmhwm_peak_mib": round(hwm_mb, 2),
    }
    if via_http:
        stats["http_status"] = http_status
        stats["x_report_rows_header"] = x_report_rows
        stats["has_summary_and_details_sheets"] = (
            "xl/worksheets/sheet1.xml" in zip_entries
            and "xl/worksheets/sheet2.xml" in zip_entries
        )
    if cg_before.get("memory_max_bytes") is not None:
        stats["container_cgroup_memory_max_bytes"] = cg_before["memory_max_bytes"]
        stats["container_cgroup_memory_max_mib"] = cg_before["memory_max_mib"]
    if cg_before.get("anon_mib") is not None:
        stats["container_cgroup_anon_before_mib"] = cg_before["anon_mib"]
        stats["container_cgroup_anon_peak_sampled_mib"] = round(peak_anon_mib, 2)
        stats["container_cgroup_anon_after_mib"] = cg_after["anon_mib"]
    if cg_after.get("oom_kill") is not None:
        stats["container_cgroup_oom_kill_count"] = cg_after["oom_kill"]
    if use_tracemalloc:
        stats["tracemalloc_peak_mib"] = round(peak_py / (1024 * 1024), 2)
    print("=== Streaming Excel Export Memory Benchmark ===")
    for k, v in stats.items():
        print(f"{k}: {v}")
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=500000)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--tracemalloc", action="store_true")
    parser.add_argument("--via-http", action="store_true")
    parser.add_argument("--base-url", type=str, default="http://127.0.0.1:8000")
    args = parser.parse_args()
    run_benchmark(
        num_rows=args.rows,
        batch_size=args.batch_size,
        use_tracemalloc=args.tracemalloc,
        via_http=args.via_http,
        base_url=args.base_url,
    )
