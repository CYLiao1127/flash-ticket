"""壓測資料準備腳本：建立 10,000 座位活動與預簽發 JWT Token 池 (Commit 22)

使用方式：
  docker compose exec -T api-1 python loadtests/prepare_loadtest.py --users 3000 --seats 10000
"""

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import io
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg2
import redis as sync_redis
from sqlalchemy import select

from app.auth.security import create_access_token, hash_password
from app.config import settings
from app.db import SyncSessionLocal
from app.models import Event, EventStatus, User, UserRole, Zone


def prepare_loadtest_data(
    num_users: int = 3000,
    num_seats: int = 10000,
    output_path: str = "loadtests/loadtest_env.json",
) -> dict:
    t0 = time.perf_counter()
    now = datetime.now(timezone.utc)
    zone_codes = ["A", "B", "C", "D"]
    seats_per_zone = num_seats // len(zone_codes)
    rem = num_seats % len(zone_codes)

    with SyncSessionLocal() as db:
        admin = db.execute(
            select(User).where(User.email == "admin@flashticket.com")
        ).scalar_one()

        ev = Event(
            creator_id=admin.id,
            title=f"Loadtest Flash Sale {num_seats} Seats ({int(time.time())})",
            description="k6 情境 A/B 壓力測試專用活動",
            category="concert",
            venue="Taipei Dome Loadtest",
            start_time=now - timedelta(hours=1),
            end_time=now + timedelta(days=2),
            status=EventStatus.PUBLISHED.value,
        )
        db.add(ev)
        db.flush()

        for idx, zc in enumerate(zone_codes):
            db.add(
                Zone(
                    event_id=ev.id,
                    zone_code=zc,
                    name=f"壓測 {zc} 區",
                    price=Decimal(str(2000 + idx * 500)),
                )
            )
        db.commit()
        event_id = ev.id

    # 以 PostgreSQL COPY 極速寫入 10,000 筆座位
    conn = psycopg2.connect(settings.SYNC_DATABASE_URL)
    try:
        with conn.cursor() as cur:
            buf = io.StringIO()
            for z_idx, zc in enumerate(zone_codes):
                count_for_zone = seats_per_zone + (1 if z_idx < rem else 0)
                for i in range(count_for_zone):
                    row_label = str((i // 50) + 1)
                    seat_num = str((i % 50) + 1)
                    buf.write(f"{event_id}\t{zc}\t{row_label}\t{seat_num}\tavailable\t1\n")
            buf.seek(0)
            cur.copy_from(
                buf,
                "seats",
                columns=("event_id", "zone_code", "row_label", "seat_number", "status", "version"),
            )

            # 批次確保壓測帳號存在（單次共用雜湊，避免 3000 次 bcrypt 耗時）
            cur.execute(
                "SELECT id, email, token_version FROM users WHERE email LIKE 'loadtest_user_%@flashticket.com' ORDER BY id ASC"
            )
            existing_users = cur.fetchall()
            existing_by_email = {row[1]: (row[0], row[2]) for row in existing_users}

            missing_emails = [
                f"loadtest_user_{i:04d}@flashticket.com"
                for i in range(1, num_users + 1)
                if f"loadtest_user_{i:04d}@flashticket.com" not in existing_by_email
            ]
            if missing_emails:
                pw_hash = hash_password("LoadtestPass123!")
                u_buf = io.StringIO()
                for email in missing_emails:
                    u_buf.write(f"{email}\t{pw_hash}\tuser\ttrue\t1\n")
                u_buf.seek(0)
                cur.copy_from(
                    u_buf,
                    "users",
                    columns=("email", "hashed_password", "role", "is_active", "token_version"),
                )

            cur.execute(
                "SELECT id, email, token_version FROM users "
                "WHERE email LIKE 'loadtest_user_%%@flashticket.com' "
                "ORDER BY id ASC LIMIT %s",
                (num_users,),
            )
            user_rows = cur.fetchall()
        conn.commit()
    finally:
        conn.close()

    # 預先簽發 JWT 並暖機 Redis token_version 快取
    r = sync_redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    tokens = []
    pipe = r.pipeline(transaction=False)
    for uid, _email, tv in user_rows:
        tok = create_access_token(
            {"sub": str(uid), "role": UserRole.USER.value, "token_version": tv}
        )
        tokens.append(tok)
        pipe.set(f"user:{uid}:token_version", str(tv), ex=3600)
        if len(tokens) % 500 == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()
    r.close()

    payload = {
        "event_id": event_id,
        "zones": zone_codes,
        "total_seats": num_seats,
        "user_count": len(tokens),
        "tokens": tokens,
    }
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload), encoding="utf-8")

    elapsed = time.perf_counter() - t0
    print(
        f"[prepare_loadtest] event_id={event_id} seats={num_seats} "
        f"users={len(tokens)} written={output_path} elapsed={elapsed:.2f}s"
    )
    return payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare loadtest event, seats, and JWT token pool")
    parser.add_argument("--users", type=int, default=3000)
    parser.add_argument("--seats", type=int, default=10000)
    parser.add_argument("--output", type=str, default="loadtests/loadtest_env.json")
    args = parser.parse_args()
    prepare_loadtest_data(num_users=args.users, num_seats=args.seats, output_path=args.output)
