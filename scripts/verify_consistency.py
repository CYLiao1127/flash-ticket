"""壓測後自動化資料一致性校驗腳本 (Commit 22)

驗證六大核心不變量 (Invariants)：
1. 零重複佔位：同一座位絕不可同時出現在兩筆有效（held / paid）order_items。
2. Held 數量完全一致：seats(status='held') == order_items(status='held')。
3. Sold 數量完全一致：seats(status='sold') == order_items(status='sold')。
4. 單人限購 4 張上限：該活動中任何單一使用者的有效票券數 <= 4。
5. 5% 服務費精確四捨五入：每筆訂單皆符合 ROUND_HALF_UP 與 total_amount = ticket + fee。
6. Outbox 序號嚴格連號：event_outbox 筆數 == events.last_seq，且序號範圍 1..last_seq 無跳號。

使用方式：
  docker compose exec -T api-1 python scripts/verify_consistency.py --event-id 34
"""

import argparse
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
import sys
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.db import SyncSessionLocal
from app.models import (
    Event,
    EventOutbox,
    Order,
    OrderItem,
    OrderItemStatus,
    Seat,
    SeatStatus,
)


@dataclass
class ConsistencyReport:
    event_id: int
    total_seats: int = 0
    seats_available: int = 0
    seats_held: int = 0
    seats_sold: int = 0
    order_items_held: int = 0
    order_items_sold: int = 0
    duplicate_active_seats: int = 0
    max_tickets_per_user: int = 0
    invalid_fee_orders: int = 0
    event_last_seq: int = 0
    outbox_count: int = 0
    outbox_min_seq: Optional[int] = None
    outbox_max_seq: Optional[int] = None
    failures: List[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.failures) == 0


def verify_event_consistency(db: Session, event_id: int) -> ConsistencyReport:
    report = ConsistencyReport(event_id=event_id)

    event = db.execute(select(Event).where(Event.id == event_id)).scalar_one_or_none()
    if event is None:
        report.failures.append(f"Event {event_id} not found")
        return report

    report.event_last_seq = int(event.last_seq or 0)

    # 1. 統計 seats 各狀態數量
    seat_counts = dict(
        db.execute(
            select(Seat.status, func.count(Seat.id))
            .where(Seat.event_id == event_id)
            .group_by(Seat.status)
        ).all()
    )
    report.seats_available = int(seat_counts.get(SeatStatus.AVAILABLE.value, 0))
    report.seats_held = int(seat_counts.get(SeatStatus.HELD.value, 0))
    report.seats_sold = int(seat_counts.get(SeatStatus.SOLD.value, 0))
    report.total_seats = report.seats_available + report.seats_held + report.seats_sold

    # 2. 統計 order_items 各有效狀態數量
    oi_counts = dict(
        db.execute(
            select(OrderItem.status, func.count(OrderItem.id))
            .join(Order, Order.id == OrderItem.order_id)
            .where(Order.event_id == event_id)
            .group_by(OrderItem.status)
        ).all()
    )
    report.order_items_held = int(oi_counts.get(OrderItemStatus.HELD.value, 0))
    report.order_items_sold = int(oi_counts.get(OrderItemStatus.PAID.value, 0))

    if report.seats_held != report.order_items_held:
        report.failures.append(
            f"Held seat mismatch: seats.held={report.seats_held} != order_items.held={report.order_items_held}"
        )
    if report.seats_sold != report.order_items_sold:
        report.failures.append(
            f"Sold seat mismatch: seats.sold={report.seats_sold} != order_items.sold={report.order_items_sold}"
        )

    # 3. 檢查是否有任何座位重複出現在多筆有效 order_items（超賣檢查）
    dup_rows = db.execute(
        text(
            "SELECT oi.seat_id, COUNT(*) AS cnt "
            "FROM order_items oi "
            "JOIN orders o ON o.id = oi.order_id "
            "WHERE o.event_id = :eid AND oi.status IN ('held', 'paid') "
            "GROUP BY oi.seat_id "
            "HAVING COUNT(*) > 1"
        ),
        {"eid": event_id},
    ).all()
    report.duplicate_active_seats = len(dup_rows)
    if report.duplicate_active_seats > 0:
        report.failures.append(
            f"Overselling detected! {report.duplicate_active_seats} duplicate active seat(s)"
        )

    # 4. 檢查單人單場限購 <= 4 張
    max_per_user = db.execute(
        text(
            "SELECT COALESCE(MAX(cnt), 0) FROM ("
            "  SELECT o.user_id, COUNT(oi.id) AS cnt "
            "  FROM order_items oi "
            "  JOIN orders o ON o.id = oi.order_id "
            "  WHERE o.event_id = :eid AND oi.status IN ('held', 'paid') "
            "  GROUP BY o.user_id"
            ") sub"
        ),
        {"eid": event_id},
    ).scalar_one()
    report.max_tickets_per_user = int(max_per_user or 0)
    if report.max_tickets_per_user > 4:
        report.failures.append(
            f"Per-user 4-ticket limit violated: max_tickets_per_user={report.max_tickets_per_user}"
        )

    # 5. 檢查所有訂單的 5% 服務費 ROUND_HALF_UP 與總金額精確性
    orders = db.execute(
        select(Order.id, Order.total_ticket_amount, Order.service_fee, Order.total_amount)
        .where(Order.event_id == event_id)
    ).all()
    invalid_fees = 0
    for _oid, ticket_amt, fee, total in orders:
        expected_fee = (Decimal(ticket_amt) * Decimal("0.05")).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
        if Decimal(fee) != expected_fee or Decimal(total) != (Decimal(ticket_amt) + expected_fee):
            invalid_fees += 1
    report.invalid_fee_orders = invalid_fees
    if invalid_fees > 0:
        report.failures.append(f"Monetary fee mismatch in {invalid_fees} order(s)")

    # 6. 檢查 Outbox 序號嚴格連號 (1..last_seq)
    outbox_stats = db.execute(
        select(
            func.count(EventOutbox.id),
            func.min(EventOutbox.sequence_id),
            func.max(EventOutbox.sequence_id),
        ).where(EventOutbox.event_id == event_id)
    ).one()
    report.outbox_count = int(outbox_stats[0] or 0)
    report.outbox_min_seq = int(outbox_stats[1]) if outbox_stats[1] is not None else None
    report.outbox_max_seq = int(outbox_stats[2]) if outbox_stats[2] is not None else None

    if report.event_last_seq == 0:
        if report.outbox_count != 0:
            report.failures.append(
                f"Outbox count mismatch: last_seq=0 but outbox_count={report.outbox_count}"
            )
    else:
        if (
            report.outbox_count != report.event_last_seq
            or report.outbox_min_seq != 1
            or report.outbox_max_seq != report.event_last_seq
        ):
            report.failures.append(
                f"Outbox sequence gap detected: last_seq={report.event_last_seq}, "
                f"count={report.outbox_count}, min={report.outbox_min_seq}, max={report.outbox_max_seq}"
            )

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify post-loadtest database consistency")
    parser.add_argument("--event-id", type=int, default=None)
    parser.add_argument("--env-file", type=str, default="loadtests/loadtest_env.json")
    args = parser.parse_args()

    event_id = args.event_id
    if event_id is None:
        env_path = Path(args.env_file)
        if env_path.exists():
            event_id = int(json.loads(env_path.read_text(encoding="utf-8"))["event_id"])
        else:
            print("ERROR: --event-id is required when loadtests/loadtest_env.json does not exist")
            return 1

    with SyncSessionLocal() as db:
        report = verify_event_consistency(db, event_id)

    print(f"=== Post-Loadtest Consistency Report (event_id={report.event_id}) ===")
    print(
        f"Seats: total={report.total_seats}, available={report.seats_available}, "
        f"held={report.seats_held}, sold={report.seats_sold}"
    )
    print(
        f"OrderItems: held={report.order_items_held}, sold={report.order_items_sold}, "
        f"duplicate_active_seats={report.duplicate_active_seats}"
    )
    print(f"Per-User Max Active Tickets: {report.max_tickets_per_user} (limit <= 4)")
    print(f"Invalid Service Fee Orders: {report.invalid_fee_orders}")
    print(
        f"Outbox Sequence: last_seq={report.event_last_seq}, count={report.outbox_count}, "
        f"range={report.outbox_min_seq}..{report.outbox_max_seq}"
    )

    if report.passed:
        print("RESULT: PASS (All 6 consistency invariants verified)")
        return 0
    print("RESULT: FAIL")
    for err in report.failures:
        print(f"  - {err}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
