#!/usr/bin/env python3
"""
scripts/generate_sample_seats.py
產生座位批量匯入用之 CSV / JSON 範例檔案。
"""

import argparse
import csv
import json
import os
from typing import List, Tuple

DEFAULT_ZONES: List[Tuple[str, float]] = [
    ("VIP", 0.2),
    ("A", 0.4),
    ("B", 0.4),
]


def build_seat_rows(total_rows: int) -> List[Tuple[str, str, str]]:
    """產生指定列數之 (zone_code, row_label, seat_number) 資料列"""
    rows: List[Tuple[str, str, str]] = []
    seats_per_row = 50
    for idx in range(total_rows):
        if idx < int(total_rows * 0.2):
            zone = "VIP"
            local_idx = idx
        elif idx < int(total_rows * 0.6):
            zone = "A"
            local_idx = idx - int(total_rows * 0.2)
        else:
            zone = "B"
            local_idx = idx - int(total_rows * 0.6)

        row_num = (local_idx // seats_per_row) + 1
        seat_num = (local_idx % seats_per_row) + 1
        rows.append((zone, f"R{row_num}", str(seat_num)))
    return rows


def write_csv(path: str, rows: List[Tuple[str, str, str]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["zone_code", "row_label", "seat_number"])
        writer.writerows(rows)


def write_json(path: str, rows: List[Tuple[str, str, str]]) -> None:
    payload = [
        {"zone_code": z, "row_label": r, "seat_number": s}
        for z, r, s in rows
    ]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sample seat import files")
    parser.add_argument("--rows", type=int, default=100, help="Number of seat rows to generate")
    parser.add_argument(
        "--format",
        choices=["csv", "json", "both"],
        default="both",
        help="Output file format",
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "sample_data"),
        help="Target output directory",
    )
    args = parser.parse_args()

    out_dir = os.path.abspath(args.output_dir)
    os.makedirs(out_dir, exist_ok=True)
    rows = build_seat_rows(args.rows)

    suffix = "_100k" if args.rows >= 100_000 else "_sample"
    if args.format in ("csv", "both"):
        csv_path = os.path.join(out_dir, f"seats{suffix}.csv")
        write_csv(csv_path, rows)
        print(f"Generated CSV ({len(rows)} rows): {csv_path}")
    if args.format in ("json", "both"):
        json_path = os.path.join(out_dir, f"seats{suffix}.json")
        write_json(json_path, rows)
        print(f"Generated JSON ({len(rows)} rows): {json_path}")


if __name__ == "__main__":
    main()
