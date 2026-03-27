from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional

from db import fetch_all



def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)



def _write_csv(file_path: Path, rows: list[dict], fieldnames: Iterable[str]) -> None:
    _ensure_dir(file_path.parent)
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)



def export_trade_events(output_dir: str | Path, for_date: Optional[date] = None) -> Path:
    target_date = for_date or datetime.utcnow().date()
    rows = fetch_all(
        """
        SELECT *
        FROM trade_events
        WHERE DATE(event_time) = %(target_date)s
        ORDER BY event_time ASC, id ASC
        """,
        {"target_date": target_date},
    )
    file_path = Path(output_dir) / str(target_date) / "trade_events.csv"
    fieldnames = rows[0].keys() if rows else [
        "id",
        "event_time",
        "event_type",
        "symbol",
        "side",
        "shares",
        "price",
        "mode",
        "order_id",
        "parent_order_id",
        "status",
        "created_at",
    ]
    _write_csv(file_path, rows, fieldnames)
    return file_path



def export_scan_runs(output_dir: str | Path, for_date: Optional[date] = None) -> Path:
    target_date = for_date or datetime.utcnow().date()
    rows = fetch_all(
        """
        SELECT *
        FROM scan_runs
        WHERE DATE(scan_time) = %(target_date)s
        ORDER BY scan_time ASC, id ASC
        """,
        {"target_date": target_date},
    )
    file_path = Path(output_dir) / str(target_date) / "scan_runs.csv"
    fieldnames = rows[0].keys() if rows else [
        "id",
        "scan_time",
        "mode",
        "scan_source",
        "market_phase",
        "candidate_count",
        "placed_count",
        "skipped_count",
        "created_at",
    ]
    _write_csv(file_path, rows, fieldnames)
    return file_path



def export_broker_orders(output_dir: str | Path, for_date: Optional[date] = None) -> Path:
    target_date = for_date or datetime.utcnow().date()
    rows = fetch_all(
        """
        SELECT *
        FROM broker_orders
        WHERE DATE(COALESCE(filled_at, submitted_at, created_at)) = %(target_date)s
        ORDER BY COALESCE(filled_at, submitted_at, created_at) ASC, id ASC
        """,
        {"target_date": target_date},
    )
    file_path = Path(output_dir) / str(target_date) / "broker_orders.csv"
    fieldnames = rows[0].keys() if rows else [
        "id",
        "order_id",
        "symbol",
        "side",
        "order_type",
        "status",
        "qty",
        "filled_qty",
        "avg_fill_price",
        "submitted_at",
        "filled_at",
        "created_at",
    ]
    _write_csv(file_path, rows, fieldnames)
    return file_path



def export_reconciliation_runs(output_dir: str | Path, for_date: Optional[date] = None) -> Path:
    target_date = for_date or datetime.utcnow().date()
    rows = fetch_all(
        """
        SELECT *
        FROM reconciliation_runs
        WHERE DATE(run_time) = %(target_date)s
        ORDER BY run_time ASC, id ASC
        """,
        {"target_date": target_date},
    )
    file_path = Path(output_dir) / str(target_date) / "reconciliation_runs.csv"
    fieldnames = rows[0].keys() if rows else [
        "id",
        "run_time",
        "matched_count",
        "unmatched_count",
        "notes",
        "created_at",
    ]
    _write_csv(file_path, rows, fieldnames)
    return file_path



def export_all_reports(output_dir: str | Path, for_date: Optional[date] = None) -> list[Path]:
    return [
        export_trade_events(output_dir, for_date=for_date),
        export_scan_runs(output_dir, for_date=for_date),
        export_broker_orders(output_dir, for_date=for_date),
        export_reconciliation_runs(output_dir, for_date=for_date),
    ]
