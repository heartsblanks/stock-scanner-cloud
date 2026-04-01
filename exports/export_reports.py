from __future__ import annotations

import csv
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional

from google.cloud import storage

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.db import fetch_all


LOG_BUCKET = os.getenv("LOG_BUCKET", "stock-scanner-490821-logs")
TRADE_ANALYSIS_SUMMARY_OBJECT = os.getenv("TRADE_ANALYSIS_SUMMARY_OBJECT", "reports/trade_analysis_summary.csv")
TRADE_ANALYSIS_PAIRED_OBJECT = os.getenv("TRADE_ANALYSIS_PAIRED_OBJECT", "reports/trade_analysis_paired_trades.csv")
SIGNAL_ANALYSIS_SUMMARY_OBJECT = os.getenv("SIGNAL_ANALYSIS_SUMMARY_OBJECT", "reports/signal_analysis_summary.csv")
SIGNAL_ANALYSIS_ROWS_OBJECT = os.getenv("SIGNAL_ANALYSIS_ROWS_OBJECT", "reports/signal_analysis_rows.csv")
RECONCILIATION_OBJECT = os.getenv("RECONCILIATION_OBJECT", "reports/alpaca_reconciliation.csv")


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_csv(file_path: Path, rows: list[dict], fieldnames: Iterable[str]) -> None:
    _ensure_dir(file_path.parent)
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _download_gcs_file(storage_client: storage.Client, bucket_name: str, source_path: str, local_path: Path) -> Optional[Path]:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_path)

    if not blob.exists():
        print(f"GCS file not found: gs://{bucket_name}/{source_path}")
        return None

    _ensure_dir(local_path.parent)
    blob.download_to_filename(str(local_path))
    return local_path

def export_analysis_files(output_dir: str | Path, for_date: Optional[date] = None) -> list[Path]:
    target_date = for_date or datetime.utcnow().date()
    base_dir = Path(output_dir) / str(target_date) / "analysis"
    storage_client = storage.Client()

    exported: list[Path] = []

    for source_path, filename in [
        (TRADE_ANALYSIS_SUMMARY_OBJECT, "trade_analysis_summary.csv"),
        (TRADE_ANALYSIS_PAIRED_OBJECT, "trade_analysis_paired_trades.csv"),
        (SIGNAL_ANALYSIS_SUMMARY_OBJECT, "signal_analysis_summary.csv"),
        (SIGNAL_ANALYSIS_ROWS_OBJECT, "signal_analysis_rows.csv"),
        (RECONCILIATION_OBJECT, "alpaca_reconciliation.csv"),
    ]:
        exported_path = _download_gcs_file(
            storage_client,
            LOG_BUCKET,
            source_path,
            base_dir / filename,
        )
        if exported_path is not None:
            exported.append(exported_path)

    return exported



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
    file_path = Path(output_dir) / str(target_date) / "db" / "trade_events.csv"
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
    file_path = Path(output_dir) / str(target_date) / "db" / "scan_runs.csv"
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
    file_path = Path(output_dir) / str(target_date) / "db" / "broker_orders.csv"
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
    file_path = Path(output_dir) / str(target_date) / "db" / "reconciliation_runs.csv"
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


def export_signal_logs(output_dir: str | Path, for_date: Optional[date] = None) -> Path:
    target_date = for_date or datetime.utcnow().date()
    rows = fetch_all(
        """
        SELECT *
        FROM signal_logs
        WHERE DATE(timestamp_utc) = %(target_date)s
        ORDER BY timestamp_utc ASC, id ASC
        """,
        {"target_date": target_date},
    )
    file_path = Path(output_dir) / str(target_date) / "db" / "signal_logs.csv"
    fieldnames = rows[0].keys() if rows else [
        "id",
        "timestamp_utc",
        "scan_id",
        "scan_source",
        "market_phase",
        "scan_execution_time_ms",
        "mode",
        "account_size",
        "current_open_positions",
        "current_open_exposure",
        "timing_ok",
        "source",
        "trade_count",
        "top_name",
        "top_symbol",
        "current_price",
        "entry",
        "stop",
        "target",
        "shares",
        "confidence",
        "reason",
        "benchmark_sp500",
        "benchmark_nasdaq",
        "paper_trade_enabled",
        "paper_trade_candidate_count",
        "paper_trade_long_candidate_count",
        "paper_trade_short_candidate_count",
        "paper_trade_placed_count",
        "paper_trade_placed_long_count",
        "paper_trade_placed_short_count",
        "paper_candidate_symbols",
        "paper_candidate_confidences",
        "paper_skipped_symbols",
        "paper_skip_reasons",
        "paper_placed_symbols",
        "paper_trade_ids",
        "created_at",
    ]
    _write_csv(file_path, rows, fieldnames)
    return file_path


def export_trade_lifecycles(output_dir: str | Path, for_date: Optional[date] = None) -> Path:
    target_date = for_date or datetime.utcnow().date()
    rows = fetch_all(
        """
        SELECT *
        FROM trade_lifecycles
        WHERE DATE(COALESCE(exit_time, entry_time, created_at)) = %(target_date)s
        ORDER BY COALESCE(exit_time, entry_time, created_at) ASC, id ASC
        """,
        {"target_date": target_date},
    )
    file_path = Path(output_dir) / str(target_date) / "db" / "trade_lifecycles.csv"
    fieldnames = rows[0].keys() if rows else [
        "id",
        "trade_key",
        "symbol",
        "mode",
        "side",
        "direction",
        "status",
        "entry_time",
        "exit_time",
        "duration_minutes",
        "shares",
        "entry_price",
        "exit_price",
        "stop_price",
        "target_price",
        "exit_reason",
        "signal_timestamp",
        "signal_entry",
        "signal_stop",
        "signal_target",
        "signal_confidence",
        "order_id",
        "parent_order_id",
        "exit_order_id",
        "realized_pnl",
        "realized_pnl_percent",
        "created_at",
        "updated_at",
    ]
    _write_csv(file_path, rows, fieldnames)
    return file_path


def export_all_reports(output_dir: str | Path, for_date: Optional[date] = None) -> list[Path]:
    exported_paths = [
        export_trade_events(output_dir, for_date=for_date),
        export_scan_runs(output_dir, for_date=for_date),
        export_broker_orders(output_dir, for_date=for_date),
        export_signal_logs(output_dir, for_date=for_date),
        export_trade_lifecycles(output_dir, for_date=for_date),
        export_reconciliation_runs(output_dir, for_date=for_date),
    ]
    exported_paths.extend(export_analysis_files(output_dir, for_date=for_date))
    return exported_paths
