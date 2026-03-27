from __future__ import annotations

import csv
import io
from typing import Any, Callable


def append_csv_row(
    *,
    storage_client_factory: Callable[[], Any],
    bucket_name: str,
    path: str,
    headers: list[str],
    row: dict[str, Any],
) -> None:
    client = storage_client_factory()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)

    existing_rows: list[dict[str, Any]] = []
    if blob.exists():
        content = blob.download_as_text()
        if content.strip():
            reader = csv.DictReader(io.StringIO(content))
            existing_rows = list(reader)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()

    for existing_row in existing_rows:
        writer.writerow({h: existing_row.get(h, "") for h in headers})

    writer.writerow({h: row.get(h, "") for h in headers})
    blob.upload_from_string(output.getvalue(), content_type="text/csv")


def append_signal_log(
    *,
    enabled: bool,
    append_csv_row_func: Callable[..., None],
    path: str,
    row: dict[str, Any],
) -> None:
    if not enabled:
        return
    headers = [
        "timestamp_utc",
        "scan_id",
        "scan_source",
        "market_phase",
        "scan_execution_time_ms",
        "mode",
        "account_size",
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
    ]
    append_csv_row_func(path=path, headers=headers, row=row)


def append_trade_log(
    *,
    enabled: bool,
    append_csv_row_func: Callable[..., None],
    path: str,
    row: dict[str, Any],
) -> None:
    if not enabled:
        return
    headers = [
        "timestamp_utc",
        "event_type",
        "symbol",
        "name",
        "mode",
        "trade_source",
        "broker",
        "broker_order_id",
        "broker_parent_order_id",
        "broker_status",
        "broker_filled_qty",
        "broker_filled_avg_price",
        "broker_exit_order_id",
        "shares",
        "entry_price",
        "stop_price",
        "target_price",
        "exit_price",
        "exit_reason",
        "status",
        "notes",
        "linked_signal_timestamp_utc",
        "linked_signal_entry",
        "linked_signal_stop",
        "linked_signal_target",
        "linked_signal_confidence",
        "inferred_stop_hit",
        "inferred_target_hit",
        "inferred_first_level_hit",
        "inferred_analysis_start_utc",
        "inferred_analysis_end_utc",
    ]
    append_csv_row_func(path=path, headers=headers, row=row)


def read_csv_rows_for_path(
    *,
    storage_client_factory: Callable[[], Any],
    bucket_name: str,
    path: str,
) -> list[dict[str, Any]]:
    client = storage_client_factory()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)

    if not blob.exists():
        return []

    content = blob.download_as_text()
    if not content.strip():
        return []

    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


def read_trade_rows_for_date(*, all_rows: list[dict[str, Any]], target_date: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in all_rows:
        ts = str(row.get("timestamp_utc", ""))
        if ts.startswith(target_date):
            rows.append(row)
    return rows