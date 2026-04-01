from __future__ import annotations

from core.db import fetch_one, fetch_all
from repositories.broker_repo import get_broker_order_status_counts
from repositories.reconcile_repo import get_latest_reconciliation_run
from repositories.scans_repo import (
    get_paper_trade_attempt_hourly_summary,
    get_latest_scan_run,
    get_paper_trade_attempt_reason_counts,
    get_paper_trade_attempt_stage_counts,
)
from repositories.trades_repo import get_closed_trade_events, get_open_trade_events, get_trade_event_counts_by_type


def get_table_row_count(table_name: str) -> int:
    allowed_tables = {
        "scan_runs",
        "signal_logs",
        "paper_trade_attempts",
        "trade_events",
        "broker_orders",
        "reconciliation_runs",
        "reconciliation_details",
        "alpaca_api_logs",
        "trade_lifecycles",
    }
    normalized_table_name = str(table_name).strip()
    if normalized_table_name not in allowed_tables:
        raise ValueError(f"Unsupported table for count: {normalized_table_name}")
    row = fetch_one(f"SELECT COUNT(*)::INT AS count FROM {normalized_table_name}", {})
    return int(row["count"]) if row else 0


def get_ops_summary() -> dict:
    latest_run = get_latest_reconciliation_run()
    lifecycle_status_counts = fetch_all(
        """
        SELECT COALESCE(status, '') AS status, COUNT(*)::INT AS count
        FROM trade_lifecycles
        GROUP BY COALESCE(status, '')
        ORDER BY count DESC, status ASC
        """,
        {},
    )
    return {
        "scan_runs_count": get_table_row_count("scan_runs"),
        "signal_logs_count": get_table_row_count("signal_logs"),
        "paper_trade_attempts_count": get_table_row_count("paper_trade_attempts"),
        "trade_events_count": get_table_row_count("trade_events"),
        "broker_orders_count": get_table_row_count("broker_orders"),
        "reconciliation_runs_count": get_table_row_count("reconciliation_runs"),
        "reconciliation_details_count": get_table_row_count("reconciliation_details"),
        "alpaca_api_logs_count": get_table_row_count("alpaca_api_logs"),
        "trade_lifecycles_count": get_table_row_count("trade_lifecycles"),
        "open_trade_events_count": len(get_open_trade_events(limit=1000)),
        "closed_trade_events_count": len(get_closed_trade_events(limit=1000)),
        "trade_lifecycle_status_counts": lifecycle_status_counts,
        "paper_trade_attempt_stage_counts": get_paper_trade_attempt_stage_counts(limit_days=7),
        "paper_trade_attempt_top_reasons": get_paper_trade_attempt_reason_counts(limit_days=7, limit=10),
        "paper_trade_attempt_hourly_summary": get_paper_trade_attempt_hourly_summary(limit_days=7),
        "latest_scan_run": get_latest_scan_run(),
        "latest_reconciliation_run_id": latest_run.get("id") if latest_run else None,
        "latest_reconciliation_run_time": latest_run.get("run_time") if latest_run else None,
        "broker_order_status_counts": get_broker_order_status_counts(),
        "trade_event_type_counts": get_trade_event_counts_by_type(),
    }
