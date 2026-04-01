from __future__ import annotations

from db import execute, fetch_all, fetch_one
from repositories.broker_repo import (
    get_broker_order,
    get_broker_order_status_counts,
    get_recent_alpaca_api_errors,
    get_recent_alpaca_api_logs,
    get_recent_broker_orders,
    insert_alpaca_api_log,
    insert_broker_order,
    prune_alpaca_api_logs,
)
from repositories.common import normalize_text as _normalize_text
from repositories.common import to_optional_float as _to_optional_float
from repositories.ops_repo import get_ops_summary, get_table_row_count
from repositories.reconcile_repo import (
    get_latest_reconciliation_run,
    get_latest_reconciliation_summary,
    get_reconciliation_details_for_run,
    get_reconciliation_runs,
    get_reconciliation_status_counts_for_run,
    get_recent_reconciliation_details,
    get_recent_reconciliation_mismatches,
    insert_reconciliation_detail,
    insert_reconciliation_run,
)
from repositories.scans_repo import (
    get_latest_scan_run,
    get_latest_scan_summary,
    get_paper_trade_attempt_daily_summary,
    get_paper_trade_attempt_reason_counts,
    get_paper_trade_attempt_stage_counts,
    get_recent_paper_trade_attempts,
    get_recent_paper_trade_rejections,
    get_recent_scan_runs,
    get_signal_log_rows,
    insert_paper_trade_attempt,
    insert_scan_run,
    insert_signal_log,
)
from repositories.trades_repo import (
    get_closed_trade_events,
    get_daily_realized_pnl,
    get_dashboard_summary,
    get_equity_curve,
    get_exit_reason_breakdown,
    get_hourly_performance,
    get_latest_open_trade_lifecycle,
    get_mode_performance,
    get_open_trade_events,
    get_recent_closed_trade_lifecycle_for_symbol,
    get_recent_trade_event_rows,
    get_recent_trade_events,
    get_symbol_performance,
    get_trade_event_by_order_id,
    get_trade_event_counts_by_type,
    get_trade_event_rows_for_date,
    get_trade_lifecycle_summary_for_date,
    get_trade_lifecycle_summary_from_table,
    get_trade_lifecycles,
    get_trade_lifecycles_for_date,
    insert_trade_event,
    upsert_trade_lifecycle,
)


# Backward-compatible aliases while callers migrate to the new canonical names.
get_trade_lifecycle_rows = get_trade_lifecycles
get_trade_lifecycle_summary = get_trade_lifecycle_summary_from_table

