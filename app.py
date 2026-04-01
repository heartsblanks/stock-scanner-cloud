import os
from datetime import datetime

from flask import Flask
from flask_cors import CORS
from logging_utils import log_exception
from paper_alpaca import place_paper_bracket_order_from_trade, get_open_positions, close_position, cancel_open_orders_for_symbol
from alpaca_sync import sync_order_by_id, get_order_by_id
from alpaca_reconcile import run_reconciliation, upload_file_to_gcs
from analytics.trade_analysis import run_trade_analysis, upload_file_to_gcs as upload_analysis_file_to_gcs
from analytics.signal_analysis import run_signal_analysis, upload_file_to_gcs as upload_signal_analysis_file_to_gcs
from db import healthcheck as db_healthcheck
from storage import (
    insert_scan_run,
    insert_signal_log,
    insert_paper_trade_attempt,
    insert_trade_event,
    insert_broker_order,
    insert_reconciliation_run,
    insert_reconciliation_detail,
    get_latest_reconciliation_summary,
    get_recent_reconciliation_mismatches,
    get_reconciliation_runs,
    get_ops_summary,
    get_open_trade_events,
    get_closed_trade_events,
    get_recent_trade_event_rows,
    get_latest_scan_summary,
    get_recent_alpaca_api_logs,
    get_recent_alpaca_api_errors,
    get_recent_paper_trade_attempts,
    get_recent_paper_trade_rejections,
    get_paper_trade_attempt_daily_summary,
    get_paper_trade_attempt_hourly_summary,
    get_trade_lifecycles,
    get_trade_lifecycle_summary_from_table,
    upsert_trade_lifecycle,
    get_dashboard_summary,
    prune_alpaca_api_logs,
)
from exports.export_daily_snapshot import run_daily_snapshot
from routes.health import register_health_routes
from routes.export import register_export_routes
from routes.analysis import register_analysis_routes
from routes.reconcile import register_reconcile_routes
from routes.reconcile_legacy import register_legacy_reconcile_routes
from routes.scheduler import register_scheduler_routes
from routes.trades import register_trade_routes
from routes.scans import register_scan_routes
from routes.dashboard import register_dashboard_routes
from paper_trade_context import (
    find_best_signal_match as context_find_best_signal_match,
    find_latest_open_trade as context_find_latest_open_trade,
    get_current_open_position_state as context_get_current_open_position_state,
    get_latest_open_paper_trade_for_symbol as context_get_latest_open_paper_trade_for_symbol,
    get_latest_paper_close_event_for_symbol as context_get_latest_paper_close_event_for_symbol,
    get_managed_open_paper_trades_for_eod_close as context_get_managed_open_paper_trades_for_eod_close,
    get_open_paper_trades as context_get_open_paper_trades,
    get_risk_exposure_summary as context_get_risk_exposure_summary,
    infer_first_level_hit as context_infer_first_level_hit,
    is_symbol_in_paper_cooldown as context_is_symbol_in_paper_cooldown,
    paper_trade_exit_already_logged as context_paper_trade_exit_already_logged,
    read_trade_rows_for_date as context_read_trade_rows_for_date,
)
from scan_context import (
    NY_TZ,
    build_scan_id,
    build_scheduled_scan_payload,
    debug_to_dict,
    market_phase_from_timestamp,
    paper_candidate_from_evaluation as build_paper_candidate_from_evaluation,
    parse_iso_utc,
    to_float_or_none,
    trade_to_dict,
)
from app_orchestration import (
    build_reconcile_now_response,
    build_reconciliation_runs_response,
    close_all_paper_positions as run_close_all_paper_positions,
    handle_scan_request as run_handle_scan_request,
    handle_sync_paper_trades as run_handle_sync_paper_trades,
    run_scheduled_paper_scan_wrapper as run_scheduled_scan_wrapper,
)
from scheduler_ops import (
    execute_maintenance_ops as build_execute_maintenance_ops,
    execute_market_ops as build_execute_market_ops,
    execute_post_close_ops as build_execute_post_close_ops,
)

from routes.sync import register_sync_routes
from services.sync_service import execute_sync_paper_trades
from services.scan_service import execute_full_scan
from services.trade_service import execute_close_all_paper_positions

from analytics.trade_scan import (
    run_scan,
    evaluate_symbol,
    market_time_check,
    MIN_CONFIDENCE,
    PRIMARY_INSTRUMENTS,
    SECONDARY_INSTRUMENTS,
    THIRD_INSTRUMENTS,
    FOURTH_INSTRUMENTS,
    CORE_ONE_INSTRUMENTS,
    CORE_TWO_INSTRUMENTS,
)
PAPER_TRADE_MIN_CONFIDENCE = 70


app = Flask(__name__)
CORS(app)


def env_flag(name: str, default: str = "true") -> bool:
    value = str(os.getenv(name, default)).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}

ENABLE_DB_LOGGING = env_flag("ENABLE_DB_LOGGING", "true")

INSTRUMENT_GROUPS = {
    "primary": PRIMARY_INSTRUMENTS,
    "secondary": SECONDARY_INSTRUMENTS,
    "third": THIRD_INSTRUMENTS,
    "fourth": FOURTH_INSTRUMENTS,
    "core_one": CORE_ONE_INSTRUMENTS,
    "core_two": CORE_TWO_INSTRUMENTS,
}


def safe_insert_scan_run(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_scan_run(**kwargs)
    except Exception as e:
        log_exception("DB scan run write failed", e, component="app", operation="insert_scan_run")


def safe_insert_signal_log(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_signal_log(**kwargs)
    except Exception as e:
        log_exception("DB signal log write failed", e, component="app", operation="insert_signal_log")


def safe_insert_trade_event(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_trade_event(**kwargs)
    except Exception as e:
        log_exception("DB trade event write failed", e, component="app", operation="insert_trade_event")


def safe_insert_paper_trade_attempt(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_paper_trade_attempt(**kwargs)
    except Exception as e:
        log_exception("DB paper trade attempt write failed", e, component="app", operation="insert_paper_trade_attempt")


def safe_insert_broker_order(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_broker_order(**kwargs)
    except Exception as e:
        log_exception("DB broker order write failed", e, component="app", operation="insert_broker_order")



def safe_insert_reconciliation_run(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_reconciliation_run(**kwargs)
    except Exception as e:
        log_exception("DB reconciliation write failed", e, component="app", operation="insert_reconciliation_run")


def safe_insert_reconciliation_detail(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_reconciliation_detail(**kwargs)
    except Exception as e:
        log_exception("DB reconciliation detail write failed", e, component="app", operation="insert_reconciliation_detail")


def find_instrument_by_symbol(symbol: str) -> tuple[str, str] | tuple[None, None]:
    symbol = symbol.strip().upper()
    for mode_name, instruments in INSTRUMENT_GROUPS.items():
        for display_name, info in instruments.items():
            if info.get("symbol", "").upper() == symbol:
                return display_name, mode_name
    return None, None


LOG_BUCKET = os.getenv("LOG_BUCKET", "stock-scanner-490821-logs")
RECONCILIATION_BUCKET = os.getenv("RECONCILIATION_BUCKET", "stock-scanner-490821-logs")
RECONCILIATION_OBJECT = os.getenv("RECONCILIATION_OBJECT", "reports/alpaca_reconciliation.csv")
TRADE_ANALYSIS_BUCKET = os.getenv("TRADE_ANALYSIS_BUCKET", "stock-scanner-490821-logs")
TRADE_ANALYSIS_SUMMARY_OBJECT = os.getenv("TRADE_ANALYSIS_SUMMARY_OBJECT", "reports/trade_analysis_summary.csv")
TRADE_ANALYSIS_PAIRED_OBJECT = os.getenv("TRADE_ANALYSIS_PAIRED_OBJECT", "reports/trade_analysis_paired_trades.csv")
SIGNAL_ANALYSIS_BUCKET = os.getenv("SIGNAL_ANALYSIS_BUCKET", "stock-scanner-490821-logs")
SIGNAL_ANALYSIS_SUMMARY_OBJECT = os.getenv("SIGNAL_ANALYSIS_SUMMARY_OBJECT", "reports/signal_analysis_summary.csv")
SIGNAL_ANALYSIS_ROWS_OBJECT = os.getenv("SIGNAL_ANALYSIS_ROWS_OBJECT", "reports/signal_analysis_rows.csv")
def paper_candidate_from_evaluation(eval_result: dict) -> dict | None:
    return build_paper_candidate_from_evaluation(eval_result, PAPER_TRADE_MIN_CONFIDENCE)


def append_signal_log(row: dict) -> None:
    safe_insert_signal_log(
        timestamp_utc=parse_iso_utc(str(row.get("timestamp_utc", ""))),
        scan_id=str(row.get("scan_id", "")).strip() or None,
        scan_source=str(row.get("scan_source", "")).strip() or None,
        market_phase=str(row.get("market_phase", "")).strip() or None,
        scan_execution_time_ms=int(row.get("scan_execution_time_ms")) if row.get("scan_execution_time_ms") not in (None, "") else None,
        mode=str(row.get("mode", "")).strip() or None,
        account_size=to_float_or_none(row.get("account_size")),
        current_open_positions=int(float(row.get("current_open_positions"))) if row.get("current_open_positions") not in (None, "") else None,
        current_open_exposure=to_float_or_none(row.get("current_open_exposure")),
        timing_ok=bool(row.get("timing_ok")) if row.get("timing_ok") is not None else None,
        source=str(row.get("source", "")).strip() or None,
        trade_count=int(row.get("trade_count")) if row.get("trade_count") not in (None, "") else None,
        top_name=str(row.get("top_name", "")).strip() or None,
        top_symbol=str(row.get("top_symbol", "")).strip().upper() or None,
        current_price=to_float_or_none(row.get("current_price")),
        entry=to_float_or_none(row.get("entry")),
        stop=to_float_or_none(row.get("stop")),
        target=to_float_or_none(row.get("target")),
        shares=to_float_or_none(row.get("shares")),
        confidence=to_float_or_none(row.get("confidence")),
        reason=str(row.get("reason", "")).strip() or None,
        benchmark_sp500=to_float_or_none(row.get("benchmark_sp500")),
        benchmark_nasdaq=to_float_or_none(row.get("benchmark_nasdaq")),
        paper_trade_enabled=bool(row.get("paper_trade_enabled")) if row.get("paper_trade_enabled") is not None else None,
        paper_trade_candidate_count=int(row.get("paper_trade_candidate_count")) if row.get("paper_trade_candidate_count") not in (None, "") else None,
        paper_trade_long_candidate_count=int(row.get("paper_trade_long_candidate_count")) if row.get("paper_trade_long_candidate_count") not in (None, "") else None,
        paper_trade_short_candidate_count=int(row.get("paper_trade_short_candidate_count")) if row.get("paper_trade_short_candidate_count") not in (None, "") else None,
        paper_trade_placed_count=int(row.get("paper_trade_placed_count")) if row.get("paper_trade_placed_count") not in (None, "") else None,
        paper_trade_placed_long_count=int(row.get("paper_trade_placed_long_count")) if row.get("paper_trade_placed_long_count") not in (None, "") else None,
        paper_trade_placed_short_count=int(row.get("paper_trade_placed_short_count")) if row.get("paper_trade_placed_short_count") not in (None, "") else None,
        paper_candidate_symbols=str(row.get("paper_candidate_symbols", "")).strip() or None,
        paper_candidate_confidences=str(row.get("paper_candidate_confidences", "")).strip() or None,
        paper_skipped_symbols=str(row.get("paper_skipped_symbols", "")).strip() or None,
        paper_skip_reasons=str(row.get("paper_skip_reasons", "")).strip() or None,
        paper_placed_symbols=str(row.get("paper_placed_symbols", "")).strip() or None,
        paper_trade_ids=str(row.get("paper_trade_ids", "")).strip() or None,
    )


def append_trade_log(row: dict) -> None:
    return None

def read_trade_rows_for_date(target_date: str) -> list[dict]:
    return context_read_trade_rows_for_date(target_date)


# --- Helper functions for paper trade syncing ---

def paper_trade_exit_already_logged(parent_order_id: str, exit_event: str) -> bool:
    return context_paper_trade_exit_already_logged(parent_order_id, exit_event)


def get_open_paper_trades() -> list[dict]:
    return context_get_open_paper_trades()



def get_managed_open_paper_trades_for_eod_close() -> list[dict]:
    return context_get_managed_open_paper_trades_for_eod_close()


def get_current_open_position_state() -> tuple[int, float]:
    return context_get_current_open_position_state()


# --- Risk exposure summary helper ---
def get_risk_exposure_summary() -> dict:
    return context_get_risk_exposure_summary()


def get_latest_open_paper_trade_for_symbol(symbol: str) -> dict | None:
    return context_get_latest_open_paper_trade_for_symbol(symbol)


def get_latest_paper_close_event_for_symbol(symbol: str) -> dict | None:
    return context_get_latest_paper_close_event_for_symbol(symbol)


def is_symbol_in_paper_cooldown(symbol: str, now_utc: str) -> tuple[bool, str]:
    return context_is_symbol_in_paper_cooldown(symbol, now_utc)


def find_best_signal_match(symbol: str, actual_entry_price: float | None, open_timestamp_utc: str) -> dict | None:
    return context_find_best_signal_match(symbol, actual_entry_price, open_timestamp_utc)


def find_latest_open_trade(symbol: str, trade_source: str | None = None, broker_parent_order_id: str | None = None) -> dict | None:
    return context_find_latest_open_trade(symbol, trade_source=trade_source, broker_parent_order_id=broker_parent_order_id)


def infer_first_level_hit(open_row: dict, close_timestamp_utc: str) -> dict:
    return context_infer_first_level_hit(open_row, close_timestamp_utc)




def handle_sync_paper_trades():
    return run_handle_sync_paper_trades(
        execute_sync_paper_trades=execute_sync_paper_trades,
        get_open_paper_trades=get_open_paper_trades,
        sync_order_by_id=sync_order_by_id,
        paper_trade_exit_already_logged=paper_trade_exit_already_logged,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        safe_insert_broker_order=safe_insert_broker_order,
        parse_iso_utc=parse_iso_utc,
        to_float_or_none=to_float_or_none,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        get_open_positions=get_open_positions,
        close_position=close_position,
    )

def handle_scan_request(payload):
    return run_handle_scan_request(
        payload,
        get_current_open_position_state=get_current_open_position_state,
        get_risk_exposure_summary=get_risk_exposure_summary,
        execute_full_scan=execute_full_scan,
        market_time_check=market_time_check,
        build_scan_id=build_scan_id,
        market_phase_from_timestamp=market_phase_from_timestamp,
        append_signal_log=append_signal_log,
        safe_insert_paper_trade_attempt=safe_insert_paper_trade_attempt,
        safe_insert_scan_run=safe_insert_scan_run,
        parse_iso_utc=parse_iso_utc,
        run_scan=run_scan,
        trade_to_dict=trade_to_dict,
        debug_to_dict=debug_to_dict,
        paper_candidate_from_evaluation=paper_candidate_from_evaluation,
        evaluate_symbol=evaluate_symbol,
        get_latest_open_paper_trade_for_symbol=get_latest_open_paper_trade_for_symbol,
        is_symbol_in_paper_cooldown=is_symbol_in_paper_cooldown,
        place_paper_bracket_order_from_trade=place_paper_bracket_order_from_trade,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        safe_insert_broker_order=safe_insert_broker_order,
        to_float_or_none=to_float_or_none,
        min_confidence=MIN_CONFIDENCE,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
    )


def run_scan_wrapper(payload):
    return handle_scan_request(payload)


def run_scheduled_paper_scan_wrapper(payload):
    now_ny = datetime.now(NY_TZ)
    return run_scheduled_scan_wrapper(
        payload,
        now_ny=now_ny,
        build_scheduled_scan_payload=build_scheduled_scan_payload,
        handle_scan_request_fn=handle_scan_request,
    )






def close_all_paper_positions():
    return run_close_all_paper_positions(
        execute_close_all_paper_positions=execute_close_all_paper_positions,
        get_open_positions=get_open_positions,
        get_managed_open_paper_trades_for_eod_close=get_managed_open_paper_trades_for_eod_close,
        cancel_open_orders_for_symbol=cancel_open_orders_for_symbol,
        close_position=close_position,
        get_order_by_id=get_order_by_id,
        safe_insert_broker_order=safe_insert_broker_order,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        to_float_or_none=to_float_or_none,
        parse_iso_utc=parse_iso_utc,
    )



def run_market_ops_scheduler(*, now_ny: datetime):
    return build_execute_market_ops(
        now_ny=now_ny,
        run_sync=handle_sync_paper_trades,
        run_scan=run_scheduled_paper_scan_wrapper,
        run_close=close_all_paper_positions,
    )


def run_daily_post_close_scheduler(*, now_ny: datetime):
    return build_execute_post_close_ops(
        now_ny=now_ny,
        run_sync=handle_sync_paper_trades,
        run_reconcile=lambda: build_reconcile_now_response(
            run_reconciliation=run_reconciliation,
            upload_file_to_gcs=upload_file_to_gcs,
            reconciliation_bucket=RECONCILIATION_BUCKET,
            reconciliation_object=RECONCILIATION_OBJECT,
            safe_insert_reconciliation_run=safe_insert_reconciliation_run,
        ),
        run_trade_analysis=run_trade_analysis,
        run_signal_analysis=run_signal_analysis,
        run_snapshot_export=run_daily_snapshot,
    )


def run_maintenance_scheduler(*, now_ny: datetime, retention_days: int = 30):
    return build_execute_maintenance_ops(
        now_ny=now_ny,
        prune_logs=prune_alpaca_api_logs,
        retention_days=retention_days,
    )


register_health_routes(
    app,
    db_healthcheck=db_healthcheck,
    enable_db_logging=ENABLE_DB_LOGGING,
    get_ops_summary=get_ops_summary,
    get_recent_alpaca_api_logs=get_recent_alpaca_api_logs,
    get_recent_alpaca_api_errors=get_recent_alpaca_api_errors,
    get_recent_paper_trade_attempts=get_recent_paper_trade_attempts,
    get_recent_paper_trade_rejections=get_recent_paper_trade_rejections,
    get_paper_trade_attempt_daily_summary=get_paper_trade_attempt_daily_summary,
    get_paper_trade_attempt_hourly_summary=get_paper_trade_attempt_hourly_summary,
    prune_alpaca_api_logs=prune_alpaca_api_logs,
)
register_export_routes(app, run_daily_snapshot=run_daily_snapshot)
register_analysis_routes(
    app,
    run_trade_analysis=run_trade_analysis,
    upload_analysis_file_to_gcs=upload_analysis_file_to_gcs,
    trade_analysis_bucket=TRADE_ANALYSIS_BUCKET,
    trade_analysis_summary_object=TRADE_ANALYSIS_SUMMARY_OBJECT,
    trade_analysis_paired_object=TRADE_ANALYSIS_PAIRED_OBJECT,
    run_signal_analysis=run_signal_analysis,
    upload_signal_analysis_file_to_gcs=upload_signal_analysis_file_to_gcs,
    signal_analysis_bucket=SIGNAL_ANALYSIS_BUCKET,
    signal_analysis_summary_object=SIGNAL_ANALYSIS_SUMMARY_OBJECT,
    signal_analysis_rows_object=SIGNAL_ANALYSIS_ROWS_OBJECT,
)
register_reconcile_routes(
    app,
    run_reconciliation=run_reconciliation,
    upload_file_to_gcs=upload_file_to_gcs,
    reconciliation_bucket=RECONCILIATION_BUCKET,
    reconciliation_object=RECONCILIATION_OBJECT,
    safe_insert_reconciliation_run=safe_insert_reconciliation_run,
    safe_insert_reconciliation_detail=safe_insert_reconciliation_detail,
    get_latest_reconciliation_summary=get_latest_reconciliation_summary,
    get_recent_reconciliation_mismatches=get_recent_reconciliation_mismatches,
)

register_trade_routes(
    app,
    append_trade_log=append_trade_log,
    safe_insert_trade_event=safe_insert_trade_event,
    safe_insert_broker_order=safe_insert_broker_order,
    close_all_paper_positions=close_all_paper_positions,
    read_trade_rows_for_date=read_trade_rows_for_date,
    find_instrument_by_symbol=find_instrument_by_symbol,
    find_best_signal_match=find_best_signal_match,
    find_latest_open_trade=find_latest_open_trade,
    infer_first_level_hit=infer_first_level_hit,
    to_float_or_none=to_float_or_none,
    parse_iso_utc=parse_iso_utc,
    get_open_trade_events=get_open_trade_events,
    get_closed_trade_events=get_closed_trade_events,
    get_recent_trade_event_rows=get_recent_trade_event_rows,
    get_latest_scan_summary=get_latest_scan_summary,
    get_trade_lifecycles=get_trade_lifecycles,
    get_trade_lifecycle_summary_from_table=get_trade_lifecycle_summary_from_table,
    upsert_trade_lifecycle=upsert_trade_lifecycle,
)

register_scan_routes(
    app,
    run_scan=run_scan_wrapper,
    run_scheduled_paper_scan=run_scheduled_paper_scan_wrapper,
)

register_sync_routes(
    app,
    sync_paper_trades_handler=handle_sync_paper_trades,
)


register_dashboard_routes(
    app,
    get_dashboard_summary=get_dashboard_summary,
    get_alpaca_open_positions=get_open_positions,
    get_risk_exposure_summary=get_risk_exposure_summary,
)
register_scheduler_routes(
    app,
    ny_tz=NY_TZ,
    execute_market_ops=run_market_ops_scheduler,
    execute_post_close_ops=run_daily_post_close_scheduler,
    execute_maintenance_ops=run_maintenance_scheduler,
)
register_legacy_reconcile_routes(
    app,
    build_reconcile_now_response=build_reconcile_now_response,
    build_reconciliation_runs_response=build_reconciliation_runs_response,
    run_reconciliation=run_reconciliation,
    upload_file_to_gcs=upload_file_to_gcs,
    reconciliation_bucket=RECONCILIATION_BUCKET,
    reconciliation_object=RECONCILIATION_OBJECT,
    safe_insert_reconciliation_run=safe_insert_reconciliation_run,
    get_reconciliation_runs=get_reconciliation_runs,
)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
