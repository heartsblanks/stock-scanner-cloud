import os
import threading
import time
from datetime import datetime
from typing import Any

from flask import Flask
from flask_cors import CORS
import requests
from core.logging_utils import log_info
from analytics.trade_analysis import run_trade_analysis, upload_file_to_gcs as upload_analysis_file_to_gcs
from analytics.signal_analysis import run_signal_analysis, upload_file_to_gcs as upload_signal_analysis_file_to_gcs
from brokers.ibkr_bridge_client import ibkr_bridge_enabled, ibkr_bridge_get
from core.db import healthcheck as db_healthcheck
from storage import (
    get_latest_reconciliation_summary,
    get_recent_reconciliation_mismatches,
    get_reconciliation_runs,
    get_ops_summary,
    get_open_trade_events,
    get_closed_trade_events,
    get_recent_trade_event_rows,
    get_latest_scan_summary,
    get_recent_paper_trade_attempts,
    get_recent_paper_trade_rejections,
    get_paper_trade_attempt_daily_summary,
    get_paper_trade_attempt_hourly_summary,
    get_trade_lifecycles,
    get_trade_lifecycles_page,
    get_trade_lifecycle_summary_from_table,
    get_stale_ibkr_closed_trade_lifecycles,
    get_latest_exit_trade_event_for_parent_order_id,
    upsert_trade_lifecycle,
    get_dashboard_summary,
    prune_operational_data,
    purge_all_test_data,
    purge_legacy_broker_data,
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
from orchestration.scan_context import (
    IBKR_SCHEDULED_MODE_ORDER,
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
from orchestration.app_orchestration import (
    build_reconcile_now_response,
    build_reconciliation_runs_response,
    close_all_paper_positions as run_close_all_paper_positions,
    handle_scan_request as run_handle_scan_request,
    handle_sync_paper_trades as run_handle_sync_paper_trades,
    run_scheduled_paper_scan_wrapper as run_scheduled_scan_wrapper,
)
from orchestration.app_runtime import (
    build_scan_pipeline_runner,
    close_all_paper_positions as runtime_close_all_paper_positions,
    close_all_paper_positions_for_broker as runtime_close_all_paper_positions_for_broker,
    handle_scan_request as runtime_handle_scan_request,
    handle_sync_paper_trades as runtime_handle_sync_paper_trades,
    run_ibkr_shadow_scans as runtime_run_ibkr_shadow_scans,
    run_scheduled_scan_wrapper as runtime_run_scheduled_scan_wrapper,
)
from orchestration.scheduler_ops import (
    execute_maintenance_ops as build_execute_maintenance_ops,
    execute_ibkr_login_alert as build_execute_ibkr_login_alert,
    execute_pre_close_prep as build_execute_pre_close_prep,
    execute_test_day_cycle as build_execute_test_day_cycle,
    execute_ibkr_vm_control as build_execute_ibkr_vm_control,
    execute_market_ops as build_execute_market_ops,
    execute_post_close_ops as build_execute_post_close_ops,
)
from orchestration.scheduler_runtime import (
    build_ibkr_operational_status,
    get_ibkr_vm_status as scheduler_runtime_get_ibkr_vm_status,
    ibkr_vm_compute_api_request as scheduler_runtime_ibkr_vm_compute_api_request,
    ibkr_vm_settings as scheduler_runtime_ibkr_vm_settings,
    metadata_access_token as scheduler_runtime_metadata_access_token,
    run_ibkr_login_alert_scheduler as scheduler_runtime_run_ibkr_login_alert_scheduler,
    run_ibkr_vm_control_scheduler as scheduler_runtime_run_ibkr_vm_control_scheduler,
    start_ibkr_vm as scheduler_runtime_start_ibkr_vm,
    stop_ibkr_vm as scheduler_runtime_stop_ibkr_vm,
)
from orchestration.persistence_context import (
    ENABLE_DB_LOGGING,
    append_signal_log,
    append_trade_log,
    find_best_signal_match,
    find_latest_open_trade,
    get_managed_open_paper_trades_for_eod_close_for_broker,
    get_open_paper_trades,
    get_risk_exposure_summary,
    infer_first_level_hit,
    is_symbol_in_paper_cooldown,
    paper_trade_exit_already_logged,
    read_trade_rows_for_date,
    safe_insert_broker_order,
    safe_insert_paper_trade_attempt,
    safe_insert_reconciliation_detail,
    safe_insert_reconciliation_run,
    safe_insert_scan_run,
    safe_insert_trade_event,
)
from orchestration.runtime_context import (
    IBKR_PAPER_BROKER,
    IBKR_PAPER_TRADE_MIN_CONFIDENCE,
    _account_equity_from_broker_account,
    close_position,
    close_position_for_broker_name,
    fetch_ibkr_intraday,
    get_current_open_position_state_for_broker,
    get_latest_open_paper_trade_for_symbol_for_broker,
    get_open_orders_for_broker_name,
    get_open_state_for_broker_name,
    get_open_positions,
    get_open_positions_for_broker_name,
    get_risk_exposure_summary_for_broker,
    get_ibkr_shadow_risk_exposure_summary,
    place_ibkr_paper_orders_from_trade,
    refresh_ibkr_mode_rankings,
    resolve_ibkr_account_size,
    resolve_ibkr_scheduled_mode_order,
    resolve_ibkr_shadow_account_size,
    sync_order_by_id,
    sync_order_by_id_for_broker,
    sync_orders_by_ids_for_broker,
)

from routes.sync import register_sync_routes
from services.alert_service import send_telegram_alert, telegram_alerts_enabled
from services.sync_service import execute_sync_paper_trades
from services.ibkr_repair_service import repair_ibkr_stale_closes
from services.ibkr_vm_journal_repair_service import repair_ibkr_stale_closes_from_bridge_journal
from services.scan_service import execute_full_scan
from services.symbol_eligibility_service import (
    refresh_symbol_eligibility_for_date,
    refresh_symbol_eligibility_for_next_session,
)
from services.trade_service import execute_close_all_paper_positions

from analytics.trade_scan import (
    holiday_and_early_close_status,
    run_scan,
    evaluate_symbol,
    market_time_check,
    MIN_CONFIDENCE,
)
from analytics.instruments import get_instrument_groups


app = Flask(__name__)
CORS(app)


def get_ibkr_operational_status() -> dict[str, Any]:
    return build_ibkr_operational_status(
        ibkr_bridge_enabled=ibkr_bridge_enabled,
        ibkr_bridge_get=ibkr_bridge_get,
        account_equity_from_broker_account=_account_equity_from_broker_account,
    )


def find_instrument_by_symbol(symbol: str) -> tuple[str, str] | tuple[None, None]:
    symbol = symbol.strip().upper()
    for mode_name, instruments in get_instrument_groups().items():
        for display_name, info in instruments.items():
            if info.get("symbol", "").upper() == symbol:
                return display_name, mode_name
    return None, None


LOG_BUCKET = os.getenv("LOG_BUCKET", "stock-scanner-490821-logs")
RECONCILIATION_BUCKET = os.getenv("RECONCILIATION_BUCKET", "stock-scanner-490821-logs")
RECONCILIATION_OBJECT = os.getenv("RECONCILIATION_OBJECT", "reports/reconciliation.csv")
TRADE_ANALYSIS_BUCKET = os.getenv("TRADE_ANALYSIS_BUCKET", "stock-scanner-490821-logs")
TRADE_ANALYSIS_SUMMARY_OBJECT = os.getenv("TRADE_ANALYSIS_SUMMARY_OBJECT", "reports/trade_analysis_summary.csv")
TRADE_ANALYSIS_PAIRED_OBJECT = os.getenv("TRADE_ANALYSIS_PAIRED_OBJECT", "reports/trade_analysis_paired_trades.csv")
SIGNAL_ANALYSIS_BUCKET = os.getenv("SIGNAL_ANALYSIS_BUCKET", "stock-scanner-490821-logs")
SIGNAL_ANALYSIS_SUMMARY_OBJECT = os.getenv("SIGNAL_ANALYSIS_SUMMARY_OBJECT", "reports/signal_analysis_summary.csv")
SIGNAL_ANALYSIS_ROWS_OBJECT = os.getenv("SIGNAL_ANALYSIS_ROWS_OBJECT", "reports/signal_analysis_rows.csv")


def paper_candidate_from_evaluation(eval_result: dict, min_confidence: float = IBKR_PAPER_TRADE_MIN_CONFIDENCE) -> dict | None:
    return build_paper_candidate_from_evaluation(eval_result, min_confidence)


execute_scan_pipeline = build_scan_pipeline_runner(
    run_handle_scan_request=run_handle_scan_request,
    execute_full_scan=execute_full_scan,
    market_time_check=market_time_check,
    build_scan_id=build_scan_id,
    market_phase_from_timestamp=market_phase_from_timestamp,
    append_signal_log=append_signal_log,
    safe_insert_paper_trade_attempt=safe_insert_paper_trade_attempt,
    safe_insert_scan_run=safe_insert_scan_run,
    parse_iso_utc=parse_iso_utc,
    trade_to_dict=trade_to_dict,
    debug_to_dict=debug_to_dict,
    evaluate_symbol=evaluate_symbol,
    is_symbol_in_paper_cooldown=is_symbol_in_paper_cooldown,
    append_trade_log=append_trade_log,
    safe_insert_trade_event=safe_insert_trade_event,
    safe_insert_broker_order=safe_insert_broker_order,
    to_float_or_none=to_float_or_none,
    min_confidence=MIN_CONFIDENCE,
    upsert_trade_lifecycle=upsert_trade_lifecycle,
)


def _run_ibkr_shadow_scan(payload: dict[str, Any]) -> dict[str, Any]:
    ibkr_payload = dict(payload)
    try:
        result = execute_scan_pipeline(
            ibkr_payload,
            broker_name="IBKR",
            run_scan_fn=lambda account_size, mode, current_open_positions=0, current_open_exposure=0.0, disable_strategy_gates=False: run_scan(
                account_size,
                mode,
                current_open_positions=current_open_positions,
                current_open_exposure=current_open_exposure,
                disable_strategy_gates=disable_strategy_gates,
                fetch_intraday_fn=fetch_ibkr_intraday,
                source_label=f"IBKR_{mode.upper()}",
            ),
            resolve_account_size_fn=resolve_ibkr_shadow_account_size,
            get_current_open_position_state_fn=lambda: get_current_open_position_state_for_broker(IBKR_PAPER_BROKER),
            get_risk_exposure_summary_fn=lambda: get_ibkr_shadow_risk_exposure_summary(ibkr_payload),
            get_latest_open_trade_fn=lambda symbol: get_latest_open_paper_trade_for_symbol_for_broker(symbol, "IBKR"),
            place_paper_orders_fn=place_ibkr_paper_orders_from_trade,
            paper_candidate_from_evaluation_fn=lambda eval_result: paper_candidate_from_evaluation(
                eval_result,
                min_confidence=IBKR_PAPER_TRADE_MIN_CONFIDENCE,
            ),
        )
        if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
            body, status_code = result
            body = dict(body)
            body.setdefault("status_code", status_code)
            return body
        if isinstance(result, dict):
            return result
        return {
            "ok": False,
            "error": "invalid_ibkr_shadow_scan_result",
            "details": str(type(result)),
            "mode": ibkr_payload.get("mode"),
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": "ibkr_shadow_failed",
            "details": str(exc),
            "mode": ibkr_payload.get("mode"),
        }


def _run_ibkr_shadow_scans(payload: dict[str, Any]) -> dict[str, Any]:
    return runtime_run_ibkr_shadow_scans(
        payload,
        ibkr_scheduled_mode_order=list(IBKR_SCHEDULED_MODE_ORDER),
        run_single_shadow_scan=_run_ibkr_shadow_scan,
    )




_SYNC_RUN_LOCK = threading.Lock()
_SYNC_RUN_STARTED_AT_MONOTONIC: float | None = None


def handle_sync_paper_trades():
    global _SYNC_RUN_STARTED_AT_MONOTONIC

    if not _SYNC_RUN_LOCK.acquire(blocking=False):
        running_for_seconds = None
        if _SYNC_RUN_STARTED_AT_MONOTONIC is not None:
            running_for_seconds = round(
                max(0.0, time.monotonic() - _SYNC_RUN_STARTED_AT_MONOTONIC),
                3,
            )
        return {
            "ok": True,
            "skipped": True,
            "reason": "sync_already_running",
            "running_for_seconds": running_for_seconds,
        }

    _SYNC_RUN_STARTED_AT_MONOTONIC = time.monotonic()
    try:
        result = runtime_handle_sync_paper_trades(
            run_handle_sync_paper_trades=run_handle_sync_paper_trades,
            execute_sync_paper_trades=execute_sync_paper_trades,
            get_open_paper_trades=get_open_paper_trades,
            sync_order_by_id=sync_order_by_id,
            sync_order_by_id_for_broker=sync_order_by_id_for_broker,
            sync_orders_by_ids_for_broker=sync_orders_by_ids_for_broker,
            paper_trade_exit_already_logged=paper_trade_exit_already_logged,
            append_trade_log=append_trade_log,
            safe_insert_trade_event=safe_insert_trade_event,
            safe_insert_broker_order=safe_insert_broker_order,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            get_open_positions=get_open_positions,
            close_position=close_position,
            get_open_positions_for_broker_name=get_open_positions_for_broker_name,
            get_open_state_for_broker_name=get_open_state_for_broker_name,
            close_position_for_broker_name=close_position_for_broker_name,
        )
        body = result[0] if isinstance(result, tuple) and len(result) == 2 else result
        status_code = result[1] if isinstance(result, tuple) and len(result) == 2 else None

        auto_repair_enabled = str(os.getenv("AUTO_IBKR_REPAIR_AFTER_PARTIAL_SYNC", "true")).strip().lower() in {"1", "true", "yes", "on"}
        if isinstance(body, dict) and auto_repair_enabled:
            partial = bool(body.get("partial"))
            stopped_reason = str(body.get("stopped_reason", "") or "").strip().lower()
            if partial and stopped_reason.startswith("ibkr_"):
                target_date = datetime.now(NY_TZ).date().isoformat()
                try:
                    repair_result = run_ibkr_stale_close_repair(target_date=target_date)
                    body["post_sync_repair"] = repair_result
                    log_info(
                        "Automatic IBKR stale-close repair completed after partial sync",
                        component="app",
                        operation="handle_sync_paper_trades",
                        target_date=target_date,
                        stopped_reason=stopped_reason,
                        repaired_count=(repair_result or {}).get("repaired_count"),
                        skipped_count=(repair_result or {}).get("skipped_count"),
                    )
                except Exception as repair_error:
                    body["post_sync_repair"] = {"ok": False, "error": str(repair_error), "target_date": target_date}
                    log_info(
                        "Automatic IBKR stale-close repair failed after partial sync",
                        component="app",
                        operation="handle_sync_paper_trades",
                        target_date=target_date,
                        stopped_reason=stopped_reason,
                        error=str(repair_error),
                    )

        return (body, status_code) if status_code is not None else body
    finally:
        _SYNC_RUN_STARTED_AT_MONOTONIC = None
        _SYNC_RUN_LOCK.release()

def handle_scan_request(payload):
    return runtime_handle_scan_request(
        payload,
        run_ibkr_scan=lambda scan_payload: execute_scan_pipeline(
            scan_payload,
            broker_name="IBKR",
            run_scan_fn=fetch_ibkr_intraday,
            resolve_account_size_fn=resolve_ibkr_account_size,
            get_current_open_position_state_fn=lambda: get_current_open_position_state_for_broker(IBKR_PAPER_BROKER),
            get_risk_exposure_summary_fn=lambda: get_risk_exposure_summary_for_broker(IBKR_PAPER_BROKER),
            get_latest_open_trade_fn=lambda symbol: get_latest_open_paper_trade_for_symbol_for_broker(symbol, "IBKR"),
            place_paper_orders_fn=place_ibkr_paper_orders_from_trade,
            paper_candidate_from_evaluation_fn=lambda eval_result: paper_candidate_from_evaluation(
                eval_result,
                min_confidence=IBKR_PAPER_TRADE_MIN_CONFIDENCE,
            ),
        ),
        shadow_mode_enabled=False,
        ibkr_bridge_enabled=ibkr_bridge_enabled(),
        run_ibkr_shadow_scans=_run_ibkr_shadow_scans,
    )


def run_scan_wrapper(payload):
    return handle_scan_request(payload)


def run_scheduled_paper_scan_wrapper(payload):
    now_ny = datetime.now(NY_TZ)
    scheduled_mode_order = resolve_ibkr_scheduled_mode_order()
    return runtime_run_scheduled_scan_wrapper(
        payload,
        now_ny=now_ny,
        run_scheduled_scan_wrapper_fn=run_scheduled_scan_wrapper,
        build_scheduled_scan_payload=build_scheduled_scan_payload,
        scheduled_mode_order=scheduled_mode_order,
        handle_scan_request_fn=handle_scan_request,
    )






def _close_all_paper_positions_for_broker(broker) -> dict[str, Any] | tuple[dict[str, Any], int]:
    return runtime_close_all_paper_positions_for_broker(
        broker,
        run_close_all_paper_positions=run_close_all_paper_positions,
        execute_close_all_paper_positions=execute_close_all_paper_positions,
        get_managed_open_paper_trades_for_eod_close_for_broker=get_managed_open_paper_trades_for_eod_close_for_broker,
        safe_insert_broker_order=safe_insert_broker_order,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        to_float_or_none=to_float_or_none,
        parse_iso_utc=parse_iso_utc,
    )


def close_all_paper_positions():
    return runtime_close_all_paper_positions(
        ibkr_broker=IBKR_PAPER_BROKER,
        shadow_mode_enabled=False,
        ibkr_bridge_enabled=ibkr_bridge_enabled(),
        close_all_paper_positions_for_broker_fn=_close_all_paper_positions_for_broker,
    )



def run_market_ops_scheduler(*, now_ny: datetime):
    return build_execute_market_ops(
        now_ny=now_ny,
        run_sync=handle_sync_paper_trades,
        run_scan=run_scheduled_paper_scan_wrapper,
        run_close=close_all_paper_positions,
        run_health_probe=get_ibkr_operational_status,
        run_pre_close_prep=lambda: build_execute_pre_close_prep(
            now_ny=now_ny,
            get_ibkr_operational_status=get_ibkr_operational_status,
        ),
    )


def run_daily_post_close_scheduler(*, now_ny: datetime):
    return build_execute_post_close_ops(
        now_ny=now_ny,
        run_sync=handle_sync_paper_trades,
        run_symbol_eligibility_refresh=lambda: refresh_symbol_eligibility_for_next_session(
            now_ny=now_ny,
            fetch_intraday_fn=fetch_ibkr_intraday,
        ),
        run_ibkr_stale_close_repair=lambda: repair_ibkr_stale_closes(
            target_date=now_ny.date().isoformat(),
            get_stale_ibkr_closed_trade_lifecycles=get_stale_ibkr_closed_trade_lifecycles,
            sync_order_by_id_for_broker=sync_order_by_id_for_broker,
            get_latest_exit_trade_event_for_parent_order_id=get_latest_exit_trade_event_for_parent_order_id,
            upsert_trade_lifecycle=upsert_trade_lifecycle,
            safe_insert_broker_order=safe_insert_broker_order,
            parse_iso_utc=parse_iso_utc,
            to_float_or_none=to_float_or_none,
        ),
        run_reconcile=lambda: {"ok": True, "skipped": True, "reason": "reconciliation_disabled_in_ibkr_only_mode"},
        run_trade_analysis=run_trade_analysis,
        run_signal_analysis=run_signal_analysis,
        run_snapshot_export=run_daily_snapshot,
        run_mode_ranking_refresh=lambda: refresh_ibkr_mode_rankings(ranking_date=now_ny.date().isoformat()),
    )


def run_test_day_cycle_scheduler(*, now_ny: datetime, payload: dict[str, Any]):
    return build_execute_test_day_cycle(
        now_ny=now_ny,
        payload=payload,
        run_scan=handle_scan_request,
        run_sync=handle_sync_paper_trades,
        run_close=close_all_paper_positions,
        run_post_close_ops=lambda: run_daily_post_close_scheduler(now_ny=datetime.now(NY_TZ)),
        sleep_fn=time.sleep,
    )


def run_ibkr_stale_close_repair(*, target_date: str):
    return repair_ibkr_stale_closes(
        target_date=target_date,
        get_stale_ibkr_closed_trade_lifecycles=get_stale_ibkr_closed_trade_lifecycles,
        sync_order_by_id_for_broker=sync_order_by_id_for_broker,
        get_latest_exit_trade_event_for_parent_order_id=get_latest_exit_trade_event_for_parent_order_id,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        safe_insert_broker_order=safe_insert_broker_order,
        parse_iso_utc=parse_iso_utc,
        to_float_or_none=to_float_or_none,
    )


def run_ibkr_vm_journal_repair(
    *,
    target_date: str,
    since: str | None = None,
    until: str | None = None,
    year: int | None = None,
    apply_changes: bool = True,
):
    def fetch_bridge_journal_lines(*, since: str, until: str) -> list[str]:
        payload = ibkr_bridge_get(
            "/journal",
            params={"since": since, "until": until, "unit": "ibkr-bridge"},
            timeout=30,
        ) or {}
        return list(payload.get("lines") or [])

    return repair_ibkr_stale_closes_from_bridge_journal(
        target_date=target_date,
        get_stale_ibkr_closed_trade_lifecycles=get_stale_ibkr_closed_trade_lifecycles,
        fetch_bridge_journal_lines=fetch_bridge_journal_lines,
        since=since,
        until=until,
        year=year,
        apply_changes=apply_changes,
    )


def run_maintenance_scheduler(*, now_ny: datetime, retention_days: int = 30):
    return build_execute_maintenance_ops(
        now_ny=now_ny,
        prune_logs=lambda _days: 0,
        prune_operational_data=prune_operational_data,
        retention_days=retention_days,
    )


def _metadata_access_token() -> str:
    return scheduler_runtime_metadata_access_token()


def _ibkr_vm_settings() -> tuple[str, str, str]:
    return scheduler_runtime_ibkr_vm_settings()


def _ibkr_vm_compute_api_request(method: str, suffix: str) -> dict:
    return scheduler_runtime_ibkr_vm_compute_api_request(
        method,
        suffix,
        metadata_access_token_fn=_metadata_access_token,
        ibkr_vm_settings_fn=_ibkr_vm_settings,
        requests_module=requests,
    )


def _get_ibkr_vm_status() -> str | None:
    return scheduler_runtime_get_ibkr_vm_status(compute_api_request_fn=_ibkr_vm_compute_api_request)


def _start_ibkr_vm() -> dict:
    return scheduler_runtime_start_ibkr_vm(
        log_info=log_info,
        compute_api_request_fn=_ibkr_vm_compute_api_request,
    )


def _stop_ibkr_vm() -> dict:
    return scheduler_runtime_stop_ibkr_vm(
        log_info=log_info,
        compute_api_request_fn=_ibkr_vm_compute_api_request,
    )


def run_ibkr_vm_control_scheduler(*, now_ny: datetime, action: str, force: bool = False):
    return scheduler_runtime_run_ibkr_vm_control_scheduler(
        now_ny=now_ny,
        action=action,
        force=force,
        holiday_and_early_close_status=holiday_and_early_close_status,
        execute_ibkr_vm_control=build_execute_ibkr_vm_control,
        get_instance_status=_get_ibkr_vm_status,
        start_instance=_start_ibkr_vm,
        stop_instance=_stop_ibkr_vm,
    )


def run_ibkr_login_alert_scheduler(*, now_ny: datetime):
    return scheduler_runtime_run_ibkr_login_alert_scheduler(
        now_ny=now_ny,
        execute_ibkr_login_alert=build_execute_ibkr_login_alert,
        get_ibkr_operational_status=get_ibkr_operational_status,
        telegram_alerts_enabled=telegram_alerts_enabled(),
        send_telegram_alert=send_telegram_alert,
    )


def run_symbol_eligibility_refresh(payload: dict[str, Any] | None = None):
    request_payload = payload or {}
    current_ny = datetime.now(NY_TZ)

    raw_modes = request_payload.get("modes")
    if isinstance(raw_modes, str):
        modes = [token.strip().lower() for token in raw_modes.split(",") if token.strip()]
    elif isinstance(raw_modes, list):
        modes = [str(token).strip().lower() for token in raw_modes if str(token).strip()]
    else:
        modes = None

    target_session_date = str(request_payload.get("target_session_date", "")).strip()
    if target_session_date:
        return refresh_symbol_eligibility_for_date(
            target_session_date=target_session_date,
            fetch_intraday_fn=fetch_ibkr_intraday,
            modes=modes,
        )

    return refresh_symbol_eligibility_for_next_session(
        now_ny=current_ny,
        fetch_intraday_fn=fetch_ibkr_intraday,
        modes=modes,
    )


register_health_routes(
    app,
    db_healthcheck=db_healthcheck,
    enable_db_logging=ENABLE_DB_LOGGING,
    get_ops_summary=get_ops_summary,
    get_recent_paper_trade_attempts=get_recent_paper_trade_attempts,
    get_recent_paper_trade_rejections=get_recent_paper_trade_rejections,
    get_paper_trade_attempt_daily_summary=get_paper_trade_attempt_daily_summary,
    get_paper_trade_attempt_hourly_summary=get_paper_trade_attempt_hourly_summary,
    get_ibkr_operational_status=get_ibkr_operational_status,
    telegram_alerts_enabled=telegram_alerts_enabled,
    send_telegram_alert=send_telegram_alert,
    purge_all_test_data=purge_all_test_data,
    purge_legacy_broker_data=purge_legacy_broker_data,
    run_symbol_eligibility_refresh=run_symbol_eligibility_refresh,
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
    run_reconciliation=lambda: {"ok": True, "skipped": True, "reason": "reconciliation_disabled_in_ibkr_only_mode"},
    upload_file_to_gcs=lambda *_args, **_kwargs: None,
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
    get_trade_lifecycles_page=get_trade_lifecycles_page,
    get_trade_lifecycle_summary_from_table=get_trade_lifecycle_summary_from_table,
    get_open_positions_for_broker_name=get_open_positions_for_broker_name,
    get_open_orders_for_broker_name=get_open_orders_for_broker_name,
    get_open_state_for_broker_name=get_open_state_for_broker_name,
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
    get_risk_exposure_summary=get_risk_exposure_summary,
)
register_scheduler_routes(
    app,
    ny_tz=NY_TZ,
    execute_market_ops=run_market_ops_scheduler,
    execute_post_close_ops=run_daily_post_close_scheduler,
    execute_maintenance_ops=run_maintenance_scheduler,
    execute_ibkr_vm_control=run_ibkr_vm_control_scheduler,
    execute_ibkr_login_alert=run_ibkr_login_alert_scheduler,
    execute_ibkr_stale_close_repair=run_ibkr_stale_close_repair,
    execute_ibkr_vm_journal_repair=run_ibkr_vm_journal_repair,
    execute_test_day_cycle=run_test_day_cycle_scheduler,
)
register_legacy_reconcile_routes(
    app,
    build_reconcile_now_response=build_reconcile_now_response,
    build_reconciliation_runs_response=build_reconciliation_runs_response,
    run_reconciliation=lambda: {"ok": True, "skipped": True, "reason": "reconciliation_disabled_in_ibkr_only_mode"},
    upload_file_to_gcs=lambda *_args, **_kwargs: None,
    reconciliation_bucket=RECONCILIATION_BUCKET,
    reconciliation_object=RECONCILIATION_OBJECT,
    safe_insert_reconciliation_run=safe_insert_reconciliation_run,
    get_reconciliation_runs=get_reconciliation_runs,
)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
