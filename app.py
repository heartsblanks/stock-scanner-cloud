import os
from datetime import datetime
from typing import Any

from flask import Flask
from flask_cors import CORS
import requests
from core.logging_utils import log_exception
from core.logging_utils import log_info
from core.logging_utils import log_warning
from alpaca.reconcile import run_reconciliation, upload_file_to_gcs
from analytics.trade_analysis import run_trade_analysis, upload_file_to_gcs as upload_analysis_file_to_gcs
from analytics.signal_analysis import run_signal_analysis, upload_file_to_gcs as upload_signal_analysis_file_to_gcs
from brokers import get_paper_broker, get_paper_broker_config
from brokers.alpaca_adapter import AlpacaPaperBroker
from brokers.ibkr_adapter import IbkrPaperBroker
from brokers.ibkr_bridge_client import ibkr_bridge_enabled, ibkr_bridge_get
from core.db import healthcheck as db_healthcheck
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
    get_latest_mode_ranking_order,
    prune_alpaca_api_logs,
    refresh_mode_rankings,
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
from orchestration.paper_trade_context import (
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
from orchestration.scan_context import (
    ALPACA_SCHEDULED_MODE_ORDER,
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
from orchestration.scheduler_ops import (
    execute_maintenance_ops as build_execute_maintenance_ops,
    execute_ibkr_vm_control as build_execute_ibkr_vm_control,
    execute_market_ops as build_execute_market_ops,
    execute_post_close_ops as build_execute_post_close_ops,
)

from routes.sync import register_sync_routes
from services.sync_service import execute_sync_paper_trades
from services.scan_service import execute_full_scan
from services.trade_service import execute_close_all_paper_positions

from analytics.trade_scan import (
    holiday_and_early_close_status,
    run_scan,
    evaluate_symbol,
    market_time_check,
    MIN_CONFIDENCE,
)
from analytics.instruments import INSTRUMENT_GROUPS
PAPER_TRADE_MIN_CONFIDENCE = 70
IBKR_PAPER_TRADE_MIN_CONFIDENCE = int(os.getenv("IBKR_PAPER_TRADE_MIN_CONFIDENCE", "40"))
ALPACA_MODE_RANKING_WINDOW_DAYS = max(1, int(os.getenv("ALPACA_MODE_RANKING_WINDOW_DAYS", "5")))
ALPACA_MODE_RANKING_MIN_CLOSED_TRADES = max(1, int(os.getenv("ALPACA_MODE_RANKING_MIN_CLOSED_TRADES", "2")))


app = Flask(__name__)
CORS(app)
PAPER_BROKER = get_paper_broker()
PAPER_BROKER_CONFIG = get_paper_broker_config()
ALPACA_PAPER_BROKER = AlpacaPaperBroker()
IBKR_PAPER_BROKER = IbkrPaperBroker()

place_paper_bracket_order_from_trade = PAPER_BROKER.place_paper_bracket_order_from_trade
get_open_positions = PAPER_BROKER.get_open_positions
close_position = PAPER_BROKER.close_position
cancel_open_orders_for_symbol = PAPER_BROKER.cancel_open_orders_for_symbol
sync_order_by_id = PAPER_BROKER.sync_order_by_id
get_order_by_id = PAPER_BROKER.get_order_by_id


def _broker_instance_by_name(broker_name: str):
    normalized = str(broker_name or "").strip().upper()
    if normalized == "IBKR":
        return IBKR_PAPER_BROKER
    return ALPACA_PAPER_BROKER


def sync_order_by_id_for_broker(broker_name: str, order_id: str) -> dict[str, Any]:
    broker = _broker_instance_by_name(broker_name)
    return broker.sync_order_by_id(order_id)


def get_open_positions_for_broker_name(broker_name: str) -> list[dict[str, Any]]:
    broker = _broker_instance_by_name(broker_name)
    return broker.get_open_positions()


def close_position_for_broker_name(broker_name: str, symbol: str):
    broker = _broker_instance_by_name(broker_name)
    return broker.close_position(symbol)


def place_paper_orders_from_trade(trade: dict[str, Any], max_notional: float | None = None) -> list[dict]:
    results: list[dict] = []

    primary_result = ALPACA_PAPER_BROKER.place_paper_bracket_order_from_trade(trade, max_notional=max_notional)
    if isinstance(primary_result, dict):
        primary_result.setdefault("broker", "ALPACA")
        results.append(primary_result)

    if PAPER_BROKER_CONFIG.shadow_mode_enabled and ibkr_bridge_enabled():
        try:
            ibkr_result = IBKR_PAPER_BROKER.place_paper_bracket_order_from_trade(trade, max_notional=max_notional)
        except Exception as exc:
            ibkr_result = {
                "attempted": True,
                "placed": False,
                "broker": "IBKR",
                "reason": "ibkr_shadow_exception",
                "details": str(exc),
            }
        if isinstance(ibkr_result, dict):
            ibkr_result.setdefault("broker", "IBKR")
            results.append(ibkr_result)

    return results


def place_alpaca_paper_orders_from_trade(trade: dict[str, Any], max_notional: float | None = None) -> list[dict]:
    result = ALPACA_PAPER_BROKER.place_paper_bracket_order_from_trade(trade, max_notional=max_notional)
    if isinstance(result, dict):
        result.setdefault("broker", "ALPACA")
        return [result]
    return []


def place_ibkr_paper_orders_from_trade(trade: dict[str, Any], max_notional: float | None = None) -> list[dict]:
    try:
        result = IBKR_PAPER_BROKER.place_paper_bracket_order_from_trade(trade, max_notional=max_notional)
    except Exception as exc:
        result = {
            "attempted": True,
            "placed": False,
            "broker": "IBKR",
            "reason": "ibkr_shadow_exception",
            "details": str(exc),
        }
    if isinstance(result, dict):
        result.setdefault("broker", "IBKR")
        return [result]
    return []


def _account_equity_from_broker_account(account: dict[str, Any] | None) -> float:
    if not isinstance(account, dict):
        return 0.0
    try:
        return float(account.get("equity") or 0.0)
    except Exception:
        return 0.0


def resolve_alpaca_account_size(payload: dict[str, Any]) -> float:
    account = ALPACA_PAPER_BROKER.get_account()
    equity = _account_equity_from_broker_account(account)
    if equity > 0:
        return equity
    raise ValueError("Unable to resolve Alpaca account equity")


def resolve_ibkr_account_size(payload: dict[str, Any]) -> float:
    fallback = to_float_or_none(
        payload.get("ibkr_account_size")
        or payload.get("shadow_account_size")
        or os.getenv("IBKR_SHADOW_ACCOUNT_SIZE_FALLBACK")
        or "1000000"
    )
    try:
        account = IBKR_PAPER_BROKER.get_account()
        equity = _account_equity_from_broker_account(account)
        if equity > 0:
            return equity
    except Exception as exc:
        log_exception(
            "Failed to resolve IBKR account equity; using fallback",
            exc,
            component="app",
            operation="resolve_ibkr_account_size",
        )
    if fallback is not None and fallback > 0:
        return float(fallback)
    raise ValueError("Unable to resolve IBKR account equity")


def resolve_ibkr_shadow_account_size(payload: dict[str, Any]) -> float:
    fallback = to_float_or_none(
        payload.get("ibkr_account_size")
        or payload.get("shadow_account_size")
        or os.getenv("IBKR_SHADOW_ACCOUNT_SIZE_FALLBACK")
        or "1000000"
    )
    if fallback is not None and fallback > 0:
        return float(fallback)
    return 1000000.0


def resolve_alpaca_scheduled_mode_order() -> list[str]:
    try:
        ranked_modes = get_latest_mode_ranking_order(
            broker="ALPACA",
            expected_modes=ALPACA_SCHEDULED_MODE_ORDER,
            window_days=ALPACA_MODE_RANKING_WINDOW_DAYS,
        )
        if ranked_modes:
            return ranked_modes
    except Exception as exc:
        log_exception(
            "Failed to resolve latest Alpaca mode ranking order; falling back to static order",
            exc,
            component="app",
            operation="resolve_alpaca_scheduled_mode_order",
        )
    return list(ALPACA_SCHEDULED_MODE_ORDER)


def refresh_alpaca_mode_rankings(*, ranking_date: str | None = None) -> dict[str, Any]:
    result = refresh_mode_rankings(
        broker="ALPACA",
        expected_modes=ALPACA_SCHEDULED_MODE_ORDER,
        window_days=ALPACA_MODE_RANKING_WINDOW_DAYS,
        as_of_date=ranking_date,
        min_closed_trade_count=ALPACA_MODE_RANKING_MIN_CLOSED_TRADES,
    )
    return {
        "ok": True,
        "message": "alpaca mode rankings refreshed",
        **result,
    }


def get_ibkr_operational_status() -> dict[str, Any]:
    status: dict[str, Any] = {
        "ok": True,
        "enabled": ibkr_bridge_enabled(),
        "state": "DISABLED",
        "bridge_health_ok": False,
        "account_ok": False,
        "market_data_ok": False,
        "login_required": False,
        "message": "",
        "bridge": None,
        "account_id": "",
        "equity": None,
        "market_data_symbol": os.getenv("IBKR_READINESS_SYMBOL", "SPY").strip().upper() or "SPY",
        "market_data_count": 0,
        "errors": [],
    }

    if not status["enabled"]:
        status["message"] = "IBKR bridge is not configured."
        return status

    bridge_timeout = int(os.getenv("IBKR_BRIDGE_HEALTH_TIMEOUT_SECONDS", "4"))
    account_timeout = int(os.getenv("IBKR_BRIDGE_ACCOUNT_TIMEOUT_SECONDS", "5"))
    market_timeout = int(os.getenv("IBKR_BRIDGE_STATUS_MARKET_DATA_TIMEOUT_SECONDS", "8"))

    try:
        bridge_payload = ibkr_bridge_get("/health", timeout=bridge_timeout) or {}
        status["bridge_health_ok"] = bool(bridge_payload.get("ok"))
        status["bridge"] = bridge_payload.get("ibkr")
    except Exception as exc:
        status["errors"].append(f"bridge_health: {exc}")
        status["state"] = "UNAVAILABLE"
        status["message"] = "IBKR bridge is not reachable."
        return status

    try:
        account_payload = ibkr_bridge_get("/account", timeout=account_timeout) or {}
        equity = _account_equity_from_broker_account(account_payload)
        status["account_ok"] = bool(account_payload.get("account_id"))
        status["account_id"] = str(account_payload.get("account_id", "") or "")
        status["equity"] = equity if equity > 0 else None
    except Exception as exc:
        status["errors"].append(f"account: {exc}")

    try:
        candles = ibkr_bridge_get(
            "/market-data/intraday",
            params={"symbol": status["market_data_symbol"], "interval": "1min", "outputsize": 5},
            timeout=market_timeout,
        ) or []
        status["market_data_count"] = len(candles)
        status["market_data_ok"] = len(candles) > 0
    except Exception as exc:
        status["errors"].append(f"market_data: {exc}")

    if status["bridge_health_ok"] and status["account_ok"] and status["market_data_ok"]:
        status["state"] = "READY"
        status["message"] = "IBKR bridge, account, and market data checks passed."
        return status

    status["login_required"] = True
    if status["bridge_health_ok"] and not status["account_ok"]:
        status["state"] = "LOGIN_REQUIRED"
        status["message"] = "IBKR bridge is up, but the account session is not ready."
    elif status["bridge_health_ok"] and status["account_ok"] and not status["market_data_ok"]:
        status["state"] = "MARKET_DATA_UNAVAILABLE"
        status["message"] = "IBKR account is up, but market data is not ready."
    else:
        status["state"] = "DEGRADED"
        status["message"] = "IBKR bridge is partially available but not ready for scans."
    return status


def get_current_open_position_state_for_broker(broker) -> tuple[int, float]:
    try:
        positions = broker.get_open_positions() or []
    except Exception:
        return 0, 0.0

    open_count = 0
    open_exposure = 0.0
    for position in positions:
        symbol = str(position.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        open_count += 1
        market_value = to_float_or_none(position.get("market_value"))
        if market_value is not None:
            open_exposure += abs(market_value)
            continue
        qty = to_float_or_none(position.get("qty"))
        current_price = to_float_or_none(position.get("current_price"))
        if qty is not None and current_price is not None:
            open_exposure += abs(qty * current_price)
    return open_count, open_exposure


def get_risk_exposure_summary_for_broker(broker) -> dict[str, Any]:
    account_size = 0.0
    try:
        account_size = _account_equity_from_broker_account(broker.get_account())
    except Exception as exc:
        log_exception(
            "Failed to resolve broker account for risk summary",
            exc,
            component="app",
            operation="get_risk_exposure_summary_for_broker",
        )
    open_count, open_exposure = get_current_open_position_state_for_broker(broker)
    return {
        "account_size": account_size,
        "open_position_count": open_count,
        "total_open_exposure": open_exposure,
        "daily_realized_pnl": 0.0,
        "daily_unrealized_pnl": 0.0,
    }


def get_ibkr_shadow_risk_exposure_summary(payload: dict[str, Any]) -> dict[str, Any]:
    open_count, open_exposure = get_current_open_position_state_for_broker(IBKR_PAPER_BROKER)
    return {
        "account_size": resolve_ibkr_shadow_account_size(payload),
        "open_position_count": open_count,
        "total_open_exposure": open_exposure,
        "daily_realized_pnl": 0.0,
        "daily_unrealized_pnl": 0.0,
    }


def get_latest_open_paper_trade_for_symbol_for_broker(symbol: str, broker_name: str) -> dict | None:
    return context_get_latest_open_paper_trade_for_symbol(symbol, broker=broker_name)


def fetch_ibkr_intraday(symbol: str, interval: str = "1min", outputsize: int | None = None) -> list[dict]:
    params: dict[str, Any] = {"symbol": symbol, "interval": interval}
    if outputsize is not None:
        params["outputsize"] = int(outputsize)
    timeout_seconds = int(os.getenv("IBKR_BRIDGE_MARKET_DATA_TIMEOUT_SECONDS", "12"))
    log_info(
        "IBKR intraday fetch requested",
        component="app",
        operation="fetch_ibkr_intraday",
        broker="IBKR",
        symbol=symbol,
        interval=interval,
        outputsize=outputsize,
        timeout=timeout_seconds,
    )
    try:
        candles = ibkr_bridge_get(
            "/market-data/intraday",
            params=params,
            timeout=timeout_seconds,
        ) or []
        if not candles:
            log_warning(
                "IBKR intraday fetch returned no candles",
                component="app",
                operation="fetch_ibkr_intraday",
                broker="IBKR",
                symbol=symbol,
                interval=interval,
                outputsize=outputsize,
                timeout=timeout_seconds,
                duration="2 D",
                bar_size=("1 min" if str(interval).strip().lower() == "1min" else "5 mins" if str(interval).strip().lower() == "5min" else None),
                what_to_show="TRADES",
                use_rth=True,
            )
        log_info(
            "IBKR intraday fetch completed",
            component="app",
            operation="fetch_ibkr_intraday",
            broker="IBKR",
            symbol=symbol,
            interval=interval,
            outputsize=outputsize,
            candle_count=len(candles),
            last_bar_datetime=(candles[-1].get("datetime") if candles else None),
        )
        return candles
    except Exception as exc:
        log_exception(
            "IBKR intraday fetch failed",
            exc,
            component="app",
            operation="fetch_ibkr_intraday",
            broker="IBKR",
            symbol=symbol,
            interval=interval,
            outputsize=outputsize,
            timeout=timeout_seconds,
        )
        raise


def env_flag(name: str, default: str = "true") -> bool:
    value = str(os.getenv(name, default)).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}

ENABLE_DB_LOGGING = env_flag("ENABLE_DB_LOGGING", "true")

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
def paper_candidate_from_evaluation(eval_result: dict, min_confidence: float = PAPER_TRADE_MIN_CONFIDENCE) -> dict | None:
    return build_paper_candidate_from_evaluation(eval_result, min_confidence)


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


def get_managed_open_paper_trades_for_eod_close_for_broker(broker) -> list[dict]:
    return context_get_managed_open_paper_trades_for_eod_close(broker=broker)


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


def execute_scan_pipeline(
    payload: dict[str, Any],
    *,
    broker_name: str,
    run_scan_fn,
    resolve_account_size_fn,
    get_current_open_position_state_fn,
    get_risk_exposure_summary_fn,
    get_latest_open_trade_fn,
    place_paper_orders_fn,
    paper_trade_min_confidence: float = PAPER_TRADE_MIN_CONFIDENCE,
):
    return run_handle_scan_request(
        payload,
        get_current_open_position_state=get_current_open_position_state_fn,
        get_risk_exposure_summary=get_risk_exposure_summary_fn,
        execute_full_scan=execute_full_scan,
        market_time_check=market_time_check,
        build_scan_id=build_scan_id,
        market_phase_from_timestamp=market_phase_from_timestamp,
        append_signal_log=append_signal_log,
        safe_insert_paper_trade_attempt=safe_insert_paper_trade_attempt,
        safe_insert_scan_run=safe_insert_scan_run,
        parse_iso_utc=parse_iso_utc,
        run_scan=run_scan_fn,
        trade_to_dict=trade_to_dict,
        debug_to_dict=debug_to_dict,
        paper_candidate_from_evaluation=lambda eval_result: paper_candidate_from_evaluation(
            eval_result,
            min_confidence=paper_trade_min_confidence,
        ),
        evaluate_symbol=evaluate_symbol,
        get_latest_open_paper_trade_for_symbol=get_latest_open_trade_fn,
        is_symbol_in_paper_cooldown=is_symbol_in_paper_cooldown,
        place_paper_orders_from_trade=place_paper_orders_fn,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        safe_insert_broker_order=safe_insert_broker_order,
        to_float_or_none=to_float_or_none,
        min_confidence=MIN_CONFIDENCE,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        resolve_account_size=resolve_account_size_fn,
        active_broker=broker_name,
    )


def _run_ibkr_shadow_scan(payload: dict[str, Any]) -> dict[str, Any]:
    ibkr_payload = dict(payload)
    try:
        return execute_scan_pipeline(
            ibkr_payload,
            broker_name="IBKR",
            run_scan_fn=lambda account_size, mode, current_open_positions=0, current_open_exposure=0.0: run_scan(
                account_size,
                mode,
                current_open_positions=current_open_positions,
                current_open_exposure=current_open_exposure,
                fetch_intraday_fn=fetch_ibkr_intraday,
                source_label=f"IBKR_{mode.upper()}",
            ),
            resolve_account_size_fn=resolve_ibkr_shadow_account_size,
            get_current_open_position_state_fn=lambda: get_current_open_position_state_for_broker(IBKR_PAPER_BROKER),
            get_risk_exposure_summary_fn=lambda: get_ibkr_shadow_risk_exposure_summary(ibkr_payload),
            get_latest_open_trade_fn=lambda symbol: get_latest_open_paper_trade_for_symbol_for_broker(symbol, "IBKR"),
            place_paper_orders_fn=place_ibkr_paper_orders_from_trade,
            paper_trade_min_confidence=IBKR_PAPER_TRADE_MIN_CONFIDENCE,
        )
    except Exception as exc:
        return {
            "ok": False,
            "error": "ibkr_shadow_failed",
            "details": str(exc),
            "mode": ibkr_payload.get("mode"),
        }


def _run_ibkr_shadow_scans(payload: dict[str, Any]) -> dict[str, Any]:
    scan_source = str((payload or {}).get("scan_source", "") or "").strip().upper()
    scheduled_shadow = scan_source == "SCHEDULED"
    modes = list(IBKR_SCHEDULED_MODE_ORDER) if scheduled_shadow else [str(payload.get("mode", "primary")).strip().lower()]

    per_mode_results: list[dict[str, Any]] = []
    total_candidates = 0
    total_placed = 0
    total_skipped = 0

    for mode in modes:
        mode_payload = dict(payload)
        mode_payload["mode"] = mode
        mode_response = _run_ibkr_shadow_scan(mode_payload)
        mode_result = mode_response if isinstance(mode_response, dict) else {"ok": False, "error": "invalid_ibkr_shadow_response", "mode": mode}
        mode_result.setdefault("mode", mode)
        per_mode_results.append(mode_result)

        total_candidates += int(float(mode_result.get("candidate_count", 0) or 0))
        total_placed += int(float(mode_result.get("placed_count", 0) or 0))
        total_skipped += int(float(mode_result.get("skipped_count", 0) or 0))

        log_info(
            "IBKR shadow scan completed",
            component="app",
            operation="handle_scan_request",
            broker="IBKR",
            ok=bool(mode_result.get("ok", False)),
            mode=mode,
            error=mode_result.get("error"),
            candidate_count=mode_result.get("candidate_count"),
            placed_count=mode_result.get("placed_count"),
            skipped_count=mode_result.get("skipped_count"),
            scan_id=mode_result.get("scan_id"),
        )

    return {
        "ok": all(bool(result.get("ok", False)) for result in per_mode_results),
        "scheduled_all_modes": scheduled_shadow,
        "mode_count": len(modes),
        "modes": modes,
        "candidate_count": total_candidates,
        "placed_count": total_placed,
        "skipped_count": total_skipped,
        "per_mode_results": per_mode_results,
    }




def handle_sync_paper_trades():
    return run_handle_sync_paper_trades(
        execute_sync_paper_trades=execute_sync_paper_trades,
        get_open_paper_trades=get_open_paper_trades,
        sync_order_by_id=sync_order_by_id,
        sync_order_by_id_for_broker=sync_order_by_id_for_broker,
        paper_trade_exit_already_logged=paper_trade_exit_already_logged,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        safe_insert_broker_order=safe_insert_broker_order,
        parse_iso_utc=parse_iso_utc,
        to_float_or_none=to_float_or_none,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        get_open_positions=get_open_positions,
        close_position=close_position,
        get_open_positions_for_broker=get_open_positions_for_broker_name,
        close_position_for_broker=close_position_for_broker_name,
    )

def handle_scan_request(payload):
    alpaca_response = execute_scan_pipeline(
        payload,
        broker_name="ALPACA",
        run_scan_fn=run_scan,
        resolve_account_size_fn=resolve_alpaca_account_size,
        get_current_open_position_state_fn=lambda: get_current_open_position_state_for_broker(ALPACA_PAPER_BROKER),
        get_risk_exposure_summary_fn=lambda: get_risk_exposure_summary_for_broker(ALPACA_PAPER_BROKER),
        get_latest_open_trade_fn=lambda symbol: get_latest_open_paper_trade_for_symbol_for_broker(symbol, "ALPACA"),
        place_paper_orders_fn=place_alpaca_paper_orders_from_trade,
    )

    if not (isinstance(payload, dict) and payload.get("paper_trade") and PAPER_BROKER_CONFIG.shadow_mode_enabled and ibkr_bridge_enabled()):
        return alpaca_response

    ibkr_response = _run_ibkr_shadow_scans(payload)

    if isinstance(alpaca_response, tuple) or isinstance(ibkr_response, tuple):
        return alpaca_response

    if isinstance(alpaca_response, dict):
        alpaca_response["parallel_runs"] = {
            "alpaca": {"ok": bool(alpaca_response.get("ok", False))},
            "ibkr": {"ok": bool(ibkr_response.get("ok", False)) if isinstance(ibkr_response, dict) else False},
        }
        alpaca_response["shadow_ibkr"] = ibkr_response
    return alpaca_response


def run_scan_wrapper(payload):
    return handle_scan_request(payload)


def run_scheduled_paper_scan_wrapper(payload):
    now_ny = datetime.now(NY_TZ)
    scheduled_mode_order = resolve_alpaca_scheduled_mode_order()
    return run_scheduled_scan_wrapper(
        payload,
        now_ny=now_ny,
        build_scheduled_scan_payload=lambda scan_payload, now_ny=None: build_scheduled_scan_payload(
            scan_payload,
            now_ny=now_ny,
            mode_order=scheduled_mode_order,
        ),
        handle_scan_request_fn=handle_scan_request,
    )






def _close_all_paper_positions_for_broker(broker) -> dict[str, Any] | tuple[dict[str, Any], int]:
    return run_close_all_paper_positions(
        execute_close_all_paper_positions=execute_close_all_paper_positions,
        get_open_positions=broker.get_open_positions,
        get_managed_open_paper_trades_for_eod_close=lambda: get_managed_open_paper_trades_for_eod_close_for_broker(broker),
        cancel_open_orders_for_symbol=broker.cancel_open_orders_for_symbol,
        close_position=broker.close_position,
        get_order_by_id=broker.get_order_by_id,
        safe_insert_broker_order=safe_insert_broker_order,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        upsert_trade_lifecycle=upsert_trade_lifecycle,
        to_float_or_none=to_float_or_none,
        parse_iso_utc=parse_iso_utc,
    )


def close_all_paper_positions():
    alpaca_result = _close_all_paper_positions_for_broker(ALPACA_PAPER_BROKER)

    if not PAPER_BROKER_CONFIG.shadow_mode_enabled or not ibkr_bridge_enabled():
        return alpaca_result

    ibkr_result = _close_all_paper_positions_for_broker(IBKR_PAPER_BROKER)

    if isinstance(alpaca_result, tuple):
        return alpaca_result

    if isinstance(ibkr_result, tuple):
        alpaca_result["shadow_ibkr_close"] = ibkr_result[0]
        alpaca_result["shadow_ibkr_close_status_code"] = ibkr_result[1]
        return alpaca_result

    aggregated = dict(alpaca_result or {})
    aggregated["shadow_ibkr_close"] = ibkr_result
    aggregated["combined_position_count"] = int(alpaca_result.get("position_count", 0) or 0) + int(ibkr_result.get("position_count", 0) or 0)
    aggregated["combined_closed_count"] = int(alpaca_result.get("closed_count", 0) or 0) + int(ibkr_result.get("closed_count", 0) or 0)
    aggregated["combined_skipped_count"] = int(alpaca_result.get("skipped_count", 0) or 0) + int(ibkr_result.get("skipped_count", 0) or 0)
    return aggregated



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
        run_mode_ranking_refresh=lambda: refresh_alpaca_mode_rankings(ranking_date=now_ny.date().isoformat()),
    )


def run_maintenance_scheduler(*, now_ny: datetime, retention_days: int = 30):
    return build_execute_maintenance_ops(
        now_ny=now_ny,
        prune_logs=prune_alpaca_api_logs,
        retention_days=retention_days,
    )


def _metadata_access_token() -> str:
    response = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    token = str(payload.get("access_token", "")).strip()
    if not token:
        raise RuntimeError("Could not resolve GCP access token from metadata server.")
    return token


def _ibkr_vm_settings() -> tuple[str, str, str]:
    project = (
        str(os.getenv("IBKR_VM_PROJECT", "")).strip()
        or str(os.getenv("GOOGLE_CLOUD_PROJECT", "")).strip()
        or str(os.getenv("GCP_PROJECT", "")).strip()
        or "stock-scanner-490821"
    )
    zone = str(os.getenv("IBKR_VM_ZONE", "europe-west1-b")).strip() or "europe-west1-b"
    instance = str(os.getenv("IBKR_VM_INSTANCE_NAME", "ibkr-bridge-vm")).strip() or "ibkr-bridge-vm"
    return project, zone, instance


def _ibkr_vm_compute_api_request(method: str, suffix: str) -> dict:
    access_token = _metadata_access_token()
    project, zone, instance = _ibkr_vm_settings()
    url = (
        "https://compute.googleapis.com/compute/v1/projects/"
        f"{project}/zones/{zone}/instances/{instance}{suffix}"
    )
    response = requests.request(
        method,
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    response.raise_for_status()
    if not response.content:
        return {}
    return response.json()


def _get_ibkr_vm_status() -> str | None:
    payload = _ibkr_vm_compute_api_request("GET", "")
    return str(payload.get("status", "")).strip().upper() or None


def _start_ibkr_vm() -> dict:
    payload = _ibkr_vm_compute_api_request("POST", "/start")
    log_info("requested ibkr vm start", component="scheduler", operation="ibkr-vm-control")
    return payload


def _stop_ibkr_vm() -> dict:
    payload = _ibkr_vm_compute_api_request("POST", "/stop")
    log_info("requested ibkr vm stop", component="scheduler", operation="ibkr-vm-control")
    return payload


def run_ibkr_vm_control_scheduler(*, now_ny: datetime, action: str, force: bool = False):
    is_trading_day, _is_early_close, _market_open_ny, _market_close_ny, holiday_message = holiday_and_early_close_status(now_ny)
    return build_execute_ibkr_vm_control(
        now_ny=now_ny,
        action=action,
        force=force,
        is_trading_day=is_trading_day,
        holiday_message=holiday_message,
        get_instance_status=_get_ibkr_vm_status,
        start_instance=_start_ibkr_vm,
        stop_instance=_stop_ibkr_vm,
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
    get_ibkr_operational_status=get_ibkr_operational_status,
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
    execute_ibkr_vm_control=run_ibkr_vm_control_scheduler,
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
