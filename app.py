import os
from datetime import datetime, timezone, timedelta

import requests
from zoneinfo import ZoneInfo

from flask import Flask
from flask_cors import CORS
from google.cloud import storage
from paper_alpaca import place_paper_bracket_order_from_trade, get_open_positions, get_open_orders, close_position, cancel_open_orders_for_symbol
from alpaca_sync import sync_order_by_id, get_order_by_id
from alpaca_reconcile import run_reconciliation, upload_file_to_gcs
from trade_analysis import run_trade_analysis, upload_file_to_gcs as upload_analysis_file_to_gcs
from signal_analysis import run_signal_analysis, upload_file_to_gcs as upload_signal_analysis_file_to_gcs
from db import healthcheck as db_healthcheck
from storage import (
    insert_scan_run,
    insert_trade_event,
    insert_broker_order,
    insert_reconciliation_run,
    insert_reconciliation_detail,
    get_latest_reconciliation_summary,
    get_recent_reconciliation_mismatches,
    get_ops_summary,
    get_open_trade_events,
    get_closed_trade_events,
    get_recent_trade_event_rows,
    get_latest_scan_summary,
    get_recent_alpaca_api_logs,
    get_recent_alpaca_api_errors,
    get_trade_lifecycles,
    get_trade_lifecycle_summary_from_table,
    upsert_trade_lifecycle,
    get_dashboard_summary,
)
from export_daily_snapshot import run_daily_snapshot
from routes.health import register_health_routes
from routes.export import register_export_routes
from routes.analysis import register_analysis_routes
from routes.reconcile import register_reconcile_routes
from routes.trades import register_trade_routes
from routes.scans import register_scan_routes
from routes.dashboard import register_dashboard_routes

from routes.sync import register_sync_routes
from services.sync_service import execute_sync_paper_trades
from services.scan_service import execute_full_scan
from services.trade_service import execute_close_all_paper_positions
from services.logging_service import (
    append_csv_row as append_csv_row_service,
    append_signal_log as append_signal_log_service,
    append_trade_log as append_trade_log_service,
    read_csv_rows_for_path,
    read_trade_rows_for_date as read_trade_rows_for_date_service,
)

from trade_scan import (
    run_scan,
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
SCHEDULED_PAPER_ACCOUNT_SIZE = float(os.getenv("SCHEDULED_PAPER_ACCOUNT_SIZE", "1000"))
SCHEDULED_ROUND_ROBIN_MODES = ["primary", "secondary", "third", "fourth", "core_one", "core_two"]

PAPER_STOP_COOLDOWN_MINUTES = int(os.getenv("PAPER_STOP_COOLDOWN_MINUTES", "30"))
PAPER_TARGET_COOLDOWN_MINUTES = int(os.getenv("PAPER_TARGET_COOLDOWN_MINUTES", "0"))
PAPER_MANUAL_CLOSE_COOLDOWN_MINUTES = int(os.getenv("PAPER_MANUAL_CLOSE_COOLDOWN_MINUTES", "0"))


app = Flask(__name__)
CORS(app)


def env_flag(name: str, default: str = "true") -> bool:
    value = str(os.getenv(name, default)).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}

ENABLE_CSV_LOGGING = env_flag("ENABLE_CSV_LOGGING", "true")
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
        print(f"DB scan run write failed: {e}", flush=True)


def safe_insert_trade_event(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_trade_event(**kwargs)
    except Exception as e:
        print(f"DB trade event write failed: {e}", flush=True)


def safe_insert_broker_order(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_broker_order(**kwargs)
    except Exception as e:
        print(f"DB broker order write failed: {e}", flush=True)



def safe_insert_reconciliation_run(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_reconciliation_run(**kwargs)
    except Exception as e:
        print(f"DB reconciliation write failed: {e}", flush=True)


def safe_insert_reconciliation_detail(**kwargs) -> None:
    if not ENABLE_DB_LOGGING:
        return
    try:
        insert_reconciliation_detail(**kwargs)
    except Exception as e:
        print(f"DB reconciliation detail write failed: {e}", flush=True)


def find_instrument_by_symbol(symbol: str) -> tuple[str, str] | tuple[None, None]:
    symbol = symbol.strip().upper()
    for mode_name, instruments in INSTRUMENT_GROUPS.items():
        for display_name, info in instruments.items():
            if info.get("symbol", "").upper() == symbol:
                return display_name, mode_name
    return None, None


def scheduled_round_robin_mode(now_ny: datetime | None = None) -> str | None:
    now_ny = now_ny or datetime.now(NY_TZ)
    total_minutes = (now_ny.hour * 60) + now_ny.minute
    first_scan_minute = (9 * 60) + 50
    last_scan_minute = (15 * 60) + 50

    if total_minutes < first_scan_minute or total_minutes > last_scan_minute:
        return None

    slot_index = ((total_minutes - first_scan_minute) // 10) % len(SCHEDULED_ROUND_ROBIN_MODES)
    return SCHEDULED_ROUND_ROBIN_MODES[slot_index]


def build_scheduled_scan_payload(payload: dict, now_ny: datetime | None = None) -> dict:
    now_ny = now_ny or datetime.now(NY_TZ)
    scheduled_mode = scheduled_round_robin_mode(now_ny)
    if scheduled_mode is None:
        raise ValueError("outside scheduled paper scan window")

    account_size = payload.get("account_size", SCHEDULED_PAPER_ACCOUNT_SIZE)

    return {
        "account_size": account_size,
        "mode": scheduled_mode,
        "paper_trade": True,
        "debug": payload.get("debug", False),
        "scan_source": "SCHEDULED",
    }

LOG_BUCKET = os.getenv("LOG_BUCKET", "stock-scanner-490821-logs")
SIGNALS_CSV_PATH = os.getenv("SIGNALS_CSV_PATH", "signals/signals.csv")
TRADES_CSV_PATH = os.getenv("TRADES_CSV_PATH", "trades/trades.csv")
RECONCILIATION_BUCKET = os.getenv("RECONCILIATION_BUCKET", "stock-scanner-490821-logs")
RECONCILIATION_OBJECT = os.getenv("RECONCILIATION_OBJECT", "reports/alpaca_reconciliation.csv")
TRADE_ANALYSIS_BUCKET = os.getenv("TRADE_ANALYSIS_BUCKET", "stock-scanner-490821-logs")
TRADE_ANALYSIS_SUMMARY_OBJECT = os.getenv("TRADE_ANALYSIS_SUMMARY_OBJECT", "reports/trade_analysis_summary.csv")
TRADE_ANALYSIS_PAIRED_OBJECT = os.getenv("TRADE_ANALYSIS_PAIRED_OBJECT", "reports/trade_analysis_paired_trades.csv")
SIGNAL_ANALYSIS_BUCKET = os.getenv("SIGNAL_ANALYSIS_BUCKET", "stock-scanner-490821-logs")
SIGNAL_ANALYSIS_SUMMARY_OBJECT = os.getenv("SIGNAL_ANALYSIS_SUMMARY_OBJECT", "reports/signal_analysis_summary.csv")
SIGNAL_ANALYSIS_ROWS_OBJECT = os.getenv("SIGNAL_ANALYSIS_ROWS_OBJECT", "reports/signal_analysis_rows.csv")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")
TWELVEDATA_BASE_URL = "https://api.twelvedata.com/time_series"
NY_TZ = ZoneInfo("America/New_York")


def trade_to_dict(eval_result: dict) -> dict:
    m = eval_result["metrics"]
    return {
        "name": eval_result["name"],
        "symbol": m["symbol"],
        "confidence": m["final_confidence"],
        "direction": m["direction"],
        "manual_eligible": m.get("manual_eligible", m["direction"] == "BUY"),
        "paper_eligible": m.get("paper_eligible", False),
        "current_price": round(m["price"], 4),
        "entry": round(m["entry"], 4),
        "stop": round(m["stop"], 4),
        "target": round(m["target"], 4),
        "shares": m["shares"],
        "position_cost": round(m["actual_position_cost"], 2),
        "risk_per_share": round(m["risk_per_share"], 4),
        "actual_risk": round(m["actual_risk"], 2),
        "max_allowed_risk": round(m["risk_amount"], 2),
        "or_high": round(m["or_high"], 4),
        "or_low": round(m["or_low"], 4),
        "vwap": round(m["vwap"], 4),
        "benchmark_key": m.get("benchmark_key"),
        "benchmark_direction": m.get("benchmark_direction"),
        "reason": eval_result["final_reason"],
    }



def debug_to_dict(eval_result: dict) -> dict:
    return {
        "name": eval_result["name"],
        "decision": eval_result["decision"],
        "final_reason": eval_result["final_reason"],
        "checks": eval_result.get("checks", {}),
        "metrics": eval_result.get("metrics", {}),
    }


# --- Helper function for paper trade candidate selection ---
def paper_candidate_from_evaluation(eval_result: dict) -> dict | None:
    metrics = eval_result.get("metrics") or {}
    direction = str(metrics.get("direction", "")).strip().upper()

    if direction not in {"BUY", "SELL"}:
        return None

    entry = to_float_or_none(metrics.get("entry"))
    price = to_float_or_none(metrics.get("price"))
    stop = to_float_or_none(metrics.get("stop"))
    target = to_float_or_none(metrics.get("target"))

    entry_value = entry if entry is not None else price
    if entry_value is None or stop is None or target is None:
        return None

    confidence = metrics.get("final_confidence")
    confidence_value = None
    try:
        confidence_value = float(confidence)
    except (TypeError, ValueError):
        priority = to_float_or_none(metrics.get("priority"))
        if priority is not None:
            confidence_value = float(priority) * 10.0

    if confidence_value is None or confidence_value < PAPER_TRADE_MIN_CONFIDENCE:
        return None

    shares = metrics.get("shares")
    if shares in (None, ""):
        shares = ""

    risk_per_share = to_float_or_none(metrics.get("risk_per_share"))
    if risk_per_share is None:
        risk_per_share = abs(entry_value - stop)

    share_count_for_calc = to_float_or_none(shares)
    if share_count_for_calc is None or share_count_for_calc <= 0:
        share_count_for_calc = 0.0

    actual_position_cost = to_float_or_none(metrics.get("actual_position_cost"))
    if actual_position_cost is None:
        actual_position_cost = entry_value * share_count_for_calc

    actual_risk = to_float_or_none(metrics.get("actual_risk"))
    if actual_risk is None:
        actual_risk = risk_per_share * share_count_for_calc

    risk_amount = to_float_or_none(metrics.get("risk_amount"))
    if risk_amount is None:
        risk_amount = actual_risk

    normalized_metrics = {
        **metrics,
        "entry": entry_value,
        "price": price if price is not None else entry_value,
        "stop": stop,
        "target": target,
        "shares": int(float(shares)) if shares not in (None, "") else "",
        "actual_position_cost": actual_position_cost,
        "risk_per_share": risk_per_share,
        "actual_risk": actual_risk,
        "risk_amount": risk_amount,
        "final_confidence": confidence_value,
        "manual_eligible": direction == "BUY" and str(eval_result.get("decision", "")).strip().upper() == "VALID",
        "paper_eligible": True,
    }

    return {
        "name": eval_result.get("name", ""),
        "final_reason": eval_result.get("final_reason", ""),
        "decision": "PAPER_CANDIDATE",
        "checks": eval_result.get("checks", {}),
        "metrics": normalized_metrics,
    }


def append_csv_row(path: str, headers: list[str], row: dict) -> None:
    append_csv_row_service(
        storage_client_factory=storage.Client,
        bucket_name=LOG_BUCKET,
        path=path,
        headers=headers,
        row=row,
    )


def append_signal_log(row: dict) -> None:
    append_signal_log_service(
        enabled=ENABLE_CSV_LOGGING,
        append_csv_row_func=append_csv_row,
        path=SIGNALS_CSV_PATH,
        row=row,
    )


def append_trade_log(row: dict) -> None:
    append_trade_log_service(
        enabled=ENABLE_CSV_LOGGING,
        append_csv_row_func=append_csv_row,
        path=TRADES_CSV_PATH,
        row=row,
    )

def read_trade_rows_for_date(target_date: str) -> list[dict]:
    return read_trade_rows_for_date_service(
        all_rows=read_all_trade_rows(),
        target_date=target_date,
    )


def read_all_trade_rows() -> list[dict]:
    return read_csv_rows_for_path(
        storage_client_factory=storage.Client,
        bucket_name=LOG_BUCKET,
        path=TRADES_CSV_PATH,
    )


# --- Helper functions for paper trade syncing ---

def paper_trade_exit_already_logged(parent_order_id: str, exit_event: str) -> bool:
    parent_order_id = str(parent_order_id).strip()
    exit_event = str(exit_event).strip().upper()
    if not parent_order_id or not exit_event:
        return False

    rows = read_all_trade_rows()
    for row in rows:
        if str(row.get("trade_source", "")).strip().upper() != "ALPACA_PAPER":
            continue
        if str(row.get("broker_parent_order_id", "")).strip() != parent_order_id:
            continue
        if str(row.get("event_type", "")).strip().upper() == exit_event:
            return True
    return False


def get_open_paper_trades() -> list[dict]:
    rows = read_all_trade_rows()
    latest_by_parent_order_id: dict[str, dict] = {}

    for row in rows:
        if str(row.get("trade_source", "")).strip().upper() != "ALPACA_PAPER":
            continue

        parent_order_id = str(row.get("broker_parent_order_id", "")).strip()
        if not parent_order_id:
            continue

        latest_by_parent_order_id[parent_order_id] = row

    open_rows = []
    for row in latest_by_parent_order_id.values():
        if str(row.get("status", "")).strip().upper() == "OPEN":
            open_rows.append(row)

    return open_rows


def get_managed_open_paper_trades_for_eod_close() -> list[dict]:
    open_rows = get_open_paper_trades()

    try:
        positions = get_open_positions()
        open_orders = get_open_orders()
    except Exception as e:
        print(f"Broker validation for open paper trades failed: {e}", flush=True)
        return open_rows

    open_position_symbols = {
        str(position.get("symbol", "")).strip().upper()
        for position in positions
        if str(position.get("symbol", "")).strip()
    }

    open_order_ids = {
        str(order.get("id", "")).strip()
        for order in open_orders
        if str(order.get("id", "")).strip()
    }

    for order in open_orders:
        for leg in order.get("legs") or []:
            leg_id = str(leg.get("id", "")).strip()
            if leg_id:
                open_order_ids.add(leg_id)

    validated_open_rows = []
    for row in open_rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        parent_order_id = str(row.get("broker_parent_order_id", "")).strip()
        broker_order_id = str(row.get("broker_order_id", "")).strip()

        if (
            symbol in open_position_symbols
            or parent_order_id in open_order_ids
            or broker_order_id in open_order_ids
        ):
            validated_open_rows.append(row)

    return validated_open_rows


def read_all_signal_rows() -> list[dict]:
    return read_csv_rows_for_path(
        storage_client_factory=storage.Client,
        bucket_name=LOG_BUCKET,
        path=SIGNALS_CSV_PATH,
    )


def parse_iso_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def to_float_or_none(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_scan_id(timestamp_utc: str, mode: str) -> str:
    safe_ts = str(timestamp_utc).replace(":", "-")
    return f"{safe_ts}_{mode}"


def market_phase_from_timestamp(timestamp_utc: str) -> str:
    try:
        dt_ny = parse_iso_utc(timestamp_utc).astimezone(NY_TZ)
    except Exception:
        return "UNKNOWN"

    minutes = (dt_ny.hour * 60) + dt_ny.minute
    open_minute = (9 * 60) + 30
    if minutes < open_minute:
        return "PREMARKET"
    if minutes < open_minute + 30:
        return "OPENING"
    if minutes < (12 * 60):
        return "MORNING"
    if minutes < (14 * 60):
        return "MIDDAY"
    if minutes < (15 * 60) + 30:
        return "AFTERNOON"
    return "POWER_HOUR"


def get_latest_open_paper_trade_for_symbol(symbol: str) -> dict | None:
    symbol = str(symbol).strip().upper()
    if not symbol:
        return None

    open_rows = get_open_paper_trades()
    for row in reversed(open_rows):
        if str(row.get("symbol", "")).strip().upper() == symbol:
            return row
    return None


def get_latest_paper_close_event_for_symbol(symbol: str) -> dict | None:
    symbol = str(symbol).strip().upper()
    if not symbol:
        return None

    for row in reversed(read_all_trade_rows()):
        if str(row.get("trade_source", "")).strip().upper() != "ALPACA_PAPER":
            continue
        if str(row.get("symbol", "")).strip().upper() != symbol:
            continue
        if str(row.get("status", "")).strip().upper() != "CLOSED":
            continue
        return row
    return None


def is_symbol_in_paper_cooldown(symbol: str, now_utc: str) -> tuple[bool, str]:
    latest_close = get_latest_paper_close_event_for_symbol(symbol)
    if not latest_close:
        return False, ""

    exit_reason = str(latest_close.get("exit_reason", "")).strip().upper()
    cooldown_minutes = 0
    cooldown_label = ""

    if exit_reason == "STOP_HIT":
        cooldown_minutes = PAPER_STOP_COOLDOWN_MINUTES
        cooldown_label = "stop"
    elif exit_reason == "TARGET_HIT":
        cooldown_minutes = PAPER_TARGET_COOLDOWN_MINUTES
        cooldown_label = "target"
    elif exit_reason in {"MANUAL_CLOSE", "EOD_CLOSE"}:
        cooldown_minutes = PAPER_MANUAL_CLOSE_COOLDOWN_MINUTES
        cooldown_label = "manual_close"
    else:
        return False, ""

    if cooldown_minutes <= 0:
        return False, ""

    latest_ts = str(latest_close.get("timestamp_utc", "")).strip()
    if not latest_ts:
        return False, ""

    try:
        now_dt = parse_iso_utc(now_utc)
        latest_dt = parse_iso_utc(latest_ts)
    except Exception:
        return False, ""

    cooldown_until = latest_dt + timedelta(minutes=cooldown_minutes)
    if now_dt < cooldown_until:
        return True, f"{cooldown_label}_cooldown_until_{cooldown_until.isoformat()}"

    return False, ""


def find_best_signal_match(symbol: str, actual_entry_price: float | None, open_timestamp_utc: str) -> dict | None:
    rows = read_all_signal_rows()
    symbol = symbol.strip().upper()
    open_dt = parse_iso_utc(open_timestamp_utc)

    candidates = []
    for row in rows:
        row_symbol = str(row.get("top_symbol", "")).strip().upper()
        if row_symbol != symbol:
            continue

        row_ts = str(row.get("timestamp_utc", "")).strip()
        if not row_ts:
            continue

        try:
            row_dt = parse_iso_utc(row_ts)
        except Exception:
            continue

        if row_dt > open_dt:
            continue

        entry_val = to_float_or_none(row.get("entry", ""))
        if actual_entry_price is None or entry_val is None:
            price_diff = float("inf")
        else:
            price_diff = abs(entry_val - actual_entry_price)

        time_diff_seconds = (open_dt - row_dt).total_seconds()
        candidates.append((price_diff, time_diff_seconds, row_dt, row))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], x[1], -x[2].timestamp()))
    return candidates[0][3]


def find_latest_open_trade(symbol: str, trade_source: str | None = None, broker_parent_order_id: str | None = None) -> dict | None:
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_trade_source = str(trade_source or "").strip().upper()
    normalized_parent_order_id = str(broker_parent_order_id or "").strip()

    if not normalized_symbol:
        return None

    if normalized_trade_source == "ALPACA_PAPER":
        open_rows = get_open_paper_trades()

        if normalized_parent_order_id:
            for row in reversed(open_rows):
                if str(row.get("broker_parent_order_id", "")).strip() == normalized_parent_order_id:
                    return row

        for row in reversed(open_rows):
            if str(row.get("symbol", "")).strip().upper() == normalized_symbol:
                return row

        return None

    rows = read_all_trade_rows()
    current_open = None

    for row in rows:
        row_symbol = str(row.get("symbol", "")).strip().upper()
        if row_symbol != normalized_symbol:
            continue

        if normalized_trade_source:
            row_trade_source = str(row.get("trade_source", "")).strip().upper()
            if row_trade_source != normalized_trade_source:
                continue

        if normalized_parent_order_id:
            row_parent_order_id = str(row.get("broker_parent_order_id", "")).strip()
            if row_parent_order_id != normalized_parent_order_id:
                continue

        event_type = str(row.get("event_type", "")).strip().upper()
        status = str(row.get("status", "")).strip().upper()

        if event_type == "OPEN":
            current_open = row
        elif status == "CLOSED":
            current_open = None

    return current_open


def fetch_candles_between(symbol: str, start_utc: str, end_utc: str, interval: str = "5min") -> list[dict]:
    if not TWELVEDATA_API_KEY:
        raise RuntimeError("Missing TWELVEDATA_API_KEY in environment.")

    start_dt_ny = parse_iso_utc(start_utc).astimezone(NY_TZ)
    end_dt_ny = parse_iso_utc(end_utc).astimezone(NY_TZ)

    elapsed_minutes = max(1, int((end_dt_ny - start_dt_ny).total_seconds() / 60))
    if interval == "5min":
        outputsize = min(5000, max(100, (elapsed_minutes // 5) + 20))
    else:
        outputsize = min(5000, max(200, elapsed_minutes + 30))

    params = {
        "symbol": symbol,
        "interval": interval,
        "start_date": start_dt_ny.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": end_dt_ny.strftime("%Y-%m-%d %H:%M:%S"),
        "outputsize": outputsize,
        "apikey": TWELVEDATA_API_KEY,
        "format": "JSON",
        "order": "asc",
    }

    response = requests.get(TWELVEDATA_BASE_URL, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()

    if data.get("status") == "error":
        raise ValueError(data.get("message", "Unknown Twelve Data error"))

    values = data.get("values") or []
    candles = []

    for row in values:
        try:
            candles.append({
                "datetime": row["datetime"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            })
        except Exception:
            continue

    return candles


def infer_first_level_hit(open_row: dict, close_timestamp_utc: str) -> dict:
    stop_raw = open_row.get("stop_price", "")
    target_raw = open_row.get("target_price", "")
    open_ts = open_row.get("timestamp_utc", "")
    symbol = str(open_row.get("symbol", "")).strip().upper()

    if not open_ts or stop_raw in ("", None) or target_raw in ("", None):
        return {
            "inferred_stop_hit": "",
            "inferred_target_hit": "",
            "inferred_first_level_hit": "",
            "inferred_analysis_start_utc": open_ts,
            "inferred_analysis_end_utc": close_timestamp_utc,
        }

    stop_price = float(stop_raw)
    target_price = float(target_raw)

    candles = fetch_candles_between(symbol, open_ts, close_timestamp_utc, interval="5min")

    stop_index = None
    target_index = None

    for i, candle in enumerate(candles):
        if stop_index is None and candle["low"] <= stop_price:
            stop_index = i
        if target_index is None and candle["high"] >= target_price:
            target_index = i

    inferred_stop_hit = "YES" if stop_index is not None else "NO"
    inferred_target_hit = "YES" if target_index is not None else "NO"

    if stop_index is None and target_index is None:
        first_hit = "NEITHER"
    elif stop_index is not None and target_index is None:
        first_hit = "STOP_FIRST"
    elif stop_index is None and target_index is not None:
        first_hit = "TARGET_FIRST"
    elif stop_index < target_index:
        first_hit = "STOP_FIRST"
    elif target_index < stop_index:
        first_hit = "TARGET_FIRST"
    else:
        first_hit = "BOTH_SAME_CANDLE"

    return {
        "inferred_stop_hit": inferred_stop_hit,
        "inferred_target_hit": inferred_target_hit,
        "inferred_first_level_hit": first_hit,
        "inferred_analysis_start_utc": open_ts,
        "inferred_analysis_end_utc": close_timestamp_utc,
    }







# --- Helper functions for scan route wrappers ---


def handle_sync_paper_trades():
    return execute_sync_paper_trades(
        get_open_paper_trades=get_open_paper_trades,
        sync_order_by_id=sync_order_by_id,
        paper_trade_exit_already_logged=paper_trade_exit_already_logged,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        safe_insert_broker_order=safe_insert_broker_order,
        parse_iso_utc=parse_iso_utc,
        to_float_or_none=to_float_or_none,
    )

def handle_scan_request(payload):
    return execute_full_scan(
        payload,
        market_time_check=market_time_check,
        build_scan_id=build_scan_id,
        market_phase_from_timestamp=market_phase_from_timestamp,
        append_signal_log=append_signal_log,
        safe_insert_scan_run=safe_insert_scan_run,
        parse_iso_utc=parse_iso_utc,
        run_scan=run_scan,
        trade_to_dict=trade_to_dict,
        debug_to_dict=debug_to_dict,
        paper_candidate_from_evaluation=paper_candidate_from_evaluation,
        get_latest_open_paper_trade_for_symbol=get_latest_open_paper_trade_for_symbol,
        is_symbol_in_paper_cooldown=is_symbol_in_paper_cooldown,
        place_paper_bracket_order_from_trade=place_paper_bracket_order_from_trade,
        append_trade_log=append_trade_log,
        safe_insert_trade_event=safe_insert_trade_event,
        safe_insert_broker_order=safe_insert_broker_order,
        to_float_or_none=to_float_or_none,
        MIN_CONFIDENCE=MIN_CONFIDENCE,
    )


def run_scan_wrapper(payload):
    return handle_scan_request(payload)


def run_scheduled_paper_scan_wrapper(payload):
    now_ny = datetime.now(NY_TZ)
    try:
        scheduled_payload = build_scheduled_scan_payload(payload, now_ny=now_ny)
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "current_new_york_time": now_ny.strftime("%Y-%m-%d %H:%M"),
        }, 400

    return handle_scan_request(scheduled_payload)






def close_all_paper_positions():
    result = execute_close_all_paper_positions(
        get_open_positions=get_open_positions,
        get_managed_open_paper_trades_for_eod_close=get_managed_open_paper_trades_for_eod_close,
        cancel_open_orders_for_symbol=cancel_open_orders_for_symbol,
        close_position=close_position,
        get_order_by_id=get_order_by_id,
        safe_insert_broker_order=safe_insert_broker_order,
        to_float_or_none=to_float_or_none,
        parse_iso_utc=parse_iso_utc,
    )
    if isinstance(result, tuple):
        body, status_code = result
        raise RuntimeError(body.get("error", f"close_all_paper_positions failed with status {status_code}"))
    return result



register_health_routes(
    app,
    db_healthcheck=db_healthcheck,
    enable_csv_logging=ENABLE_CSV_LOGGING,
    enable_db_logging=ENABLE_DB_LOGGING,
    get_ops_summary=get_ops_summary,
    get_recent_alpaca_api_logs=get_recent_alpaca_api_logs,
    get_recent_alpaca_api_errors=get_recent_alpaca_api_errors,
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
)



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)